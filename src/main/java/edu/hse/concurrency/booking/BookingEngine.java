package edu.hse.concurrency.booking;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

final class BookingEngine {

    private final Hall hall;
    private final Duration holdDuration;
    private final SeatState[] places;
    private final Object[] creationLocks = new Object[256];

    private final Object lifecycleLock = new Object();
    private final Set<Waiter> activeWaiters = new HashSet<>();
    private volatile boolean stopped;

    private final Object expirationLock = new Object();
    private final TreeSet<Reservation> expirationQueue = new TreeSet<>(
            Comparator.comparing((Reservation r) -> r.hold.expiresAt())
                    .thenComparingLong(r -> r.hold.id())
    );
    private long nextId = 1;
    private final Thread expirationThread;

    BookingEngine(Hall hall, Duration holdDuration) {
        this.hall = Objects.requireNonNull(hall, "hall");
        this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
        if (holdDuration.isZero() || holdDuration.isNegative()) {
            throw new IllegalArgumentException("holdDuration must be positive");
        }
        places = new SeatState[hall.capacity()];
        for (int i = 0; i < creationLocks.length; i++) {
            creationLocks[i] = new Object();
        }
        expirationThread = new Thread(this::expire, "booking-expiration");
        expirationThread.setDaemon(true);
        expirationThread.start();
    }

    Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Request request = request(clientId, seats);
        ensureRunning();
        Result result = withLocks(request.states, 0,
                () -> acquire(clientId, request.seats, request.states, null));
        return Optional.ofNullable(result.hold);
    }

    Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Request request = request(clientId, seats);
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        ensureRunning();
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
        if (timeout.isZero()) {
            Result result = withLocks(request.states, 0, () -> {
                if (Thread.interrupted()) {
                    return new Result(Outcome.INTERRUPTED, null);
                }
                return acquire(clientId, request.seats, request.states, null);
            });
            if (result.outcome == Outcome.INTERRUPTED) {
                throw new InterruptedException();
            }
            return Optional.ofNullable(result.hold);
        }

        long started = System.nanoTime();
        long limit = nanos(timeout);
        Waiter waiter = new Waiter();
        boolean first = true;
        try {
            while (true) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (!first && remaining(started, limit) <= 0) {
                    return Optional.empty();
                }
                boolean firstAttempt = first;
                first = false;
                Result result = withLocks(request.states, 0, () -> {
                    if (Thread.interrupted()) {
                        return new Result(Outcome.INTERRUPTED, null);
                    }
                    if (!firstAttempt && remaining(started, limit) <= 0) {
                        return new Result(Outcome.TIMED_OUT, null);
                    }
                    waiter.clear();
                    return acquire(clientId, request.seats, request.states, waiter);
                });
                if (result.outcome == Outcome.INTERRUPTED) {
                    throw new InterruptedException();
                }
                if (result.outcome == Outcome.CREATED) {
                    return Optional.of(result.hold);
                }
                if (result.outcome != Outcome.BUSY) {
                    return Optional.empty();
                }
                long left = remaining(started, limit);
                if (left <= 0) {
                    return Optional.empty();
                }
                waiter.await(left);
            }
        } finally {
            if (waiter.registered) {
                withLocks(request.states, 0, () -> {
                    synchronized (lifecycleLock) {
                        for (SeatState state : request.states) {
                            state.waiters.remove(waiter);
                        }
                        activeWaiters.remove(waiter);
                        waiter.registered = false;
                    }
                    return null;
                });
            }
        }
    }

    boolean complete(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "hold");
        ensureRunning();
        if (hold.seats().isEmpty()) {
            return false;
        }
        Seat representative = hold.seats().iterator().next();
        if (!hall.contains(representative)) {
            return false;
        }
        SeatState state = place(index(representative));
        Reservation reservation;
        synchronized (state) {
            reservation = state.reservation;
            if (reservation == null || reservation.hold != hold) {
                return false;
            }
        }
        return withLocks(reservation.states, 0, () -> {
            if (stopped) {
                throw new IllegalStateException("Service is stopped");
            }
            if (!Instant.now().isBefore(reservation.hold.expiresAt())) {
                return false;
            }
            for (SeatState seatState : reservation.states) {
                if (seatState.reservation != reservation || seatState.sold) {
                    return false;
                }
            }
            for (SeatState seatState : reservation.states) {
                seatState.reservation = null;
                seatState.sold = sell;
                seatState.notifyWaiters();
            }
            synchronized (expirationLock) {
                expirationQueue.remove(reservation);
                expirationLock.notifyAll();
            }
            return true;
        });
    }

    SeatStatus status(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Seat is outside the hall");
        }
        SeatState state = place(index(seat));
        synchronized (state) {
            if (state.sold) {
                return SeatStatus.SOLD;
            }
            return state.reservation != null
                    && Instant.now().isBefore(state.reservation.hold.expiresAt())
                    ? SeatStatus.HELD : SeatStatus.FREE;
        }
    }

    void shutdown() {
        List<Waiter> toWake;
        synchronized (lifecycleLock) {
            stopped = true;
            toWake = new ArrayList<>(activeWaiters);
        }
        for (Waiter waiter : toWake) {
            waiter.signal();
        }
        synchronized (expirationLock) {
            expirationLock.notifyAll();
        }
        boolean interrupted = false;
        while (true) {
            try {
                expirationThread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Result acquire(String clientId, Set<Seat> seats,
                           SeatState[] states, Waiter waiter) {
        if (stopped) {
            return new Result(Outcome.STOPPED, null);
        }
        Instant now = Instant.now();
        boolean busy = false;
        for (SeatState state : states) {
            if (state.sold) {
                return new Result(Outcome.SOLD, null);
            }
            if (state.reservation != null
                    && now.isBefore(state.reservation.hold.expiresAt())) {
                busy = true;
            }
        }
        synchronized (lifecycleLock) {
            if (stopped) {
                return new Result(Outcome.STOPPED, null);
            }
            if (busy) {
                if (waiter != null && !waiter.registered) {
                    for (SeatState state : states) {
                        state.waiters.add(waiter);
                    }
                    activeWaiters.add(waiter);
                    waiter.registered = true;
                }
                return new Result(Outcome.BUSY, null);
            }
            Reservation reservation;
            synchronized (expirationLock) {
                long id = nextId++;
                Hold hold = new Hold(id, clientId, seats, expiresAt(Instant.now()));
                reservation = new Reservation(hold, states);
                expirationQueue.add(reservation);
                expirationLock.notifyAll();
            }
            for (SeatState state : states) {
                if (state.reservation != null) {
                    state.notifyWaiters();
                }
                state.reservation = reservation;
            }
            return new Result(Outcome.CREATED, reservation.hold);
        }
    }

    private void expire() {
        while (true) {
            Reservation reservation;
            synchronized (expirationLock) {
                while (true) {
                    if (stopped) {
                        return;
                    }
                    if (expirationQueue.isEmpty()) {
                        try {
                            expirationLock.wait();
                        } catch (InterruptedException ignored) {
                        }
                        continue;
                    }
                    Reservation first = expirationQueue.first();
                    Duration delay = Duration.between(Instant.now(), first.hold.expiresAt());
                    if (delay.isZero() || delay.isNegative()) {
                        reservation = expirationQueue.pollFirst();
                        break;
                    }
                    try {
                        waitFor(expirationLock, delay);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            withLocks(reservation.states, 0, () -> {
                if (Instant.now().isBefore(reservation.hold.expiresAt())) {
                    synchronized (expirationLock) {
                        expirationQueue.add(reservation);
                        expirationLock.notifyAll();
                    }
                    return null;
                }
                for (SeatState state : reservation.states) {
                    if (state.reservation == reservation) {
                        state.reservation = null;
                        state.notifyWaiters();
                    }
                }
                return null;
            });
        }
    }

    private Request request(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(seats, "seats");
        if (clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
        if (seats.isEmpty() || seats.size() > BookingService.MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("Invalid number of seats");
        }
        Set<Seat> copy = Set.copyOf(seats);
        SeatState[] states = new SeatState[copy.size()];
        int i = 0;
        for (Seat seat : copy) {
            if (!hall.contains(seat)) {
                throw new IllegalArgumentException("Seat is outside the hall");
            }
            states[i++] = place(index(seat));
        }
        Arrays.sort(states, Comparator.comparingInt(s -> s.index));
        return new Request(copy, states);
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Service is stopped");
        }
    }

    private int index(Seat seat) {
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private SeatState place(int index) {
        synchronized (creationLocks[index % creationLocks.length]) {
            SeatState state = places[index];
            if (state == null) {
                state = new SeatState(index);
                places[index] = state;
            }
            return state;
        }
    }

    private <T> T withLocks(SeatState[] states, int at, Operation<T> action) {
        if (at == states.length) {
            return action.run();
        }
        synchronized (states[at]) {
            return withLocks(states, at + 1, action);
        }
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static long remaining(long started, long limit) {
        return limit - (System.nanoTime() - started);
    }

    private Instant expiresAt(Instant now) {
        try {
            return now.plus(holdDuration);
        } catch (DateTimeException | ArithmeticException e) {
            return Instant.MAX;
        }
    }

    private static void waitFor(Object monitor, Duration delay)
            throws InterruptedException {
        long seconds = delay.getSeconds();
        int nanos = delay.getNano();
        if (seconds >= Long.MAX_VALUE / 1000) {
            monitor.wait(Long.MAX_VALUE);
        } else {
            monitor.wait(seconds * 1000 + nanos / 1_000_000,
                    nanos % 1_000_000);
        }
    }

    private interface Operation<T> {
        T run();
    }

    private static final class SeatState {
        private final int index;
        private Reservation reservation;
        private boolean sold;
        private final Set<Waiter> waiters = new HashSet<>();

        private SeatState(int index) {
            this.index = index;
        }

        private void notifyWaiters() {
            for (Waiter waiter : waiters) {
                waiter.signal();
            }
        }
    }

    private static final class Reservation {
        private final Hold hold;
        private final SeatState[] states;

        private Reservation(Hold hold, SeatState[] states) {
            this.hold = hold;
            this.states = states;
        }
    }

    private static final class Waiter {
        private final Object monitor = new Object();
        private boolean signalled;
        private boolean registered;

        private void signal() {
            synchronized (monitor) {
                signalled = true;
                monitor.notify();
            }
        }

        private void clear() {
            synchronized (monitor) {
                signalled = false;
            }
        }

        private void await(long timeoutNanos) throws InterruptedException {
            long started = System.nanoTime();
            long remaining = timeoutNanos;

            synchronized (monitor) {
                while (!signalled && remaining > 0) {
                    monitor.wait(
                            remaining / 1_000_000,
                            (int) (remaining % 1_000_000)
                    );

                    remaining = timeoutNanos
                            - (System.nanoTime() - started);
                }

                signalled = false;
            }
        }
    }

    private record Request(Set<Seat> seats, SeatState[] states) {
    }

    private enum Outcome {
        CREATED, BUSY, SOLD, STOPPED, INTERRUPTED, TIMED_OUT
    }

    private record Result(Outcome outcome, Hold hold) {
    }
}