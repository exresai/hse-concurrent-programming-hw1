package edu.hse.concurrency.booking;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public class BookingServiceImpl implements BookingService {
    private final BookingEngine engine;

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        engine = new BookingEngine(hall, holdDuration);
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        return engine.hold(clientId, seats);
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        return engine.holdOrWait(clientId, seats, timeout);
    }

    @Override
    public boolean confirm(Hold hold) {
        return engine.complete(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return engine.complete(hold, false);
    }

    @Override
    public SeatStatus status(Seat seat) {
        return engine.status(seat);
    }

    @Override
    public void shutdown() {
        engine.shutdown();
    }
}
