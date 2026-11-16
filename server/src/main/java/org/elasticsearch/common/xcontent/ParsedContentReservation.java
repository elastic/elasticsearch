/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xcontent.XContentParser;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Charges a circuit breaker for the heap that parsing a request's content takes, and holds the charge until the response is sent, or
 * until the work that keeps the parsed request ends if that is later.
 *
 * <p>What a request is parsed into can be far larger than its content, since every token becomes at least one object. The in-flight
 * requests breaker only charges the content, so parsers from {@link #track} charge for what they read. A request that responds before
 * its work ends, such as an async search, keeps the parsed request after the response, so its work holds the charge until it ends, see
 * {@link #hold(ThreadContext)}. Other such requests, such as a task started with {@code wait_for_completion=false}, still release the
 * charge at the response.
 *
 * <p>A request can be read by many parsers, such as one per {@code _msearch} line. The reservation counts what they charge without a
 * check, so the request is checked once per chunk however small each parser is.
 */
public final class ParsedContentReservation implements Releasable {

    /** The label of these charges and releases. A circuit breaker exception reports it, and it is their breaker metrics category. */
    public static final String LABEL = ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST;

    /**
     * The thread context transient that carries the reservation of the REST request being handled, see
     * {@link #track(ThreadContext, XContentParser)}.
     */
    public static final String TRANSIENT_NAME = "_parsed_content_reservation";

    /** Marks a released reservation. Charges only add to the count, so it never reaches this. */
    private static final long RELEASED = Long.MIN_VALUE;

    private final CircuitBreaker breaker;
    private final boolean canTrip;
    /** One reference for the request, dropped by {@link #close}, and one for each hold. The last one releases the charge. */
    private final RefCounted refs = AbstractRefCounted.of(this::release);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong reserved = new AtomicLong();
    /** Charged without a check since the last check. */
    private final AtomicLong unchecked = new AtomicLong();

    /**
     * @param breaker the breaker to charge
     * @param canTrip whether a charge may trip the breaker, false for requests that must not be rejected for the memory they use
     */
    public ParsedContentReservation(CircuitBreaker breaker, boolean canTrip) {
        this.breaker = breaker;
        this.canTrip = canTrip;
    }

    /**
     * Returns a parser that reads from {@code parser} and charges this reservation for what it reads. Closing it closes {@code parser}.
     */
    public XContentParser track(XContentParser parser) {
        return new ChargingXContentParser(parser, this, false);
    }

    /**
     * Returns a parser that charges a reservation of its own, released when the parser is closed. For content parsed where no REST
     * request is handled, such as the source of a {@code wrapper} query on a data node. The breaker is checked while the content is
     * parsed, though what it is parsed into is not held charged after.
     */
    public static XContentParser trackWhileParsing(CircuitBreaker breaker, XContentParser parser) {
        return new ChargingXContentParser(parser, new ParsedContentReservation(breaker, true), true);
    }

    /**
     * Like {@link #track(XContentParser)} for the reservation of the REST request handled in {@code threadContext}, or returns
     * {@code parser} itself if there is none. For content parsed on the request's behalf later, such as the query inside a {@code wrapper}
     * query.
     */
    public static XContentParser track(ThreadContext threadContext, XContentParser parser) {
        final ParsedContentReservation reservation = threadContext.getTransient(TRANSIENT_NAME);
        return reservation == null ? parser : reservation.track(parser);
    }

    /**
     * Charges {@code bytes}, checking the limit unless the request must not trip it. The check also covers earlier charges made without
     * one. Once the charge is released, nothing is charged or checked.
     */
    void reserve(long bytes) {
        if (reserved.get() == RELEASED) {
            return;
        }
        // Taken before the check, so that two checks at the same time cannot both take it.
        final long priorUnchecked = unchecked.getAndSet(0);
        try {
            if (canTrip) {
                breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
            } else {
                breaker.addWithoutBreaking(bytes, LABEL);
            }
        } catch (CircuitBreakingException e) {
            unchecked.addAndGet(priorUnchecked);
            throw e;
        }
        record(bytes);
    }

    /**
     * Charges {@code bytes} without checking the limit, for memory already in use. The next check covers them. Once the charge is
     * released, nothing is charged.
     */
    void reserveWithoutBreaking(long bytes) {
        if (reserved.get() == RELEASED) {
            return;
        }
        breaker.addWithoutBreaking(bytes, LABEL);
        unchecked.addAndGet(bytes);
        record(bytes);
    }

    /**
     * The bytes charged without a check since the last check. A parser is checked once these and its own charge reach a chunk.
     */
    long uncheckedBytes() {
        return unchecked.get();
    }

    private void record(long bytes) {
        // A forked parser can still charge after its request completed. A charge that raced with the release is released at once.
        if (reserved.getAndUpdate(r -> r == RELEASED ? RELEASED : r + bytes) == RELEASED) {
            breaker.addWithoutBreaking(-bytes, LABEL);
        }
    }

    /**
     * The bytes charged, until the charge is released.
     */
    public long reservedBytes() {
        final long r = reserved.get();
        return r == RELEASED ? 0 : r;
    }

    /**
     * Holds the charge until the returned releasable is closed, even if this reservation is closed before. Charges made meanwhile are
     * held too. Does nothing if the charge is released already.
     */
    Releasable hold() {
        return refs.tryIncRef() ? Releasables.releaseOnce(refs::decRef) : () -> {};
    }

    /**
     * Holds the charge of the reservation of the REST request handled in {@code threadContext} until the returned releasable is closed,
     * even if the reservation is closed before. Charges made meanwhile are held too. For work that keeps the parsed request after the
     * response is sent, such as an async search. Does nothing if there is no reservation, or its charge is released already.
     */
    public static Releasable hold(ThreadContext threadContext) {
        final ParsedContentReservation reservation = threadContext.getTransient(TRANSIENT_NAME);
        return reservation == null ? () -> {} : reservation.hold();
    }

    /**
     * Drops the request's own reference. Once no {@link #hold(ThreadContext) hold} remains either, everything charged is released and
     * later charges are ignored.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            refs.decRef();
        }
    }

    private void release() {
        final long toRelease = reserved.getAndSet(RELEASED);
        if (toRelease > 0) {
            breaker.addWithoutBreaking(-toRelease, LABEL);
        }
    }
}
