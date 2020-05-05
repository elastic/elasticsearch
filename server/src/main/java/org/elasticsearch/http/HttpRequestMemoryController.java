/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class HttpRequestMemoryController implements Releasable {

    private static final int NOT_AGGREGATING = -1;

    private final Supplier<CircuitBreaker> circuitBreaker;
    private final Predicate<String> requestCanTripBreaker;

    private int bytesReceived = NOT_AGGREGATING;
    private String currentUri;
    private CircuitBreakingException breakingException;
    private boolean currentRequestCanTrip = true;

    public HttpRequestMemoryController(Supplier<CircuitBreaker> circuitBreaker, Predicate<String> requestCanTripBreaker) {
        this.circuitBreaker = circuitBreaker;
        this.requestCanTripBreaker = requestCanTripBreaker;
    }

    public void startRequest(String uri) {
        assert isAggregating() == false;
        this.currentUri = uri;
        this.bytesReceived = 0;
        this.currentRequestCanTrip = requestCanTripBreaker.test(uri);
    }

    public void bytesReceived(int bytes) {
        assert isAggregating();
        bytesReceived += bytes;
    }

    public Tuple<Releasable, CircuitBreakingException> finish() {
        assert isAggregating();
        final BreakerControl breakerControl = new BreakerControl(circuitBreaker);
        checkBreaker(bytesReceived, breakerControl);
        final CircuitBreakingException exceptionToReturn = breakingException;

        currentUri = null;
        breakingException = null;
        currentRequestCanTrip = true;
        bytesReceived = NOT_AGGREGATING;
        return new Tuple<>(breakerControl, exceptionToReturn);
    }

    @Override
    public void close() {
        // Nothing to do currently.
    }

    private boolean isAggregating() {
        return bytesReceived != NOT_AGGREGATING;
    }

    private void checkBreaker(final int contentLength, final BreakerControl breakerControl) {

        if (currentRequestCanTrip) {
            try {
                circuitBreaker.get().addEstimateBytesAndMaybeBreak(contentLength, currentUri);
                breakerControl.setReservedBytes(contentLength);
            } catch (CircuitBreakingException e) {
                breakingException = e;
            }
        } else {
            circuitBreaker.get().addWithoutBreaking(contentLength);
            breakerControl.setReservedBytes(contentLength);
        }
    }

    private static class BreakerControl implements Releasable {

        private static final int CLOSED = -1;

        private final Supplier<CircuitBreaker> circuitBreaker;
        private final AtomicInteger bytesToRelease = new AtomicInteger(0);

        private BreakerControl(Supplier<CircuitBreaker> circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }

        private void setReservedBytes(int reservedBytes) {
            final boolean set = bytesToRelease.compareAndSet(0, reservedBytes);
            assert set : "Expected bytesToRelease to be 0, found " + bytesToRelease.get();
        }

        @Override
        public void close() {
            final int toRelease = bytesToRelease.getAndSet(CLOSED);
            assert toRelease != CLOSED;
            if (toRelease > 0) {
                circuitBreaker.get().addWithoutBreaking(-toRelease);
            }
        }
    }
}
