package org.elasticsearch.common.breaker;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;

/**
 * {@link CircuitBreakerService} that preallocates some bytes on construction.
 * Use this when you know you'll be allocating many small 
 */
public class PreallocatedCircuitBreakerService extends CircuitBreakerService implements Releasable {
    private final CircuitBreakerService next;
    private final PreallocedCircuitBreaker preallocated;

    public PreallocatedCircuitBreakerService(CircuitBreakerService next, String breakerToPreallocate, long bytesToPreallocate) {
        CircuitBreaker nextBreaker = next.getBreaker(breakerToPreallocate);
        nextBreaker.addEstimateBytesAndMaybeBreak(bytesToPreallocate, "preallocate");
        this.next = next;
        this.preallocated = new PreallocedCircuitBreaker(nextBreaker, bytesToPreallocate);
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        if (name.equals(preallocated.getName())) {
            return preallocated;
        }
        return next.getBreaker(name);
    }

    @Override
    public AllCircuitBreakerStats stats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        preallocated.close();
    }

    private static class PreallocedCircuitBreaker implements CircuitBreaker, Releasable {
        private final CircuitBreaker next;
        private final long preallocated;
        private long preallocationUsed;

        PreallocedCircuitBreaker(CircuitBreaker next, long preallocated) {
            this.next = next;
            this.preallocated = preallocated;
        }

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {
            next.circuitBreak(fieldName, bytesNeeded);
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (preallocationUsed == preallocated) {
                return next.addEstimateBytesAndMaybeBreak(bytes, label);
            }
            long newUsed = preallocationUsed + bytes;
            if (newUsed > preallocated) {
                preallocationUsed = preallocated;
                return next.addEstimateBytesAndMaybeBreak(newUsed - preallocated, label);
            }
            // This is the fast case. No volatile reads or writes here, ma!
            preallocationUsed = newUsed;
            // We return garbage here but callers never use the result for anything interesting
            return 0;
        }

        @Override
        public long addWithoutBreaking(long bytes) {   //NOCOMMIT tests for giving back
            if (preallocationUsed == preallocated) {
                return next.addWithoutBreaking(bytes);
            }
            long newUsed = preallocationUsed + bytes;
            if (newUsed > preallocated) {
                preallocationUsed = preallocated;
                return next.addWithoutBreaking(newUsed - preallocated);
            }
            // This is the fast case. No volatile reads or writes here, ma!
            preallocationUsed = newUsed;
            // We return garbage here but callers never use the result for anything interesting
            return 0;
        }

        @Override
        public String getName() {
            return next.getName();
        }

        @Override
        public void close() {
            next.addWithoutBreaking(-preallocated);
        }

        @Override
        public long getUsed() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLimit() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getOverhead() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTrippedCount() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Durability getDurability() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {
            throw new UnsupportedOperationException();
        }
    }
}
