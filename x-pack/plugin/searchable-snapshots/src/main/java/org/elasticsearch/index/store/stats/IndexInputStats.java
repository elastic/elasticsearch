/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.stats;

import java.util.Objects;

public class IndexInputStats {

    private final long openCount;
    private final long closeCount;
    private final long sliceCount;
    private final long cloneCount;

    private final Counter forwardSeeks;
    private final Counter backwardSeeks;
    private final Counter totalSeeks;

    private final Counter contiguousReads;
    private final Counter nonContiguousReads;
    private final Counter totalReads;

    public IndexInputStats(final long openings, final long closings, final long slicings, final long clonings,
                           final Counter forwardSeeks, final Counter backwardSeeks,
                           final Counter contiguousReads, final Counter nonContiguousReads) {
        this.openCount = openings;
        this.closeCount = closings;
        this.sliceCount = slicings;
        this.cloneCount = clonings;
        this.forwardSeeks = Objects.requireNonNull(forwardSeeks);
        this.backwardSeeks = Objects.requireNonNull(backwardSeeks);
        this.totalSeeks = Counter.merge(forwardSeeks, backwardSeeks);
        this.contiguousReads = Objects.requireNonNull(contiguousReads);
        this.nonContiguousReads = Objects.requireNonNull(nonContiguousReads);
        this.totalReads = Counter.merge(contiguousReads, nonContiguousReads);
    }

    /**
     * @return the number of times the IndexInput has been opened (does not include slice or clone openings).
     */
    public long getOpenCount() {
        return openCount;
    }

    /**
     * @return the number of times the IndexInput has been closed (does not include slice or clone closing).
     */
    public long getCloseCount() {
        return closeCount;
    }

    /**
     * @return the number of times the IndexInput has been sliced.
     */
    public long getSliceCount() {
        return sliceCount;
    }

    /**
     * @return the number of times the IndexInput has been cloned.
     */
    public long getCloneCount() {
        return cloneCount;
    }

    /**
     * @return the {@link Counter} associated with forward seeks.
     */
    public Counter getForwardSeeks() {
        return forwardSeeks;
    }

    /**
     * @return the {@link Counter} associated with backward seeks.
     */
    public Counter getBackwardSeeks() {
        return backwardSeeks;
    }

    /**
     * @return the {@link Counter} associated with all seeks.
     */
    public Counter getTotalSeeks() {
        return totalSeeks;
    }

    /**
     * @return the {@link Counter} associated with contiguous bytes reads, ie when
     * the IndexInput's file pointer did NOT change between reads.
     */
    public Counter getContiguousReads() {
        return contiguousReads;
    }

    /**
     * @return the {@link Counter} associated with non-contiguous bytes reads, ie when
     * the IndexInput's file pointer did change between reads.
     */
    public Counter getNonContiguousReads() {
        return nonContiguousReads;
    }

    /**
     * @return the {@link Counter} associated with all bytes reads.
     */
    public Counter getTotalReads() {
        return totalReads;
    }

    /**
     * Counter for long values
     */
    public static class Counter {

        private final long count;
        private final long total;
        private final long min;
        private final long max;

        public Counter(final long count, final long total, final long min, final long max) {
            this.count = count;
            this.total = total;
            this.min = min;
            this.max = max;
        }

        /**
         * @return the count of values
         */
        public long getCount() {
            return count;
        }

        /**
         * @return the sum of values
         */
        public long getTotal() {
            return total;
        }

        /**
         * @return the minimum value (or zero if none)
         */
        public long getMin() {
            return min;
        }

        /**
         * @return the maximum value (or zero if none)
         */
        public long getMax() {
            return max;
        }

        /**
         * @return rhe arithmetic mean of values (or zero if none)
         */
        public double getAverage() {
            return count == 0L ? 0.0d : (double) total / count;
        }

        static Counter merge(final Counter first, final Counter... others) {
            if (others == null) {
                return first;
            }
            Counter result = first;
            for (Counter other : others) {
                result = new Counter(
                    result.getCount() + other.getCount(),
                    result.getTotal() + other.getTotal(),
                    Math.min(result.getMin(), other.getMin()),
                    Math.max(result.getMax(), other.getMax()));
            }
            return result;
        }
    }
}
