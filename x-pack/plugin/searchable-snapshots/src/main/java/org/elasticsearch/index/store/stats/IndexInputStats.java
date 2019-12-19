/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.stats;

import java.util.Objects;

public class IndexInputStats {

    private final long length;

    private final long openCount;
    private final long closeCount;
    private final long sliceCount;
    private final long cloneCount;

    private final Counter forwardSmallSeeks;
    private final Counter backwardSmallSeeks;
    private final Counter forwardLargeSeeks;
    private final Counter backwardLargeSeeks;
    private final Counter totalSeeks;

    private final Counter contiguousReads;
    private final Counter nonContiguousReads;
    private final Counter totalReads;

    public IndexInputStats(final long length, final long openings, final long closings, final long slicings, final long clonings,
                           final Counter forwardSmallSeeks, final Counter backwardSmallSeeks,
                           final Counter forwardLargeSeeks, final Counter backwardLargeSeeks,
                           final Counter contiguousReads, final Counter nonContiguousReads) {
        this.length = length;
        this.openCount = openings;
        this.closeCount = closings;
        this.sliceCount = slicings;
        this.cloneCount = clonings;
        this.forwardSmallSeeks = Objects.requireNonNull(forwardSmallSeeks);
        this.backwardSmallSeeks = Objects.requireNonNull(backwardSmallSeeks);
        this.forwardLargeSeeks = Objects.requireNonNull(forwardLargeSeeks);
        this.backwardLargeSeeks = Objects.requireNonNull(backwardLargeSeeks);
        this.contiguousReads = Objects.requireNonNull(contiguousReads);
        this.nonContiguousReads = Objects.requireNonNull(nonContiguousReads);
        this.totalReads = Counter.merge(contiguousReads, nonContiguousReads);
        Counter totalSmallSeeks = Counter.merge(forwardSmallSeeks, backwardSmallSeeks);
        Counter totalLargeSeeks = Counter.merge(forwardLargeSeeks, backwardLargeSeeks);
        this.totalSeeks = Counter.merge(totalSmallSeeks, totalLargeSeeks);
    }

    /**
     * @return the IndexInput's length
     */
    public long getLength() {
        return length;
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
     * @return the {@link Counter} associated with small forward seeks.
     */
    public Counter getForwardSmallSeeks() {
        return forwardSmallSeeks;
    }

    /**
     * @return the {@link Counter} associated with small backward seeks.
     */
    public Counter getBackwardSmallSeeks() {
        return backwardSmallSeeks;
    }

    /**
     * @return the {@link Counter} associated with large forward seeks.
     */
    public Counter getForwardLargeSeeks() {
        return forwardLargeSeeks;
    }

    /**
     * @return the {@link Counter} associated with large backward seeks.
     */
    public Counter getBackwardLargeSeeks() {
        return backwardLargeSeeks;
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

        @Override
        public boolean equals(Object other) {
            if (this == other)  {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Counter counter = (Counter) other;
            return count == counter.count &&
                total == counter.total &&
                min == counter.min &&
                max == counter.max;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, total, min, max);
        }

        @Override
        public String toString() {
            return "Counter{" +
                "count=" + count +
                ", total=" + total +
                ", min=" + min +
                ", max=" + max +
                '}';
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
