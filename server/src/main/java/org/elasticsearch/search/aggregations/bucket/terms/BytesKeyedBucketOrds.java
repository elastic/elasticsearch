/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;

/**
 * Maps {@link BytesRef} bucket keys to bucket ordinals.
 */
public abstract class BytesKeyedBucketOrds implements Releasable {
    /**
     * Build a {@link LongKeyedBucketOrds}.
     */
    public static BytesKeyedBucketOrds build(BigArrays bigArrays, CardinalityUpperBound cardinality) {
        return cardinality.map(estimate -> estimate < 2 ? new FromSingle(bigArrays) : new FromMany(bigArrays));
    }

    private BytesKeyedBucketOrds() {}

    /**
     * Add the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have yet to be added, or {@code -1-ord}
     * if they were already present.
     */
    public abstract long add(long owningBucketOrd, BytesRef value);

    /**
     * Count the buckets in {@code owningBucketOrd}.
     */
    public abstract long bucketsInOrd(long owningBucketOrd);

    /**
     * The number of collected buckets.
     */
    public abstract long size();

    /**
     * Build an iterator for buckets inside {@code owningBucketOrd} in order
     * of increasing ord.
     * <p>
     * When this is first returns it is "unpositioned" and you must call
     * {@link BucketOrdsEnum#next()} to move it to the first value.
     */
    public abstract BucketOrdsEnum ordsEnum(long owningBucketOrd);

    /**
     * An iterator for buckets inside a particular {@code owningBucketOrd}.
     */
    public interface BucketOrdsEnum {
        /**
         * Advance to the next value.
         * @return {@code true} if there *is* a next value,
         *         {@code false} if there isn't
         */
        boolean next();

        /**
         * The ordinal of the current value.
         */
        long ord();

        /**
         * Read the current value.
         */
        void readValue(BytesRef dest);

        /**
         * An {@linkplain BucketOrdsEnum} that is empty.
         */
        BucketOrdsEnum EMPTY = new BucketOrdsEnum() {
            @Override
            public boolean next() {
                return false;
            }

            @Override
            public long ord() {
                return 0;
            }

            @Override
            public void readValue(BytesRef dest) {}
        };
    }

    /**
     * Implementation that only works if it is collecting from a single bucket.
     */
    private static class FromSingle extends BytesKeyedBucketOrds {
        private final BytesRefHash ords;

        private FromSingle(BigArrays bigArrays) {
            ords = new BytesRefHash(1, bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, BytesRef value) {
            assert owningBucketOrd == 0;
            return ords.add(value);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            return ords.size();
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            return new BucketOrdsEnum() {
                private int ord = -1;

                @Override
                public boolean next() {
                    ord++;
                    return ord < ords.size();
                }

                @Override
                public long ord() {
                    return ord;
                }

                @Override
                public void readValue(BytesRef dest) {
                    ords.get(ord, dest);
                }
            };
        }

        @Override
        public void close() {
            ords.close();
        }
    }

    /**
     * Implementation that works properly when collecting from many buckets.
     */
    private static class FromMany extends BytesKeyedBucketOrds {
        // TODO we can almost certainly do better here by building something fit for purpose rather than trying to lego together stuff
        private final BytesRefHash bytesToLong;
        private final LongKeyedBucketOrds longToBucketOrds;

        private FromMany(BigArrays bigArrays) {
            bytesToLong = new BytesRefHash(1, bigArrays);
            boolean success = false;
            try {
                longToBucketOrds = LongKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.MANY);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        @Override
        public long add(long owningBucketOrd, BytesRef value) {
            long l = bytesToLong.add(value);
            if (l < 0) {
                l = -1 - l;
            }
            return longToBucketOrds.add(owningBucketOrd, l);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            return longToBucketOrds.bucketsInOrd(owningBucketOrd);
        }

        @Override
        public long size() {
            return longToBucketOrds.size();
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            LongKeyedBucketOrds.BucketOrdsEnum delegate = longToBucketOrds.ordsEnum(owningBucketOrd);
            return new BucketOrdsEnum() {
                @Override
                public boolean next() {
                    return delegate.next();
                }

                @Override
                public long ord() {
                    return delegate.ord();
                }

                @Override
                public void readValue(BytesRef dest) {
                    bytesToLong.get(delegate.value(), dest);
                }
            };
        }

        @Override
        public void close() {
            Releasables.close(bytesToLong, longToBucketOrds);
        }
    }
}
