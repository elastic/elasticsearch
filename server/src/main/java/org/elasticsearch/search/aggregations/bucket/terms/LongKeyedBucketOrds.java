/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.ObjectArray;

/**
 * Maps long bucket keys to bucket ordinals.
 */
public abstract class LongKeyedBucketOrds implements Releasable {
    /**
     * Build a {@link LongKeyedBucketOrds}.
     */
    public static LongKeyedBucketOrds build(BigArrays bigArrays, boolean collectsFromSingleBucket) {
        return collectsFromSingleBucket ? new FromSingle(bigArrays) : new FromMany(bigArrays);
    }

    private LongKeyedBucketOrds() {}

    /**
     * Add the {@code owningBucketOrd, term} pair. Return the ord for
     * their bucket if they have yet to be added, or {@code -1-ord}
     * if they were already present.
     */
    public abstract long add(long owningBucketOrd, long value);

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
         * The current value.
         */
        long value();

        /**
         * An {@linkplain BucketOrdsEnum} that is empty. 
         */
        BucketOrdsEnum EMPTY = new BucketOrdsEnum() {
            @Override
            public boolean next() { return false; }
            @Override
            public long ord() { return 0; }
            @Override
            public long value() { return 0; }
        };
    }


    /**
     * Implementation that only works if it is collecting from a single bucket.
     */
    private static class FromSingle extends LongKeyedBucketOrds {
        private final LongHash ords;

        FromSingle(BigArrays bigArrays) {
            ords = new LongHash(1, bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            assert owningBucketOrd == 0;
            return ords.add(value);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return ords.size();
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            assert owningBucketOrd == 0;
            return new BucketOrdsEnum() {
                private long ord = -1;
                private long value;

                @Override
                public boolean next() {
                    ord++;
                    if (ord >= ords.size()) {
                        return false;
                    }
                    value = ords.get(ord);
                    return true;
                }

                @Override
                public long value() {
                    return value;
                }

                @Override
                public long ord() {
                    return ord;
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
    private static class FromMany extends LongKeyedBucketOrds {
        // TODO we can almost certainly do better here by building something fit for purpose rather than trying to lego together stuff
        private static class Buckets implements Releasable {
            private final LongHash valueToThisBucketOrd;
            private LongArray thisBucketOrdToGlobalOrd;

            Buckets(BigArrays bigArrays) {
                valueToThisBucketOrd = new LongHash(1, bigArrays);
                thisBucketOrdToGlobalOrd = bigArrays.newLongArray(1, false);
            }

            @Override
            public void close() {
                Releasables.close(valueToThisBucketOrd, thisBucketOrdToGlobalOrd);
            }
        }
        private final BigArrays bigArrays; 
        private ObjectArray<Buckets> owningOrdToBuckets;
        private long lastGlobalOrd = -1;

        FromMany(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            owningOrdToBuckets = bigArrays.newObjectArray(1);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            Buckets buckets = bucketsForOrd(owningBucketOrd);
            long thisBucketOrd = buckets.valueToThisBucketOrd.add(value);
            if (thisBucketOrd < 0) {
                // Already in the hash
                thisBucketOrd = -1 - thisBucketOrd;
                return -1 - buckets.thisBucketOrdToGlobalOrd.get(thisBucketOrd);
            }
            buckets.thisBucketOrdToGlobalOrd = bigArrays.grow(buckets.thisBucketOrdToGlobalOrd, thisBucketOrd + 1);
            lastGlobalOrd++;
            buckets.thisBucketOrdToGlobalOrd.set(thisBucketOrd, lastGlobalOrd);
            return lastGlobalOrd;
        }

        private Buckets bucketsForOrd(long owningBucketOrd) {
            if (owningOrdToBuckets.size() <= owningBucketOrd) {
                owningOrdToBuckets = bigArrays.grow(owningOrdToBuckets, owningBucketOrd + 1); 
                Buckets buckets = new Buckets(bigArrays);
                owningOrdToBuckets.set(owningBucketOrd, buckets);
                return buckets;
            }
            Buckets buckets = owningOrdToBuckets.get(owningBucketOrd);
            if (buckets == null) {
                buckets = new Buckets(bigArrays);
                owningOrdToBuckets.set(owningBucketOrd, buckets);
            }
            return buckets;
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            if (owningBucketOrd >= owningOrdToBuckets.size()) {
                return 0;
            }
            Buckets buckets = owningOrdToBuckets.get(owningBucketOrd);
            if (buckets == null) {
                return 0;
            }
            return buckets.valueToThisBucketOrd.size();
        }

        @Override
        public long size() {
            return lastGlobalOrd + 1;
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            if (owningBucketOrd >= owningOrdToBuckets.size()) {
                return BucketOrdsEnum.EMPTY;
            }
            Buckets buckets = owningOrdToBuckets.get(owningBucketOrd);
            if (buckets == null) {
                return BucketOrdsEnum.EMPTY;
            }
            return new BucketOrdsEnum() {
                private long thisBucketOrd = -1;
                private long value;
                private long ord;

                @Override
                public boolean next() {
                    thisBucketOrd++;
                    if (thisBucketOrd >= buckets.valueToThisBucketOrd.size()) {
                        return false;
                    }
                    value = buckets.valueToThisBucketOrd.get(thisBucketOrd);
                    ord = buckets.thisBucketOrdToGlobalOrd.get(thisBucketOrd);
                    return true;
                }

                @Override
                public long value() {
                    return value;
                }

                @Override
                public long ord() {
                    return ord;
                }
            };
        }

        @Override
        public void close() {
            for (long owningBucketOrd = 0; owningBucketOrd < owningOrdToBuckets.size(); owningBucketOrd++) {
                Buckets buckets = owningOrdToBuckets.get(owningBucketOrd);
                if (buckets != null) {
                    buckets.close();
                }
            }
            owningOrdToBuckets.close();
        }
    }
}
