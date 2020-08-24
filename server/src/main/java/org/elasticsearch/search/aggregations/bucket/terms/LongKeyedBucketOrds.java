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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.util.LongLongHash;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;

/**
 * Maps long bucket keys to bucket ordinals.
 */
public abstract class LongKeyedBucketOrds implements Releasable {
    /**
     * Build a {@link LongKeyedBucketOrds}.
     */
    public static LongKeyedBucketOrds build(BigArrays bigArrays, CardinalityUpperBound cardinality) {
        // TODO nothing NONE?
        return cardinality != CardinalityUpperBound.MANY ? new FromSingle(bigArrays) : new FromMany(bigArrays);
    }

    private LongKeyedBucketOrds() {}

    /**
     * Add the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have yet to be added, or {@code -1-ord}
     * if they were already present.
     */
    public abstract long add(long owningBucketOrd, long value);

    /**
     * Count the buckets in {@code owningBucketOrd}.
     * <p>
     * Some aggregations expect this to be fast but most wouldn't
     * mind particularly if it weren't.
     */
    public abstract long bucketsInOrd(long owningBucketOrd);

    /**
     * Find the {@code owningBucketOrd, value} pair. Return the ord for
     * their bucket if they have been added or {@code -1} if they haven't.
     */
   public abstract long find(long owningBucketOrd, long value);

    /**
     * The number of collected buckets.
     */
    public abstract long size();

    /**
     * The maximum possible used {@code owningBucketOrd}.
     */
    public abstract long maxOwningBucketOrd();

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
    public static class FromSingle extends LongKeyedBucketOrds {
        private final LongHash ords;

        public FromSingle(BigArrays bigArrays) {
            ords = new LongHash(1, bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            assert owningBucketOrd == 0;
            return ords.add(value);
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            assert owningBucketOrd == 0;
            return ords.find(value);
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
        public long maxOwningBucketOrd() {
            return 0;
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
    public static class FromMany extends LongKeyedBucketOrds {
        private final LongLongHash ords;

        public FromMany(BigArrays bigArrays) {
            ords = new LongLongHash(2, bigArrays);
        }

        @Override
        public long add(long owningBucketOrd, long value) {
            // This is in the critical path for collecting most aggs. Be careful of performance.
            return ords.add(owningBucketOrd, value);
        }

        @Override
        public long find(long owningBucketOrd, long value) {
            return ords.find(owningBucketOrd, value);
        }

        @Override
        public long bucketsInOrd(long owningBucketOrd) {
            // TODO it'd be faster to count the number of buckets in a list of these ords rather than one at a time
            long count = 0;
            for (long i = 0; i < ords.size(); i++) {
                if (ords.getKey1(i) == owningBucketOrd) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public long size() {
            return ords.size();
        }

        @Override
        public long maxOwningBucketOrd() {
            // TODO this is fairly expensive to compute. Can we avoid needing it?
            long max = -1;
            for (long i = 0; i < ords.size(); i++) {
                max = Math.max(max, ords.getKey1(i));
            }
            return max;
        }

        @Override
        public BucketOrdsEnum ordsEnum(long owningBucketOrd) {
            // TODO it'd be faster to iterate many ords at once rather than one at a time
            return new BucketOrdsEnum() {
                private long ord = -1;
                private long value;

                @Override
                public boolean next() {
                    while (true) {
                        ord++;
                        if (ord >= ords.size()) {
                            return false;
                        }
                        if (ords.getKey1(ord) == owningBucketOrd) {
                            value = ords.getKey2(ord);
                            return true;
                        }
                    }
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
}
