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

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

/**
 * An approximate set membership datastructure that scales as more unique values are inserted.
 * Can definitively say if a member does not exist (no false negatives), but may say an item exists
 * when it does not (has false negatives).  Similar in usage to a Bloom Filter.
 *
 * Internally, the datastructure maintains a Set of hashes up to a specified threshold.  This provides
 * 100% accurate membership queries.
 *
 * When the threshold is breached, a list of CuckooFilters are created and used to track membership.
 * These filters are approximate similar to Bloom Filters.
 *
 * This datastructure scales as more values are inserted by growing the list of CuckooFilters.
 * Final size is dependent on the cardinality of data inserted, and the precision specified.
 */
public class SetBackedScalingCuckooFilter implements Writeable {

    private static final int FILTER_CAPACITY = 1000000;

    // Package-private for testing
    Set<MurmurHash3.Hash128> hashes;
    List<CuckooFilter> filters;

    private final int threshold;
    private final Random rng;
    private final int capacity;
    private final double fpp;
    private Consumer<Long> breaker = aLong -> {
        //noop
    };
    private boolean isSetMode = true;

    /**
     * @param threshold The number of distinct values that should be tracked
     *                  before converting to an approximate representation
     * @param rng A random number generator needed for the cuckoo hashing process
     * @param fpp the false-positive rate that should be used for the cuckoo filters.
     */
    public SetBackedScalingCuckooFilter(int threshold, Random rng, double fpp) {
        this.hashes = new HashSet<>(threshold);
        this.threshold = threshold;
        this.rng = rng;
        this.capacity = FILTER_CAPACITY;
        this.fpp = fpp;
    }

    public SetBackedScalingCuckooFilter(StreamInput in, Random rng) throws IOException {
        this.threshold = in.readVInt();
        this.isSetMode = in.readBoolean();
        this.rng = rng;
        this.capacity = in.readVInt();
        this.fpp = in.readDouble();

        if (isSetMode) {
            this.hashes = in.readSet(in1 -> {
                MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
                hash.h1 = in1.readZLong();
                hash.h2 = in1.readZLong();
                return hash;
            });
        } else {
            this.filters = in.readList(in12 -> new CuckooFilter(in12, rng));
        }
    }

    public SetBackedScalingCuckooFilter(SetBackedScalingCuckooFilter other) {
        this.threshold = other.threshold;
        this.isSetMode = other.isSetMode;
        this.rng = other.rng;
        this.breaker = other.breaker;
        this.capacity = other.capacity;
        this.fpp = other.fpp;
        if (isSetMode) {
            this.hashes = new HashSet<>(other.hashes);
        } else {
            this.filters = new ArrayList<>(other.filters);
        }
    }

    /**
     * Registers a circuit breaker with the datastructure.
     *
     * CuckooFilter's can "saturate" and refuse to accept any new values.  When this happens,
     * the datastructure scales by adding a new filter.  This new filter's bytes will be tracked
     * in the registered breaker when configured.
     */
    public void registerBreaker(Consumer<Long> breaker) {
        this.breaker = breaker;
        breaker.accept(getSizeInBytes());
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    public boolean mightContain(BytesRef value) {
        return mightContain(value.bytes, value.offset, value.length);
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    public boolean mightContain(byte[] value) {
        return mightContain(value, 0, value.length);
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    public boolean mightContain(long value) {
        return mightContain(Numbers.longToBytes(value));
    }

    private boolean mightContain(byte[] bytes, int offset, int length) {
        return mightContain(MurmurHash3.hash128(bytes, offset, length, 0, new MurmurHash3.Hash128()));
    }

    private boolean mightContain(MurmurHash3.Hash128 hash) {
        if (isSetMode) {
            return hashes.contains(hash);
        }
        return filters.stream().anyMatch(filter -> filter.mightContain(hash));
    }

    /**
     * Returns true if any of the filters contain this fingerprint at the specified bucket.
     * This is an expert-level API since it is dealing with buckets and fingerprints, not raw values
     * being hashed.
     */
    private boolean mightContainFingerprint(int bucket, int fingerprint) {
        return filters.stream().anyMatch(filter -> filter.mightContainFingerprint(bucket, fingerprint));
    }

    /**
     * Add's the provided value to the set for tracking
     */
    public boolean add(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, new MurmurHash3.Hash128());
        return add(hash);
    }

    /**
     * Add's the provided value to the set for tracking
     */
    public boolean add(byte[] value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value, 0, value.length, 0, new MurmurHash3.Hash128());
        return add(hash);
    }

    /**
     * Add's the provided value to the set for tracking
     */
    public boolean add(long value) {
        return add(Numbers.longToBytes(value));
    }

    private boolean add(MurmurHash3.Hash128 hash) {
        if (isSetMode) {
            hashes.add(hash);
            if (hashes.size() > threshold) {
                convert();
            }
            return true;
        }

        boolean success = filters.get(filters.size() - 1).add(hash);
        if (success == false) {
            // filter is full, create a new one and insert there
            CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
            t.add(hash);
            filters.add(t);
            breaker.accept(t.getSizeInBytes()); // make sure we account for the new filter
        }
        return true;
    }

    /**
     * If we still holding values in a set, convert this filter into an approximate, cuckoo-backed filter.
     * This will create a list of CuckooFilters, and null out the set of hashes
     */
    private void convert() {
        if (isSetMode) {
            long oldSize = getSizeInBytes();

            filters = new ArrayList<>();
            CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
            hashes.forEach(t::add);
            filters.add(t);

            hashes = null;
            isSetMode = false;

            breaker.accept(-oldSize); // this zeros out the overhead of the set
            breaker.accept(getSizeInBytes()); // this adds back in the new overhead of the cuckoo filters
        }
    }

    /**
     * Get the approximate size of this datastructure.  Approximate because only the Set occupants
     * are tracked, not the overhead of the Set itself.
     */
    public long getSizeInBytes() {
        long bytes = 0;
        if (hashes != null) {
            bytes = (hashes.size() * 16) + 8 + 4 + 1;
        }
        if (filters != null) {
            bytes += filters.stream().mapToLong(CuckooFilter::getSizeInBytes).sum();
        }
        return bytes;
    }


    /**
     * Merge `other` cuckoo filter into this cuckoo.  After merging, this filter's state will
     * be the union of the two.  During the merging process, the internal Set may be upgraded
     * to a cuckoo if it goes over threshold
     */
    public void merge(SetBackedScalingCuckooFilter other) {
        if (isSetMode && other.isSetMode) {
            // Both in sets, merge collections then see if we need to convert to cuckoo
            hashes.addAll(other.hashes);
            if (hashes.size() > threshold) {
                convert();
            }
        } else if (isSetMode && other.isSetMode == false) {
            // Other is in cuckoo mode, so we convert our set to a cuckoo then merge collections.
            // We could probably get fancy and keep our side in set-mode, but simpler to just convert
            convert();
            filters.addAll(other.filters);
        } else if (isSetMode == false && other.isSetMode) {
            // Rather than converting the other to a cuckoo first, we can just
            // replay the values directly into our filter.
            other.hashes.forEach(this::add);
        } else {
            // Both are in cuckoo mode, merge raw fingerprints

            int current = 0;
            CuckooFilter currentFilter = filters.get(current);

            for (CuckooFilter filter : other.filters) {

                // The iterator returns an array of longs corresponding to the
                // fingerprints for buckets at the current position
                Iterator<long[]> iter = filter.getBuckets();
                int bucket = 0;
                while (iter.hasNext()) {
                    long[] fingerprints = iter.next();

                    // We check to see if the fingerprint is present in any of the existing filters
                    // (in the same bucket/alternate bucket), or if the fingerprint is empty.  In these cases
                    // we can skip the fingerprint
                    for (long fingerprint : fingerprints) {
                        if (fingerprint == CuckooFilter.EMPTY || mightContainFingerprint(bucket, (int) fingerprint)) {
                            continue;
                        }
                        boolean success = false;

                        // If the fingerprint is new, we try to merge it into the filter at our `current` pointer.
                        // This might fail (e.g. the filter is full), so we may have to try multiple times
                        while (success == false) {
                            success = currentFilter.mergeFingerprint(bucket, (int) fingerprint);

                            // If we failed to insert, the current filter is full, get next one
                            if (success == false) {
                                current += 1;

                                // if we're out of filters, we need to create a new one
                                if (current >= filters.size()) {
                                    CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
                                    filters.add(t);
                                    breaker.accept(t.getSizeInBytes()); // make sure we account for the new filter
                                }
                                currentFilter = filters.get(current);
                            }
                        }
                    }
                    bucket += 1;
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(threshold);
        out.writeBoolean(isSetMode);
        out.writeVInt(capacity);
        out.writeDouble(fpp);
        if (isSetMode) {
            out.writeCollection(hashes, (out1, hash) -> {
                out1.writeZLong(hash.h1);
                out1.writeZLong(hash.h2);
            });
        } else {
            out.writeList(filters);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashes, filters, threshold, isSetMode, capacity, fpp);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SetBackedScalingCuckooFilter that = (SetBackedScalingCuckooFilter) other;
        return Objects.equals(this.hashes, that.hashes)
            && Objects.equals(this.filters, that.filters)
            && Objects.equals(this.threshold, that.threshold)
            && Objects.equals(this.isSetMode, that.isSetMode)
            && Objects.equals(this.capacity, that.capacity)
            && Objects.equals(this.fpp, that.fpp);
    }
}
