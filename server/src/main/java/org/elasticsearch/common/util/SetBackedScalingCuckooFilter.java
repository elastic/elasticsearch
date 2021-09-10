/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
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
 * when it does not (has false positives).  Similar in usage to a Bloom Filter.
 * <p>
 * Internally, the datastructure maintains a Set of hashes up to a specified threshold.  This provides
 * 100% accurate membership queries.
 * <p>
 * When the threshold is breached, a list of CuckooFilters are created and used to track membership.
 * These filters are approximate similar to Bloom Filters.
 * <p>
 * This datastructure scales as more values are inserted by growing the list of CuckooFilters.
 * Final size is dependent on the cardinality of data inserted, and the precision specified.
 */
public class SetBackedScalingCuckooFilter implements Writeable {

    /**
     * This is the estimated insertion capacity for each individual internal CuckooFilter.
     */
    private static final int FILTER_CAPACITY = 1000000;

    /**
     * This set is used to track the insertions before we convert over to an approximate
     * filter. This gives us 100% accuracy for small cardinalities.  This will be null
     * if isSetMode = false;
     *
     * package-private for testing
     */
    Set<Long> hashes;

    /**
     * This list holds our approximate filters, after we have migrated out of a set.
     * This will be null if isSetMode = true;
     */
    List<CuckooFilter> filters;

    private final int threshold;
    private final Random rng;
    private final int capacity;
    private final double fpp;
    private Consumer<Long> breaker = aLong -> {
        //noop
    };

    // cached here for performance reasons
    private int numBuckets = 0;
    private int bitsPerEntry = 0;
    private int fingerprintMask = 0;
    private MurmurHash3.Hash128 scratchHash = new MurmurHash3.Hash128();

    // True if we are tracking inserts with a set, false otherwise
    private boolean isSetMode = true;

    /**
     * @param threshold The number of distinct values that should be tracked
     *                  before converting to an approximate representation
     * @param rng A random number generator needed for the cuckoo hashing process
     * @param fpp the false-positive rate that should be used for the cuckoo filters.
     */
    public SetBackedScalingCuckooFilter(int threshold, Random rng, double fpp) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("[threshold] must be a positive integer");
        }

        // We have to ensure that, in the worst case, two full sets can be converted into
        // one cuckoo filter without overflowing.  This keeps merging logic simpler
        if (threshold * 2 > FILTER_CAPACITY) {
            throw new IllegalArgumentException("[threshold] must be smaller than [" + (FILTER_CAPACITY / 2) + "]");
        }
        if (fpp < 0) {
            throw new IllegalArgumentException("[fpp] must be a positive double");
        }
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
            this.hashes = in.readSet(StreamInput::readZLong);
        } else {
            this.filters = in.readList(in12 -> new CuckooFilter(in12, rng));
            this.numBuckets = filters.get(0).getNumBuckets();
            this.fingerprintMask = filters.get(0).getFingerprintMask();
            this.bitsPerEntry = filters.get(0).getBitsPerEntry();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(threshold);
        out.writeBoolean(isSetMode);
        out.writeVInt(capacity);
        out.writeDouble(fpp);
        if (isSetMode) {
            out.writeCollection(hashes, StreamOutput::writeZLong);
        } else {
            out.writeList(filters);
        }
    }

    /**
     * Returns the number of distinct values that are tracked before converting to an approximate representation.
     * */
    public int getThreshold() {
        return threshold;
    }

    /**
     * Returns the random number generator used for the cuckoo hashing process.
     * */
    public Random getRng() {
        return rng;
    }

    /**
     * Returns the false-positive rate used for the cuckoo filters.
     * */
    public double getFpp() {
        return fpp;
    }

    /**
     * Registers a circuit breaker with the datastructure.
     *
     * CuckooFilter's can "saturate" and refuse to accept any new values.  When this happens,
     * the datastructure scales by adding a new filter.  This new filter's bytes will be tracked
     * in the registered breaker when configured.
     */
    public void registerBreaker(Consumer<Long> breaker) {
        this.breaker = Objects.requireNonNull(breaker, "Circuit Breaker Consumer cannot be null");
        breaker.accept(getSizeInBytes());
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    public boolean mightContain(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, scratchHash);
        return mightContainHash(hash.h1);
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    public boolean mightContain(long value) {
        long hash = MurmurHash3.murmur64(value);
        return mightContainHash(hash);
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    private boolean mightContainHash(long hash) {
        if (isSetMode) {
            return hashes.contains(hash);
        }

        // We calculate these once up front for all the filters and use the expert API
        int bucket = CuckooFilter.hashToIndex((int) hash, numBuckets);
        int fingerprint = CuckooFilter.fingerprint((int) (hash >> 32), bitsPerEntry, fingerprintMask);
        int alternateIndex = CuckooFilter.alternateIndex(bucket, fingerprint, numBuckets);

        for (CuckooFilter filter : filters) {
            if (filter.mightContainFingerprint(bucket, fingerprint, alternateIndex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if any of the filters contain this fingerprint at the specified bucket.
     * This is an expert-level API since it is dealing with buckets and fingerprints, not raw values
     * being hashed.
     */
    private boolean mightContainFingerprint(int bucket, int fingerprint) {
        int alternateIndex = CuckooFilter.alternateIndex(bucket, fingerprint, numBuckets);
        for (CuckooFilter filter : filters) {
            if (filter.mightContainFingerprint(bucket, fingerprint, alternateIndex)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Add's the provided value to the set for tracking
     */
    public void add(BytesRef value) {
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(value.bytes, value.offset, value.length, 0, scratchHash);
        addHash(hash.h1);
    }

    /**
     * Add's the provided value to the set for tracking
     */
    public void add(long value) {
        addHash(MurmurHash3.murmur64(value));
    }

    private void addHash(long hash) {
        if (isSetMode) {
            hashes.add(hash);
            maybeConvert();
            return;
        }

        boolean success = filters.get(filters.size() - 1).add(hash);
        if (success == false) {
            // filter is full, create a new one and insert there
            CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
            t.add(hash);
            filters.add(t);
            breaker.accept(t.getSizeInBytes()); // make sure we account for the new filter
        }
    }

    private void maybeConvert() {
        if (isSetMode && hashes.size() > threshold) {
            convert();
        }
    }

    /**
     * If we still holding values in a set, convert this filter into an approximate, cuckoo-backed filter.
     * This will create a list of CuckooFilters, and null out the set of hashes
     */
    void convert() {
        if (isSetMode == false) {
            throw new IllegalStateException("Cannot convert SetBackedScalingCuckooFilter to approximate " +
                "when it has already been converted.");
        }
        long oldSize = getSizeInBytes();

        filters = new ArrayList<>();
        CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
        // Cache the chosen numBuckets for later use
        numBuckets = t.getNumBuckets();
        fingerprintMask = t.getFingerprintMask();
        bitsPerEntry = t.getBitsPerEntry();

        hashes.forEach(t::add);
        filters.add(t);

        hashes = null;
        isSetMode = false;

        breaker.accept(-oldSize); // this zeros out the overhead of the set
        breaker.accept(getSizeInBytes()); // this adds back in the new overhead of the cuckoo filters

    }

    /**
     * Get the approximate size of this datastructure.  Approximate because only the Set occupants
     * are tracked, not the overhead of the Set itself.
     */
    public long getSizeInBytes() {
        long bytes = 13; // fpp (double), threshold (int), isSetMode (boolean)
        if (hashes != null) {
            bytes = (hashes.size() * 16);
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
        // Some basic sanity checks to make sure we can merge
        if (this.threshold != other.threshold) {
            throw new IllegalStateException("Cannot merge other CuckooFilter because thresholds do not match: ["
                + this.threshold + "] vs [" + other.threshold + "]");
        }
        if (this.capacity != other.capacity) {
            throw new IllegalStateException("Cannot merge other CuckooFilter because capacities do not match: ["
                + this.capacity + "] vs [" + other.capacity + "]");
        }
        if (this.fpp != other.fpp) {
            throw new IllegalStateException("Cannot merge other CuckooFilter because precisions do not match: ["
                + this.fpp + "] vs [" + other.fpp + "]");
        }

        if (isSetMode && other.isSetMode) {
            // Both in sets, merge collections then see if we need to convert to cuckoo
            hashes.addAll(other.hashes);
            maybeConvert();
        } else if (isSetMode && other.isSetMode == false) {
            // Other is in cuckoo mode, so we convert our set to a cuckoo, then
            // call the merge function again.  Since both are now in set-mode
            // this will fall through to the last conditional and do a cuckoo-cuckoo merge
            convert();
            merge(other);
        } else if (isSetMode == false && other.isSetMode) {
            // Rather than converting the other to a cuckoo first, we can just
            // replay the values directly into our filter.
            other.hashes.forEach(this::add);
        } else {
            // Both are in cuckoo mode, merge raw fingerprints

            CuckooFilter currentFilter = filters.get(filters.size() - 1);

            for (CuckooFilter otherFilter : other.filters) {

                // The iterator returns an array of longs corresponding to the
                // fingerprints for buckets at the current position
                Iterator<long[]> iter = otherFilter.getBuckets();
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
                        // Try to insert into the last filter in our list
                        if (currentFilter.mergeFingerprint(bucket, (int) fingerprint) == false) {
                            // if we failed, the filter is now saturated and we need to create a new one
                            CuckooFilter t = new CuckooFilter(capacity, fpp, rng);
                            filters.add(t);
                            breaker.accept(t.getSizeInBytes()); // make sure we account for the new filter

                            currentFilter = filters.get(filters.size() - 1);
                        }
                    }
                    bucket += 1;
                }
            }
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
