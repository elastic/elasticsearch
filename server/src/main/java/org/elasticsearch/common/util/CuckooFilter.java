/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;

/**
 * An approximate set membership datastructure
 *
 * CuckooFilters are similar to Bloom Filters in usage; values are inserted, and the Cuckoo
 * can be asked if it has seen a particular value before.  Because the structure is approximate,
 * it can return false positives (says it has seen an item when it has not).  False negatives
 * are not possible though; if the structure says it _has not_ seen an item, that can be
 * trusted.
 *
 * The filter can "saturate" at which point the map has hit it's configured load factor (or near enough
 * that a large number of evictions are not able to find a free slot) and will refuse to accept
 * any new insertions.
 *
 * NOTE: this version does not support deletions, and as such does not save duplicate
 * fingerprints (e.g. when inserting, if the fingerprint is already present in the
 * candidate buckets, it is not inserted).  By not saving duplicates, the CuckooFilter
 * loses the ability to delete values.  But not by allowing deletions, we can save space
 * (do not need to waste slots on duplicate fingerprints), and we do not need to worry
 * about inserts "overflowing" a bucket because the same item has been repeated repeatedly
 *
 * NOTE: this CuckooFilter exposes a number of Expert APIs which assume the caller has
 * intimate knowledge about how the algorithm works.  It is recommended to use
 * {@link SetBackedScalingCuckooFilter} instead.
 *
 * Based on the paper:
 *
 * Fan, Bin, et al. "Cuckoo filter: Practically better than bloom."
 * Proceedings of the 10th ACM International on Conference on emerging Networking Experiments and Technologies. ACM, 2014.
 *
 * https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
 */
public class CuckooFilter implements Writeable {

    private static final double LN_2 = Math.log(2);
    private static final int MAX_EVICTIONS = 500;
    static final int EMPTY = 0;

    private final PackedArray data;
    private final int numBuckets;
    private final int bitsPerEntry;
    private final int fingerprintMask;
    private final int entriesPerBucket;
    private final Random rng;
    private int count;
    private int evictedFingerprint = EMPTY;

    /**
     * @param capacity The number of expected inserts.  The filter can hold more than this value, it is just an estimate
     * @param fpp The desired false positive rate.  Smaller values will reduce the
     *            false positives at expense of larger size
     * @param rng A random number generator, used with the cuckoo hashing process
     */
    CuckooFilter(long capacity, double fpp, Random rng) {
        this.rng = rng;
        this.entriesPerBucket = entriesPerBucket(fpp);
        double loadFactor = getLoadFactor(entriesPerBucket);
        this.bitsPerEntry = bitsPerEntry(fpp, entriesPerBucket);
        this.numBuckets = getNumBuckets(capacity, loadFactor, entriesPerBucket);

        if ((long) numBuckets * (long) entriesPerBucket > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Attempted to create [" + numBuckets * entriesPerBucket + "] entries which is > Integer.MAX_VALUE"
            );
        }
        this.data = new PackedArray(numBuckets * entriesPerBucket, bitsPerEntry);

        // puts the bits at the right side of the mask, e.g. `0000000000001111` for bitsPerEntry = 4
        this.fingerprintMask = (0x80000000 >> (bitsPerEntry - 1)) >>> (Integer.SIZE - bitsPerEntry);
    }

    /**
     * This ctor is likely slow and should only be used for testing
     */
    CuckooFilter(CuckooFilter other) {
        this.numBuckets = other.numBuckets;
        this.bitsPerEntry = other.bitsPerEntry;
        this.entriesPerBucket = other.entriesPerBucket;
        this.count = other.count;
        this.evictedFingerprint = other.evictedFingerprint;
        this.rng = other.rng;
        this.fingerprintMask = other.fingerprintMask;

        // This shouldn't happen, but as a sanity check
        if ((long) numBuckets * (long) entriesPerBucket > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "Attempted to create [" + numBuckets * entriesPerBucket + "] entries which is > Integer.MAX_VALUE"
            );
        }
        // TODO this is probably super slow, but just used for testing atm
        this.data = new PackedArray(numBuckets * entriesPerBucket, bitsPerEntry);
        for (int i = 0; i < other.data.size(); i++) {
            data.set(i, other.data.get(i));
        }
    }

    CuckooFilter(StreamInput in, Random rng) throws IOException {
        this.numBuckets = in.readVInt();
        this.bitsPerEntry = in.readVInt();
        this.entriesPerBucket = in.readVInt();
        this.count = in.readVInt();
        this.evictedFingerprint = in.readVInt();
        this.rng = rng;

        this.fingerprintMask = (0x80000000 >> (bitsPerEntry - 1)) >>> (Integer.SIZE - bitsPerEntry);
        this.data = new PackedArray(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numBuckets);
        out.writeVInt(bitsPerEntry);
        out.writeVInt(entriesPerBucket);
        out.writeVInt(count);
        out.writeVInt(evictedFingerprint);
        this.data.save(out);
    }

    /**
     * Get the number of unique items that are being tracked
     */
    public int getCount() {
        return count;
    }

    /**
     * Returns the number of buckets that has been chosen based
     * on the initial configuration
     *
     * Expert-level API
     */
    int getNumBuckets() {
        return numBuckets;
    }

    /**
     * Returns the number of bits used per entry
     *
     * Expert-level API
     */
    int getBitsPerEntry() {
        return bitsPerEntry;
    }

    /**
     * Returns the cached fingerprint mask.  This is simply a mask for the
     * first bitsPerEntry bits, used by {@link CuckooFilter#fingerprint(int, int, int)}
     * to generate the fingerprint of a hash
     *
     * Expert-level API
     */
    int getFingerprintMask() {
        return fingerprintMask;
    }

    /**
     * Returns an iterator that returns the long[] representation of each bucket.  The value
     * inside each long will be a fingerprint (or 0L, representing empty).
     *
     * Expert-level API
     */
    Iterator<long[]> getBuckets() {
        return new Iterator<>() {
            int current = 0;

            @Override
            public boolean hasNext() {
                return current < numBuckets;
            }

            @Override
            public long[] next() {
                long[] values = new long[entriesPerBucket];
                int offset = getOffset(current, 0);
                data.get(offset, values, 0, entriesPerBucket);
                current += 1;
                return values;
            }
        };
    }

    /**
     * Returns true if the set might contain the provided value, false otherwise.  False values are
     * 100% accurate, while true values may be a false-positive.
     */
    boolean mightContain(long hash) {
        int bucket = hashToIndex((int) hash, numBuckets);
        int fingerprint = fingerprint((int) (hash >>> 32), bitsPerEntry, fingerprintMask);
        int alternateIndex = alternateIndex(bucket, fingerprint, numBuckets);

        return mightContainFingerprint(bucket, fingerprint, alternateIndex);
    }

    /**
     * Returns true if the bucket or it's alternate bucket contains the fingerprint.
     *
     * Expert-level API, use {@link CuckooFilter#mightContain(long)} to check if
     * a value is in the filter.
     */
    boolean mightContainFingerprint(int bucket, int fingerprint, int alternateBucket) {

        // check all entries for both buckets and the evicted slot
        return hasFingerprint(bucket, fingerprint) || hasFingerprint(alternateBucket, fingerprint) || evictedFingerprint == fingerprint;
    }

    /**
     * Return's true if any of the entries in the bucket contain the fingerprint
     */
    private boolean hasFingerprint(int bucket, long fingerprint) {
        long[] values = new long[entriesPerBucket];
        int offset = getOffset(bucket, 0);
        data.get(offset, values, 0, entriesPerBucket);

        for (int i = 0; i < entriesPerBucket; i++) {
            if (values[i] == fingerprint) {
                return true;
            }
        }
        return false;
    }

    /**
     * Add's the hash to the bucket or alternate bucket.  Returns true if the insertion was
     * successful, false if the filter is saturated.
     */
    boolean add(long hash) {
        // Each bucket needs 32 bits, so we truncate for the first bucket and shift/truncate for second
        int bucket = hashToIndex((int) hash, numBuckets);
        int fingerprint = fingerprint((int) (hash >>> 32), bitsPerEntry, fingerprintMask);
        return mergeFingerprint(bucket, fingerprint);
    }

    /**
     * Attempts to merge the fingerprint into the specified bucket or it's alternate bucket.
     * Returns true if the insertion was successful, false if the filter is saturated.
     *
     * Expert-level API, use {@link CuckooFilter#add(long)} to insert
     * values into the filter
     */
    boolean mergeFingerprint(int bucket, int fingerprint) {
        // If we already have an evicted fingerprint we are full, no need to try
        if (evictedFingerprint != EMPTY) {
            return false;
        }

        int alternateBucket = alternateIndex(bucket, fingerprint, numBuckets);
        if (tryInsert(bucket, fingerprint) || tryInsert(alternateBucket, fingerprint)) {
            count += 1;
            return true;
        }

        for (int i = 0; i < MAX_EVICTIONS; i++) {
            // overwrite our alternate bucket, and a random entry
            int offset = getOffset(alternateBucket, rng.nextInt(entriesPerBucket - 1));
            int oldFingerprint = (int) data.get(offset);
            data.set(offset, fingerprint);

            // replace details and start again
            fingerprint = oldFingerprint;
            bucket = alternateBucket;
            alternateBucket = alternateIndex(bucket, fingerprint, numBuckets);

            // Only try to insert into alternate bucket
            if (tryInsert(alternateBucket, fingerprint)) {
                count += 1;
                return true;
            }
        }

        // If we get this far, we failed to insert the value after MAX_EVICTION rounds,
        // so cache the last evicted value (so we don't lose it) and signal we failed
        evictedFingerprint = fingerprint;
        return false;
    }

    /**
     * Low-level insert method. Attempts to write the fingerprint into an empty entry
     * at this bucket's position.  Returns true if that was sucessful, false if all entries
     * were occupied.
     *
     * If the fingerprint already exists in one of the entries, it will not duplicate the
     * fingerprint like the original paper.  This means the filter _cannot_ support deletes,
     * but is not sensitive to "overflowing" buckets with repeated inserts
     */
    private boolean tryInsert(int bucket, int fingerprint) {
        long[] values = new long[entriesPerBucket];
        int offset = getOffset(bucket, 0);
        data.get(offset, values, 0, entriesPerBucket);

        // TODO implement semi-sorting
        for (int i = 0; i < values.length; i++) {
            if (values[i] == EMPTY) {
                data.set(offset + i, fingerprint);
                return true;
            } else if (values[i] == fingerprint) {
                // Already have the fingerprint, no need to save
                return true;
            }
        }
        return false;
    }

    /**
     * Converts a hash into a bucket index (primary or alternate).
     *
     * If the hash is negative, this flips the bits.  The hash is then modulo numBuckets
     * to get the final index.
     *
     * Expert-level API
     */
    static int hashToIndex(int hash, int numBuckets) {
        return hash & (numBuckets - 1);
    }

    /**
     * Calculates the alternate bucket for a given bucket:fingerprint tuple
     *
     * The alternate bucket is the fingerprint multiplied by a mixing constant,
     * then xor'd against the bucket.  This new value is modulo'd against
     * the buckets via {@link CuckooFilter#hashToIndex(int, int)} to get the final
     * index.
     *
     * Note that the xor makes this operation reversible as long as we have the
     * fingerprint and current bucket (regardless of if that bucket was the primary
     * or alternate).
     *
     * Expert-level API
     */
    static int alternateIndex(int bucket, int fingerprint, int numBuckets) {
        /*
            Reference impl uses murmur2 mixing constant:
            https://github.com/efficient/cuckoofilter/blob/master/src/cuckoofilter.h#L78
                // NOTE(binfan): originally we use:
                // index ^ HashUtil::BobHash((const void*) (&tag), 4)) & table_->INDEXMASK;
                // now doing a quick-n-dirty way:
                // 0x5bd1e995 is the hash constant from MurmurHash2
                return IndexHash((uint32_t)(index ^ (tag * 0x5bd1e995)));
         */
        int index = bucket ^ (fingerprint * 0x5bd1e995);
        return hashToIndex(index, numBuckets);
    }

    /**
     * Given the bucket and entry position, returns the absolute offset
     * inside the PackedInts datastructure
     */
    private int getOffset(int bucket, int position) {
        return (bucket * entriesPerBucket) + position;
    }

    /**
     * Calculates the fingerprint for a given hash.
     *
     * The fingerprint is simply the first `bitsPerEntry` number of bits that are non-zero.
     * If the entire hash is zero, `(int) 1` is used
     *
     * Expert-level API
     */
    static int fingerprint(int hash, int bitsPerEntry, int fingerprintMask) {
        if (hash == 0) {
            // we use 0 as "empty" so if the hash actually hashes to zero... return 1
            // Some other impls will re-hash with a salt but this seems simpler
            return 1;
        }

        for (int i = 0; i + bitsPerEntry <= Long.SIZE; i += bitsPerEntry) {
            int v = (hash >> i) & fingerprintMask;
            if (v != 0) {
                return v;
            }
        }
        return 1;
    }

    /**
     * Calculate the optimal number of bits per entry
     */
    private static int bitsPerEntry(double fpp, int numEntriesPerBucket) {
        return (int) Math.round(log2((2 * numEntriesPerBucket) / fpp));
    }

    /**
     * Calculate the optimal number of entries per bucket.  Will return 2, 4 or 8
     * depending on the false positive rate
     */
    private static int entriesPerBucket(double fpp) {
        /*
          Empirical constants from paper:
            "the space-optimal bucket size depends on the target false positive rate ε:
             when ε > 0.002, having two entries per bucket yields slightly better results
             than using four entries per bucket; when ε decreases to 0.00001 < ε <= 0.002,
             four entries per bucket minimzes space"
         */

        if (fpp > 0.002) {
            return 2;
        } else if (fpp > 0.00001 && fpp <= 0.002) {
            return 4;
        }
        return 8;
    }

    /**
     * Calculates the optimal load factor for the filter, given the number of entries
     * per bucket.  Will return 0.84, 0.955 or 0.98 depending on b
     */
    private static double getLoadFactor(int b) {
        if ((b == 2 || b == 4 || b == 8) == false) {
            throw new IllegalArgumentException("b must be one of [2,4,8]");
        }
        /*
          Empirical constants from the paper:
            "With k = 2 hash functions, the load factor α is 50% when bucket size b = 1 (i.e
            the hash table is directly mapped), but increases to 84%, 95%, 98% respectively
            using bucket size b = 2, 4, 8"
         */
        if (b == 2) {
            return 0.84D;
        } else if (b == 4) {
            return 0.955D;
        } else {
            return 0.98D;
        }
    }

    /**
     * Calculates the optimal number of buckets for this filter.  The xor used in the bucketing
     * algorithm requires this to be a power of two, so the optimal number of buckets will
     * be rounded to the next largest power of two where applicable.
     *
     * TODO: there are schemes to avoid powers of two, might want to investigate those
     */
    private static int getNumBuckets(long capacity, double loadFactor, int b) {
        long buckets = Math.round((((double) capacity / loadFactor)) / (double) b);

        // Rounds up to nearest power of 2
        return 1 << -Integer.numberOfLeadingZeros((int) buckets - 1);
    }

    private static double log2(double x) {
        return Math.log(x) / LN_2;
    }

    public long getSizeInBytes() {
        // (numBuckets, bitsPerEntry, fingerprintMask, entriesPerBucket, count, evictedFingerprint) * 4b == 24b
        return data.ramBytesUsed() + 24;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numBuckets, bitsPerEntry, entriesPerBucket, count, evictedFingerprint);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final CuckooFilter that = (CuckooFilter) other;
        return Objects.equals(this.numBuckets, that.numBuckets)
            && Objects.equals(this.bitsPerEntry, that.bitsPerEntry)
            && Objects.equals(this.entriesPerBucket, that.entriesPerBucket)
            && Objects.equals(this.count, that.count)
            && Objects.equals(this.evictedFingerprint, that.evictedFingerprint);
    }

    /**
     * Forked from Lucene's Packed64 class. The main difference is that this version
     * can be read from / write to Elasticsearch streams.
     */
    private static class PackedArray {
        private static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
        private static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
        private static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

        /**
         * Values are stores contiguously in the blocks array.
         */
        private final long[] blocks;
        /**
         * A right-aligned mask of width BitsPerValue used by {@link #get(int)}.
         */
        private final long maskRight;
        /**
         * Optimization: Saves one lookup in {@link #get(int)}.
         */
        private final int bpvMinusBlockSize;

        private final int bitsPerValue;
        private final int valueCount;

        PackedArray(int valueCount, int bitsPerValue) {
            this.bitsPerValue = bitsPerValue;
            this.valueCount = valueCount;
            final int longCount = PackedInts.Format.PACKED.longCount(PackedInts.VERSION_CURRENT, valueCount, bitsPerValue);
            this.blocks = new long[longCount];
            maskRight = ~0L << (BLOCK_SIZE - bitsPerValue) >>> (BLOCK_SIZE - bitsPerValue);
            bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
        }

        PackedArray(StreamInput in) throws IOException {
            this.bitsPerValue = in.readVInt();
            this.valueCount = in.readVInt();
            this.blocks = in.readLongArray();
            maskRight = ~0L << (BLOCK_SIZE - bitsPerValue) >>> (BLOCK_SIZE - bitsPerValue);
            bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
        }

        public void save(StreamOutput out) throws IOException {
            out.writeVInt(bitsPerValue);
            out.writeVInt(valueCount);
            out.writeLongArray(blocks);
        }

        public int size() {
            return valueCount;
        }

        public long get(final int index) {
            // The abstract index in a bit stream
            final long majorBitPos = (long) index * bitsPerValue;
            // The index in the backing long-array
            final int elementPos = (int) (majorBitPos >>> BLOCK_BITS);
            // The number of value-bits in the second long
            final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

            if (endBits <= 0) { // Single block
                return (blocks[elementPos] >>> -endBits) & maskRight;
            }
            // Two blocks
            return ((blocks[elementPos] << endBits) | (blocks[elementPos + 1] >>> (BLOCK_SIZE - endBits))) & maskRight;
        }

        public int get(int index, long[] arr, int off, int len) {
            assert len > 0 : "len must be > 0 (got " + len + ")";
            assert index >= 0 && index < valueCount;
            len = Math.min(len, valueCount - index);
            assert off + len <= arr.length;

            final int originalIndex = index;
            final PackedInts.Decoder decoder = PackedInts.getDecoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsPerValue);
            // go to the next block where the value does not span across two blocks
            final int offsetInBlocks = index % decoder.longValueCount();
            if (offsetInBlocks != 0) {
                for (int i = offsetInBlocks; i < decoder.longValueCount() && len > 0; ++i) {
                    arr[off++] = get(index++);
                    --len;
                }
                if (len == 0) {
                    return index - originalIndex;
                }
            }

            // bulk get
            assert index % decoder.longValueCount() == 0;
            int blockIndex = (int) (((long) index * bitsPerValue) >>> BLOCK_BITS);
            assert (((long) index * bitsPerValue) & MOD_MASK) == 0;
            final int iterations = len / decoder.longValueCount();
            decoder.decode(blocks, blockIndex, arr, off, iterations);
            final int gotValues = iterations * decoder.longValueCount();
            index += gotValues;
            len -= gotValues;
            assert len >= 0;

            if (index > originalIndex) {
                // stay at the block boundary
                return index - originalIndex;
            } else {
                // no progress so far => already at a block boundary but no full block to get
                assert index == originalIndex;
                assert len > 0 : "len must be > 0 (got " + len + ")";
                assert index >= 0 && index < size();
                assert off + len <= arr.length;

                final int gets = Math.min(size() - index, len);
                for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
                    arr[o] = get(i);
                }
                return gets;
            }
        }

        public void set(final int index, final long value) {
            // The abstract index in a contiguous bit stream
            final long majorBitPos = (long) index * bitsPerValue;
            // The index in the backing long-array
            final int elementPos = (int) (majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
            // The number of value-bits in the second long
            final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

            if (endBits <= 0) { // Single block
                blocks[elementPos] = blocks[elementPos] & ~(maskRight << -endBits) | (value << -endBits);
                return;
            }
            // Two blocks
            blocks[elementPos] = blocks[elementPos] & ~(maskRight >>> endBits) | (value >>> endBits);
            blocks[elementPos + 1] = blocks[elementPos + 1] & (~0L >>> endBits) | (value << (BLOCK_SIZE - endBits));
        }

        public long ramBytesUsed() {
            return RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 3 * Integer.BYTES   // bpvMinusBlockSize,valueCount,bitsPerValue
                    + Long.BYTES          // maskRight
                    + RamUsageEstimator.NUM_BYTES_OBJECT_REF
            ) // blocks ref
                + RamUsageEstimator.sizeOf(blocks);
        }
    }
}
