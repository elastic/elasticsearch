/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;

/**
 * Assigns each row position in a {@link Page} to a bucket in {@code [0, B)}, based on
 * the combined hash of the group key columns described by a list of {@link BlockHash.GroupSpec}.
 *
 * <p>This class is immutable and shared across threads. Per-call state (the scratch {@link BytesRef}
 * and the {@link MurmurHash3.Hash128} reuse buffer) must be allocated per worker; pass them in via
 * {@link #computeBuckets(Page, int, int[], BytesRef, MurmurHash3.Hash128)}.
 *
 * <p>Multivalue key columns are signalled by returning {@link #MULTIVALUE_DETECTED}. Null keys
 * are treated as a valid group and hash to a per-column constant that is distinct for each column
 * index, ensuring that {@code (null, X)} and {@code (X, null)} are not confused with each other.
 *
 * <p>Bucket assignment uses Lemire fastrange: {@code (int)(((h >>> 32) * (long) B) >>> 32)}.
 * This avoids division and imposes no power-of-two constraint on B.
 */
public final class GroupKeyBucketer {

    /** Returned by {@link #computeBuckets} when any key channel contains multivalued data. */
    public static final int MULTIVALUE_DETECTED = -1;

    private final List<BlockHash.GroupSpec> specs;
    /** Per-column null hashes, precomputed once at construction. */
    private final long[] nullHashes;

    public GroupKeyBucketer(List<BlockHash.GroupSpec> specs) {
        this.specs = List.copyOf(specs);
        this.nullHashes = new long[specs.size()];
        for (int col = 0; col < specs.size(); col++) {
            // Unique per-column constant — distinct across column indices and from any non-null hash.
            nullHashes[col] = MurmurHash3.murmur64((long) col ^ 0xdeadbeefcafebabeL);
        }
    }

    /**
     * Fills {@code result[0..positionCount-1]} with bucket indices in {@code [0, buckets)}.
     *
     * @param page      source page; key blocks are at channels given by each spec
     * @param buckets   number of buckets B; must be &gt;= 1
     * @param result    output array; must have length &gt;= {@code page.getPositionCount()}
     * @param scratch   reusable BytesRef for BytesRef columns (per-worker, not shared)
     * @param hash128   reusable Hash128 for BytesRef hashing (per-worker, not shared)
     * @return {@link #MULTIVALUE_DETECTED} if any position has multiple values in any key channel,
     *         otherwise 0 (result is filled)
     */
    public int computeBuckets(Page page, int buckets, int[] result, BytesRef scratch, MurmurHash3.Hash128 hash128) {
        int positionCount = page.getPositionCount();

        // Fast pre-check: if any key block reports it may have multivalued fields, scan per-position.
        // This is a definite 'yes' question (mayHaveMultivaluedFields returns true conservatively).
        boolean needsPerPositionMvCheck = false;
        for (BlockHash.GroupSpec spec : specs) {
            if (page.getBlock(spec.channel()).mayHaveMultivaluedFields()) {
                needsPerPositionMvCheck = true;
                break;
            }
        }

        for (int pos = 0; pos < positionCount; pos++) {
            if (needsPerPositionMvCheck) {
                for (BlockHash.GroupSpec spec : specs) {
                    Block blk = page.getBlock(spec.channel());
                    if (blk.getValueCount(pos) > 1) {
                        return MULTIVALUE_DETECTED;
                    }
                }
            }
            long h = hashPosition(page, pos, scratch, hash128);
            result[pos] = lemireFastrange(h, buckets);
        }
        return 0;
    }

    /** Compute the combined hash for all group key columns at {@code pos}. */
    private long hashPosition(Page page, int pos, BytesRef scratch, MurmurHash3.Hash128 hash128) {
        long h = 0;
        for (int col = 0; col < specs.size(); col++) {
            BlockHash.GroupSpec spec = specs.get(col);
            Block blk = page.getBlock(spec.channel());

            long colHash;
            if (blk.isNull(pos)) {
                colHash = nullHashes[col];
            } else {
                int valueIndex = blk.getFirstValueIndex(pos);
                colHash = switch (spec.elementType()) {
                    case LONG -> MurmurHash3.murmur64(((LongBlock) blk).getLong(valueIndex));
                    case INT -> MurmurHash3.murmur64(((IntBlock) blk).getInt(valueIndex));
                    case BOOLEAN -> MurmurHash3.murmur64(((BooleanBlock) blk).getBoolean(valueIndex) ? 1L : 0L);
                    case DOUBLE -> MurmurHash3.murmur64(Double.doubleToLongBits(((DoubleBlock) blk).getDouble(valueIndex)));
                    case FLOAT -> MurmurHash3.murmur64(Float.floatToIntBits(((FloatBlock) blk).getFloat(valueIndex)));
                    case BYTES_REF -> {
                        BytesRef ref = ((BytesRefBlock) blk).getBytesRef(valueIndex, scratch);
                        MurmurHash3.hash128(ref.bytes, ref.offset, ref.length, 0, hash128);
                        yield hash128.h1 ^ hash128.h2;
                    }
                    case NULL -> nullHashes[col]; // ConstantNullBlock — treat like a null position
                    default -> throw new IllegalArgumentException("unsupported element type for group bucketing: " + spec.elementType());
                };
            }

            h ^= colHash;
            h = MurmurHash3.murmur64(h);
        }
        return h;
    }

    /**
     * Lemire fastrange: maps the upper 32 bits of {@code h} uniformly into {@code [0, B)}.
     * No division, no power-of-two constraint.
     */
    private static int lemireFastrange(long h, int B) {
        return (int) (((h >>> 32) * (long) B) >>> 32);
    }

    public List<BlockHash.GroupSpec> specs() {
        return specs;
    }
}
