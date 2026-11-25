/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;

/**
 * Compacts the dictionary of an {@link OrdinalBytesRefBlock} when at most half of a sufficiently
 * large dictionary is referenced. Consumers of ordinal blocks (dictionary-aware evaluators,
 * {@code VALUES}, {@code BlockHash}) process the dictionary once and map results back, so
 * unreferenced entries add downstream work. Copying a mostly referenced dictionary per batch is
 * more expensive than that saving, however, so this uses the same skip and share thresholds as
 * {@link PageColumnReader}, without its stateful back-off.
 *
 * <p>{@link PageColumnReader} compacts on the read path, from the raw ordinals it decodes. This
 * class covers the blocks that are read uncompacted on purpose: late-materialization predicate
 * blocks, whose ordinals must stay in the row-group dictionary's numbering while the predicate is
 * evaluated (the per-row-group dictionary-match memo in {@code OptimizedParquetColumnIterator} is
 * indexed by those ordinals), and have the compaction threshold applied here once they are emitted.
 *
 * <p>Both paths use the same scheme: one bit per dictionary entry marks the referenced ones, and the
 * new ordinal of an entry is its rank among them. That keeps the relative dictionary order, so the
 * group order produced by hashing does not change.
 */
final class OrdinalCompaction {

    /** Circuit breaker label for the mark / rank scratch. */
    static final String SCRATCH_LABEL = "parquet dictionary remap";

    private OrdinalCompaction() {}

    /** Number of {@code long} words holding one bit per entry of a {@code dictionarySize}-entry dictionary. */
    static int words(int dictionarySize) {
        return Math.toIntExact(((long) dictionarySize + Long.SIZE - 1) >>> 6);
    }

    /** Heap bytes of the {@code long[words]} mark bitset plus the {@code int[words]} rank prefix sums. */
    static long scratchBytes(int words) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) words * Long.BYTES) + RamUsageEstimator
            .alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) words * Integer.BYTES);
    }

    /**
     * Fills {@code rankBase[w]} with the number of marked entries in the words before {@code w}. Only the
     * first {@code rankBase.length} words of {@code marked} are read, so it may carry more after them.
     *
     * @return the total number of marked entries
     */
    static int fillRankBase(long[] marked, int[] rankBase) {
        int running = 0;
        for (int w = 0; w < rankBase.length; w++) {
            rankBase[w] = running;
            running += Long.bitCount(marked[w]);
        }
        return running;
    }

    /** The rank of the marked entry {@code ord} among all marked entries, in ascending dictionary order. */
    static int rank(long[] marked, int[] rankBase, int ord) {
        int w = ord >>> 6;
        // The shift distance of a long shift is taken mod 64, so this masks the bits below ord in its word.
        return rankBase[w] + Long.bitCount(marked[w] & ((1L << ord) - 1));
    }

    /**
     * Returns {@code source}, compacting its dictionary when the shared dictionary is large relative
     * to the non-null values and at most half of its entries are referenced.
     * <p>
     * {@code source} is returned untouched when it is not an ordinal block, when
     * {@code dictionarySize * PageColumnReader.SKIP_DICTIONARY_RATIO <= nonNullValues}, or when more
     * than half of the dictionary is referenced. Otherwise a new block is built and {@code source}
     * is closed, but only on success: if this method throws, the caller still owns {@code source}
     * (the same contract as {@link PageColumnReader#filterBlock}).
     * <p>
     * The mark / rank scratch is charged to the request breaker, as {@link #SCRATCH_LABEL}, for the
     * duration of the call.
     */
    static Block compact(Block source, BlockFactory blockFactory) {
        OrdinalBytesRefBlock ordinalBlock = source instanceof BytesRefBlock bytes ? bytes.asOrdinals() : null;
        if (ordinalBlock == null) {
            return source;
        }
        IntBlock ordinals = ordinalBlock.getOrdinalsBlock();
        // Parquet ordinal blocks come from flat (maxRepLevel == 0) columns only.
        assert ordinals.mayHaveMultivaluedFields() == false : "Parquet ordinal blocks are single-valued";
        if (ordinals.mayHaveMultivaluedFields()) {
            return source;
        }
        BytesRefVector dictionary = ordinalBlock.getDictionaryVector();
        int dictionarySize = dictionary.getPositionCount();
        int positions = ordinals.getPositionCount();
        int values = ordinals.getTotalValueCount();
        if ((long) dictionarySize * PageColumnReader.SKIP_DICTIONARY_RATIO <= values) {
            return source;
        }
        int words = words(dictionarySize);
        long scratchBytes = scratchBytes(words);
        CircuitBreaker breaker = blockFactory.breaker();
        breaker.addEstimateBytesAndMaybeBreak(scratchBytes, SCRATCH_LABEL);
        try {
            long[] marked = new long[words];
            for (int p = 0; p < positions; p++) {
                if (ordinals.isNull(p) == false) {
                    int ord = ordinals.getInt(ordinals.getFirstValueIndex(p));
                    marked[ord >>> 6] |= 1L << ord;
                }
            }
            int[] rankBase = new int[words];
            int referenced = fillRankBase(marked, rankBase);
            if ((long) referenced * 2 > dictionarySize) {
                return source;
            }
            Block result = referenced == 0
                ? blockFactory.newConstantNullBlock(positions)
                : buildCompacted(ordinals, dictionary, marked, rankBase, referenced, blockFactory);
            source.close();
            return result;
        } finally {
            breaker.addWithoutBreaking(-scratchBytes);
        }
    }

    private static Block buildCompacted(
        IntBlock ordinals,
        BytesRefVector dictionary,
        long[] marked,
        int[] rankBase,
        int referenced,
        BlockFactory blockFactory
    ) {
        int positions = ordinals.getPositionCount();
        IntBlock newOrdinals = null;
        BytesRefVector newDictionary = null;
        try {
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions)) {
                for (int p = 0; p < positions; p++) {
                    if (ordinals.isNull(p)) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(rank(marked, rankBase, ordinals.getInt(ordinals.getFirstValueIndex(p))));
                    }
                }
                newOrdinals = builder.build();
            }
            try (BytesRefVector.Builder builder = blockFactory.newBytesRefVectorBuilder(referenced)) {
                // Entries are copied as they are: they were sanitized when the source dictionary was built.
                BytesRef scratch = new BytesRef();
                for (int w = 0; w < marked.length; w++) {
                    long bits = marked[w];
                    while (bits != 0) {
                        int ord = (w << 6) + Long.numberOfTrailingZeros(bits);
                        builder.appendBytesRef(dictionary.getBytesRef(ord, scratch));
                        bits &= bits - 1;
                    }
                }
                newDictionary = builder.build();
            }
            return new OrdinalBytesRefBlock(newOrdinals, newDictionary);
        } catch (Throwable e) {
            ParquetReadFailures.closePreservingCause(e, newOrdinals, newDictionary);
            throw e;
        }
    }
}
