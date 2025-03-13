/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.Locale;

/**
 * Extracts sort keys for top-n from their {@link BytesRefBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class KeyExtractorForBytesRef implements KeyExtractor {
    static KeyExtractorForBytesRef extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, BytesRefBlock block) {
        BytesRefVector v = block.asVector();
        if (v != null) {
            return new KeyExtractorForBytesRef.FromVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorForBytesRef.MinFromAscendingBlock(encoder, nul, nonNul, block)
                : new KeyExtractorForBytesRef.MinFromUnorderedBlock(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorForBytesRef.MaxFromAscendingBlock(encoder, nul, nonNul, block)
            : new KeyExtractorForBytesRef.MaxFromUnorderedBlock(encoder, nul, nonNul, block);
    }

    private final TopNEncoder encoder;
    protected final BytesRef scratch = new BytesRef();
    private final byte nul;
    private final byte nonNul;

    KeyExtractorForBytesRef(TopNEncoder encoder, byte nul, byte nonNul) {
        this.encoder = encoder;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final int nonNul(BreakingBytesRefBuilder key, BytesRef value) {
        key.append(nonNul);
        return encoder.encodeBytesRef(value, key) + 1;
    }

    protected final int nul(BreakingBytesRefBuilder key) {
        key.append(nul);
        return 1;
    }

    @Override
    public final String toString() {
        return String.format(Locale.ROOT, "KeyExtractorForBytesRef%s(%s, %s, %s)", getClass().getSimpleName(), encoder, nul, nonNul);
    }

    static class FromVector extends KeyExtractorForBytesRef {
        private final BytesRefVector vector;

        FromVector(TopNEncoder encoder, byte nul, byte nonNul, BytesRefVector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            return nonNul(key, vector.getBytesRef(position, scratch));
        }
    }

    static class MinFromAscendingBlock extends KeyExtractorForBytesRef {
        private final BytesRefBlock block;

        MinFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, BytesRefBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getBytesRef(block.getFirstValueIndex(position), scratch));
        }
    }

    static class MaxFromAscendingBlock extends KeyExtractorForBytesRef {
        private final BytesRefBlock block;

        MaxFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, BytesRefBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                return nul(key);
            }
            return nonNul(key, block.getBytesRef(block.getFirstValueIndex(position) + block.getValueCount(position) - 1, scratch));
        }
    }

    static class MinFromUnorderedBlock extends KeyExtractorForBytesRef {
        private final BytesRefBlock block;

        private final BytesRef minScratch = new BytesRef();

        MinFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, BytesRefBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                return nul(key);
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            BytesRef min = block.getBytesRef(start, minScratch);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                if (v.compareTo(min) < 0) {
                    min.bytes = v.bytes;
                    min.offset = v.offset;
                    min.length = v.length;
                }
            }
            return nonNul(key, min);
        }
    }

    static class MaxFromUnorderedBlock extends KeyExtractorForBytesRef {
        private final BytesRefBlock block;

        private final BytesRef maxScratch = new BytesRef();

        MaxFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, BytesRefBlock block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public int writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                return nul(key);
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            BytesRef max = block.getBytesRef(start, maxScratch);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                if (v.compareTo(max) > 0) {
                    max.bytes = v.bytes;
                    max.offset = v.offset;
                    max.length = v.length;
                }
            }
            return nonNul(key, max);
        }
    }
}
