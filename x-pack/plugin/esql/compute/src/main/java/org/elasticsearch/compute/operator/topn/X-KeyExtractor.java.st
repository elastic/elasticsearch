/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

$if(BytesRef)$
import org.apache.lucene.util.BytesRef;
$endif$
import org.elasticsearch.compute.data.$Type$Block;
import org.elasticsearch.compute.data.$Type$Vector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.util.Locale;

/**
 * Extracts sort keys for top-n from their {@link $Type$Block}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class KeyExtractorFor$Type$ implements KeyExtractor {
    static KeyExtractorFor$Type$ extractorFor(TopNEncoder encoder, boolean ascending, byte nul, byte nonNul, $Type$Block block) {
        $Type$Vector v = block.asVector();
        if (v != null) {
            return new KeyExtractorFor$Type$.FromVector(encoder, nul, nonNul, v);
        }
        if (ascending) {
            return block.mvSortedAscending()
                ? new KeyExtractorFor$Type$.MinFromAscendingBlock(encoder, nul, nonNul, block)
                : new KeyExtractorFor$Type$.MinFromUnorderedBlock(encoder, nul, nonNul, block);
        }
        return block.mvSortedAscending()
            ? new KeyExtractorFor$Type$.MaxFromAscendingBlock(encoder, nul, nonNul, block)
            : new KeyExtractorFor$Type$.MaxFromUnorderedBlock(encoder, nul, nonNul, block);
    }

    private final TopNEncoder encoder;
$if(BytesRef)$
    protected final BytesRef scratch = new BytesRef();
$endif$
    private final byte nul;
    private final byte nonNul;

    KeyExtractorFor$Type$(TopNEncoder encoder, byte nul, byte nonNul) {
        this.encoder = encoder;
        this.nul = nul;
        this.nonNul = nonNul;
    }

    protected final void nonNul(BreakingBytesRefBuilder key, $type$ value) {
        key.append(nonNul);
$if(BytesRef)$
        encoder.encodeBytesRef(value, key);
$elseif(boolean)$
        encoder.encodeBoolean(value, key);
$else$
        encoder.encode$Type$(value, key);
$endif$
    }

    protected final void nul(BreakingBytesRefBuilder key) {
        key.append(nul);
    }

    @Override
    public final String toString() {
$if(BytesRef)$
        return String.format(Locale.ROOT, "KeyExtractorFor$Type$%s(%s, %s, %s)", getClass().getSimpleName(), encoder, nul, nonNul);
$else$
        return String.format(Locale.ROOT, "KeyExtractorFor$Type$%s(%s, %s)", getClass().getSimpleName(), nul, nonNul);
$endif$
    }

    static class FromVector extends KeyExtractorFor$Type$ {
        private final $Type$Vector vector;

        FromVector(TopNEncoder encoder, byte nul, byte nonNul, $Type$Vector vector) {
            super(encoder, nul, nonNul);
            this.vector = vector;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
$if(BytesRef)$
            nonNul(key, vector.get$Type$(position, scratch));
$else$
            nonNul(key, vector.get$Type$(position));
$endif$
        }
    }

    static class MinFromAscendingBlock extends KeyExtractorFor$Type$ {
        private final $Type$Block block;

        MinFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, $Type$Block block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                nul(key);
                return;
            }
$if(BytesRef)$
            nonNul(key, block.get$Type$(block.getFirstValueIndex(position), scratch));
$else$
            nonNul(key, block.get$Type$(block.getFirstValueIndex(position)));
$endif$
        }
    }

    static class MaxFromAscendingBlock extends KeyExtractorFor$Type$ {
        private final $Type$Block block;

        MaxFromAscendingBlock(TopNEncoder encoder, byte nul, byte nonNul, $Type$Block block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            if (block.isNull(position)) {
                nul(key);
                return;
            }
$if(BytesRef)$
            nonNul(key, block.get$Type$(block.getFirstValueIndex(position) + block.getValueCount(position) - 1, scratch));
$else$
            nonNul(key, block.get$Type$(block.getFirstValueIndex(position) + block.getValueCount(position) - 1));
$endif$
        }
    }

    static class MinFromUnorderedBlock extends KeyExtractorFor$Type$ {
        private final $Type$Block block;

$if(BytesRef)$
        private final BytesRef minScratch = new BytesRef();
$endif$

        MinFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, $Type$Block block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                nul(key);
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
$if(BytesRef)$
            BytesRef min = block.getBytesRef(start, minScratch);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                if (v.compareTo(min) < 0) {
                    min.bytes = v.bytes;
                    min.offset = v.offset;
                    min.length = v.length;
                }
            }
            nonNul(key, min);
$elseif(boolean)$
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i) == false) {
                    nonNul(key, false);
                    return;
                }
            }
            nonNul(key, true);
$else$
            $type$ min = block.get$Type$(start);
            for (int i = start + 1; i < end; i++) {
                min = Math.min(min, block.get$Type$(i));
            }
            nonNul(key, min);
$endif$
        }
    }

    static class MaxFromUnorderedBlock extends KeyExtractorFor$Type$ {
        private final $Type$Block block;

$if(BytesRef)$
        private final BytesRef maxScratch = new BytesRef();
$endif$

        MaxFromUnorderedBlock(TopNEncoder encoder, byte nul, byte nonNul, $Type$Block block) {
            super(encoder, nul, nonNul);
            this.block = block;
        }

        @Override
        public void writeKey(BreakingBytesRefBuilder key, int position) {
            int size = block.getValueCount(position);
            if (size == 0) {
                nul(key);
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
$if(BytesRef)$
            BytesRef max = block.getBytesRef(start, maxScratch);
            for (int i = start; i < end; i++) {
                BytesRef v = block.getBytesRef(i, scratch);
                if (v.compareTo(max) > 0) {
                    max.bytes = v.bytes;
                    max.offset = v.offset;
                    max.length = v.length;
                }
            }
            nonNul(key, max);
$elseif(boolean)$
            for (int i = start; i < end; i++) {
                if (block.getBoolean(i)) {
                    nonNul(key, true);
                    return;
                }
            }
            nonNul(key, false);
$else$
            $type$ max = block.get$Type$(start);
            for (int i = start + 1; i < end; i++) {
                max = Math.max(max, block.get$Type$(i));
            }
            nonNul(key, max);
$endif$
        }
    }
}
