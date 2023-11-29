/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class ValueExtractorForLong implements ValueExtractor {
    static ValueExtractorForLong extractorFor(TopNEncoder encoder, boolean inKey, LongBlock block) {
        LongVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForLong.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForLong.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;

    ValueExtractorForLong(TopNEncoder encoder, boolean inKey) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, long value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(value, values);
    }

    static class ForVector extends ValueExtractorForLong {
        private final LongVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, LongVector vector) {
            super(encoder, inKey);
            this.vector = vector;
        }

        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            writeCount(values, 1);
            if (inKey) {
                // will read results from the key
                return;
            }
            actualWriteValue(values, vector.getLong(position));
        }
    }

    static class ForBlock extends ValueExtractorForLong {
        private final LongBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, LongBlock block) {
            super(encoder, inKey);
            this.block = block;
        }

        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            int size = block.getValueCount(position);
            writeCount(values, size);
            if (size == 1 && inKey) {
                // Will read results from the key
                return;
            }
            int start = block.getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                actualWriteValue(values, block.getLong(i));
            }
        }
    }
}
