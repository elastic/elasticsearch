/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class ValueExtractorForBoolean implements ValueExtractor {
    static ValueExtractorForBoolean extractorFor(TopNEncoder encoder, boolean inKey, BooleanBlock block) {
        BooleanVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForBoolean.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForBoolean.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;

    ValueExtractorForBoolean(TopNEncoder encoder, boolean inKey) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, boolean value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeBoolean(value, values);
    }

    static class ForVector extends ValueExtractorForBoolean {
        private final BooleanVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, BooleanVector vector) {
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
            actualWriteValue(values, vector.getBoolean(position));
        }
    }

    static class ForBlock extends ValueExtractorForBoolean {
        private final BooleanBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, BooleanBlock block) {
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
                actualWriteValue(values, block.getBoolean(i));
            }
        }
    }
}
