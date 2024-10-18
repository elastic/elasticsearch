/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.FloatVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Extracts non-sort-key values for top-n from their {@link FloatBlock}s.
 * This class is generated. Edit {@code X-KeyExtractor.java.st} instead.
 */
abstract class ValueExtractorForFloat implements ValueExtractor {
    static ValueExtractorForFloat extractorFor(TopNEncoder encoder, boolean inKey, FloatBlock block) {
        FloatVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForFloat.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForFloat.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;

    ValueExtractorForFloat(TopNEncoder encoder, boolean inKey) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, float value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeFloat(value, values);
    }

    static class ForVector extends ValueExtractorForFloat {
        private final FloatVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, FloatVector vector) {
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
            actualWriteValue(values, vector.getFloat(position));
        }
    }

    static class ForBlock extends ValueExtractorForFloat {
        private final FloatBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, FloatBlock block) {
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
                actualWriteValue(values, block.getFloat(i));
            }
        }
    }
}
