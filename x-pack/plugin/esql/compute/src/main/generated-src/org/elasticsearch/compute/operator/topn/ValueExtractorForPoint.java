/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.PointBlock;
import org.elasticsearch.compute.data.PointVector;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class ValueExtractorForPoint implements ValueExtractor {
    static ValueExtractorForPoint extractorFor(TopNEncoder encoder, boolean inKey, PointBlock block) {
        PointVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForPoint.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForPoint.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;

    ValueExtractorForPoint(TopNEncoder encoder, boolean inKey) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, double x, double y) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodePoint(x, y, values);
    }

    static class ForVector extends ValueExtractorForPoint {
        private final PointVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, PointVector vector) {
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
            actualWriteValue(values, vector.getX(position), vector.getY(position));
        }
    }

    static class ForBlock extends ValueExtractorForPoint {
        private final PointBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, PointBlock block) {
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
                actualWriteValue(values, block.getX(i), block.getY(i));
            }
        }
    }
}
