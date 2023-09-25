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

abstract class ValueExtractorForBytesRef implements ValueExtractor {
    static ValueExtractorForBytesRef extractorFor(TopNEncoder encoder, boolean inKey, BytesRefBlock block) {
        BytesRefVector vector = block.asVector();
        if (vector != null) {
            return new ValueExtractorForBytesRef.ForVector(encoder, inKey, vector);
        }
        return new ValueExtractorForBytesRef.ForBlock(encoder, inKey, block);
    }

    private final TopNEncoder encoder;

    protected final BytesRef scratch = new BytesRef();

    protected final boolean inKey;

    ValueExtractorForBytesRef(TopNEncoder encoder, boolean inKey) {
        this.encoder = encoder;
        this.inKey = inKey;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, BytesRef value) {
        encoder.encodeBytesRef(value, values);
    }

    static class ForVector extends ValueExtractorForBytesRef {
        private final BytesRefVector vector;

        ForVector(TopNEncoder encoder, boolean inKey, BytesRefVector vector) {
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
            actualWriteValue(values, vector.getBytesRef(position, scratch));
        }
    }

    static class ForBlock extends ValueExtractorForBytesRef {
        private final BytesRefBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, BytesRefBlock block) {
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
                actualWriteValue(values, block.getBytesRef(i, scratch));
            }
        }
    }
}
