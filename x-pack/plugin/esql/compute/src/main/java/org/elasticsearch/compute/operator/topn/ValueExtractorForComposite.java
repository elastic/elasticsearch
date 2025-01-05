/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

abstract class ValueExtractorForComposite implements ValueExtractor {
    static ValueExtractorForComposite extractorFor(TopNEncoder encoder, boolean inKey, CompositeBlock block) {
        // todo: implement ForVector?
        return new ValueExtractorForComposite.ForBlock(encoder, inKey, block);
    }

    protected final boolean inKey;

    ValueExtractorForComposite(TopNEncoder encoder, boolean inKey) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE : encoder.toString();
        this.inKey = inKey;
    }

    protected final void writeCount(BreakingBytesRefBuilder values, int count) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(count, values);
    }

    protected final void actualWriteValue(BreakingBytesRefBuilder values, double value) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(value, values);
    }

    static class ForBlock extends ValueExtractorForComposite {
        private final CompositeBlock block;

        ForBlock(TopNEncoder encoder, boolean inKey, CompositeBlock block) {
            super(encoder, inKey);
            this.block = block;
        }

        @Override
        public void writeValue(BreakingBytesRefBuilder values, int position) {
            int size = block.getBlock(1).getValueCount(position);
            writeCount(values, size);
            if (size == 1 && inKey) {
                // Will read results from the key
                return;
            }
            // TODO: include all aggregate double metric sub fields. Currently hardcoded to max sub field, which is always the second block:
            int start = block.getBlock(1).getFirstValueIndex(position);
            int end = start + size;
            for (int i = start; i < end; i++) {
                actualWriteValue(values, ((DoubleBlock) block.getBlock(1)).getDouble(i));
            }
        }
    }
}
