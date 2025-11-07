/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.ExponentialHistogramBlock;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

public class ValueExtractorForExponentialHistogram implements ValueExtractor {
    private final ExponentialHistogramBlock block;

    private final BytesRef scratch = new BytesRef();
    private final ReusableTopNEncoderOutput reusableOutput = new ReusableTopNEncoderOutput();

    ValueExtractorForExponentialHistogram(TopNEncoder encoder, ExponentialHistogramBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder values, int position) {
        // number of multi-values first for compatibility with ValueExtractorForNull
        if (block.isNull(position)) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(0, values);
        } else {
            assert block.getValueCount(position) == 1 : "Multi-valued ExponentialHistogram blocks are not supported in TopN";
            TopNEncoder.DEFAULT_UNSORTABLE.encodeVInt(1, values);
            int valueIndex = block.getFirstValueIndex(position);
            reusableOutput.target = values;
            block.serializeExponentialHistogram(valueIndex, reusableOutput, scratch);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForExponentialHistogram";
    }

    private static final class ReusableTopNEncoderOutput implements ExponentialHistogramBlock.SerializedOutput {
        BreakingBytesRefBuilder target;

        @Override
        public void appendDouble(double value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeDouble(value, target);
        }

        @Override
        public void appendLong(long value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeLong(value, target);
        }

        @Override
        public void appendBytesRef(BytesRef value) {
            TopNEncoder.DEFAULT_UNSORTABLE.encodeBytesRef(value, target);
        }
    }
}
