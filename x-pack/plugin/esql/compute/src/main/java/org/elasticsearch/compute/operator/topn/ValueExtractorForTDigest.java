/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.compute.data.TDigestBlock;

public class ValueExtractorForTDigest implements ValueExtractor {
    private final TDigestBlock block;

    private final BytesRef scratch = new BytesRef();
    private final PagedReusableTopNEncoderOutput pagedReusableOutput = new PagedReusableTopNEncoderOutput();

    ValueExtractorForTDigest(TopNEncoder encoder, TDigestBlock block) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.block = block;
    }

    @Override
    public void writeValue(PagedBytesBuilder values, int position) {
        // number of multi-values first for compatibility with ValueExtractorForNull
        if (block.isNull(position)) {
            values.appendVInt(0);
        } else {
            assert block.getValueCount(position) == 1 : "Multi-valued ExponentialHistogram blocks are not supported in TopN";
            values.appendVInt(1);
            int valueIndex = block.getFirstValueIndex(position);
            pagedReusableOutput.target = values;
            block.serializeTDigest(valueIndex, pagedReusableOutput, scratch);
        }
    }

    @Override
    public String toString() {
        return "ValueExtractorForExponentialHistogram";
    }

    private static final class PagedReusableTopNEncoderOutput implements TDigestBlock.SerializedTDigestOutput {
        PagedBytesBuilder target;

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
