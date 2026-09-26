/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.PackDimBlock;
import org.elasticsearch.compute.data.PackDimValue;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/** Carries sparse records as sort payloads without treating them as sortable keys or consulting their value codec. */
final class ValueExtractorForPackDim implements ValueExtractor {
    private final PackDimBlock block;
    private final PackDimValue record = new PackDimValue();

    ValueExtractorForPackDim(PackDimBlock block) {
        this.block = block;
    }

    @Override
    public void writeValue(BreakingBytesRefBuilder output, int position) {
        var encoder = TopNEncoder.DEFAULT_UNSORTABLE;
        encoder.encodeVInt(block.isNull(position) ? 0 : 1, output);
        if (block.isNull(position)) return;
        block.getPackDim(block.getFirstValueIndex(position), record);
        int count = record.size();
        encoder.encodeInt(count, output);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < count; i++) {
            encoder.encodeBytesRef(record.nameAt(i, scratch), output);
            encoder.encodeBytesRef(record.valueAt(i, scratch), output);
        }
    }
}
