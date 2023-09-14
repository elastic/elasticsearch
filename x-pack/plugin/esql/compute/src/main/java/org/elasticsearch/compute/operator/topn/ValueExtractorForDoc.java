/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.compute.data.DocVector;

class ValueExtractorForDoc implements ValueExtractor {
    private final DocVector vector;

    ValueExtractorForDoc(TopNEncoder encoder, DocVector vector) {
        assert encoder == TopNEncoder.DEFAULT_UNSORTABLE;
        this.vector = vector;
    }

    @Override
    public void writeValue(BytesRefBuilder values, int position) {
        TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(vector.shards().getInt(position), values);
        TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(vector.segments().getInt(position), values);
        TopNEncoder.DEFAULT_UNSORTABLE.encodeInt(vector.docs().getInt(position), values);
    }

    @Override
    public String toString() {
        return "ValueExtractorForDoc";
    }
}
