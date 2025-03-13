/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * TopNEncoder for data types that are unsupported. This is just a placeholder class, reaching the encode/decode methods here is a bug.
 *
 * While this class is needed to build the TopNOperator value and key extractors infrastructure, encoding/decoding is needed
 * when actually sorting on a field (which shouldn't be possible for unsupported data types) using key extractors, or when encoding/decoding
 * unsupported data types fields values (which should always be "null" by convention) using value extractors.
 */
class UnsupportedTypesTopNEncoder extends SortableTopNEncoder {
    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        throw new UnsupportedOperationException("Encountered a bug; trying to encode an unsupported data type value for TopN");
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        throw new UnsupportedOperationException("Encountered a bug; trying to decode an unsupported data type value for TopN");
    }

    @Override
    public String toString() {
        return "UnsupportedTypesTopNEncoder";
    }

    @Override
    public TopNEncoder toSortable() {
        return this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
