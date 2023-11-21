/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

class DefaultSortableTopNEncoder extends SortableTopNEncoder {
    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        throw new IllegalStateException("Cannot find encoder for BytesRef value");
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        throw new IllegalStateException("Cannot find encoder for BytesRef value");
    }

    @Override
    public String toString() {
        return "DefaultSortable";
    }

    @Override
    public TopNEncoder toSortable() {
        return this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return TopNEncoder.DEFAULT_UNSORTABLE;
    }
}
