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
 * A {@link TopNEncoder} that doesn't encode values so they are sortable but is
 * capable of encoding any values.
 */
final class DefaultUnsortableTopNEncoder extends UnsortableTopNEncoder {

    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TopNEncoder toSortable() {
        return TopNEncoder.DEFAULT_SORTABLE;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }

    @Override
    public String toString() {
        return "DefaultUnsortable";
    }
}
