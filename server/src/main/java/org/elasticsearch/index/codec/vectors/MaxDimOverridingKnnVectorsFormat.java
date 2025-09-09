/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public final class MaxDimOverridingKnnVectorsFormat extends KnnVectorsFormat {
    private final KnnVectorsFormat delegate;
    private final int maxDimension;

    public MaxDimOverridingKnnVectorsFormat(KnnVectorsFormat delegate, int maxDimension) {
        super(delegate.getName());
        this.delegate = delegate;
        this.maxDimension = maxDimension;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return delegate.fieldsWriter(state);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return delegate.fieldsReader(state);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return maxDimension;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public KnnVectorsFormat getDelegate() {
        return delegate;
    }
}
