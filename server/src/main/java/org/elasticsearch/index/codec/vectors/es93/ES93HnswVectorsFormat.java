/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractHnswVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ES93HnswVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES93HnswVectorsFormat";

    private final FlatVectorsFormat flatVectorsFormat;

    public ES93HnswVectorsFormat() {
        super(NAME);
        flatVectorsFormat = new ES93GenericFlatVectorsFormat();
    }

    public ES93HnswVectorsFormat(DenseVectorFieldMapper.ElementType elementType) {
        super(NAME);
        flatVectorsFormat = new ES93GenericFlatVectorsFormat(elementType, false);
    }

    public ES93HnswVectorsFormat(int maxConn, int beamWidth, DenseVectorFieldMapper.ElementType elementType) {
        super(NAME, maxConn, beamWidth);
        flatVectorsFormat = new ES93GenericFlatVectorsFormat(elementType, false);
    }

    public ES93HnswVectorsFormat(
        int maxConn,
        int beamWidth,
        DenseVectorFieldMapper.ElementType elementType,
        int numMergeWorkers,
        ExecutorService mergeExec
    ) {
        super(NAME, maxConn, beamWidth, numMergeWorkers, mergeExec);
        flatVectorsFormat = new ES93GenericFlatVectorsFormat(elementType, false);
    }

    @Override
    protected FlatVectorsFormat flatVectorsFormat() {
        return flatVectorsFormat;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state), numMergeWorkers, mergeExec);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }
}
