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
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractHnswVectorsFormat;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ES93HnswScalarQuantizedVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES93HnswScalarQuantizedVectorsFormat";

    static final Lucene104ScalarQuantizedVectorScorer flatVectorScorer = new Lucene104ScalarQuantizedVectorScorer(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    private final Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding;
    private final FlatVectorsFormat rawVectorFormat;

    public ES93HnswScalarQuantizedVectorsFormat() {
        super(NAME);
        this.encoding = Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT;
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(ES93GenericFlatVectorsFormat.ElementType.STANDARD, false);
    }

    public ES93HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
        ES93GenericFlatVectorsFormat.ElementType elementType,
        boolean useDirectIO
    ) {
        super(NAME, maxConn, beamWidth);
        this.encoding = encoding;
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(elementType, useDirectIO);
    }

    public ES93HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
        ES93GenericFlatVectorsFormat.ElementType elementType,
        boolean useDirectIO,
        int numMergeWorkers,
        ExecutorService mergeExec
    ) {
        super(NAME, maxConn, beamWidth, numMergeWorkers, mergeExec);
        this.encoding = encoding;
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(elementType, useDirectIO);
    }

    @Override
    protected FlatVectorsFormat flatVectorsFormat() {
        return rawVectorFormat;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(
            state,
            maxConn,
            beamWidth,
            new Lucene104ScalarQuantizedVectorsWriter(state, encoding, rawVectorFormat.fieldsWriter(state), flatVectorScorer),
            numMergeWorkers,
            mergeExec,
            0
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(
            state,
            new Lucene104ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer)
        );
    }
}
