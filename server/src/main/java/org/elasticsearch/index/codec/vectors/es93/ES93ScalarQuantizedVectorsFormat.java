/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES93ScalarQuantizedVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "ES93ScalarQuantizedVectorsFormat";

    static final Lucene104ScalarQuantizedVectorScorer flatVectorScorer = new Lucene104ScalarQuantizedVectorScorer(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    private final Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding;
    private final FlatVectorsFormat rawVectorFormat;

    public ES93ScalarQuantizedVectorsFormat(
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding,
        boolean useBFloat16,
        boolean useDirectIO
    ) {
        super(NAME);
        this.encoding = encoding;
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(useBFloat16, useDirectIO);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return NAME
            + "(name="
            + NAME
            + ", encoding="
            + encoding
            + ", flatVectorScorer="
            + flatVectorScorer
            + ", rawVectorFormat="
            + rawVectorFormat
            + ")";
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene104ScalarQuantizedVectorsWriter(state, encoding, rawVectorFormat.fieldsWriter(state), flatVectorScorer) {
        };
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene104ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer);
    }
}
