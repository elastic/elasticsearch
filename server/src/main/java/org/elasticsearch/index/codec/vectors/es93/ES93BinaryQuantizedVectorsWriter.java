/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.BinaryQuantizedVectorsFieldWriter;
import org.elasticsearch.index.codec.vectors.es818.ES818BinaryFlatVectorsScorer;
import org.elasticsearch.index.codec.vectors.es818.ES818BinaryQuantizedVectorsWriter;

import java.io.IOException;

/**
 * ES93-specific BQ writer that produces {@link ES93BinaryQuantizedFieldWriter}
 * instances backed by {@link ES93FlatFieldVectorsWriter}.  When the flat writer
 * migrates to MemorySegment storage, the BQ field writer adapts alongside it.
 */
class ES93BinaryQuantizedVectorsWriter extends ES818BinaryQuantizedVectorsWriter {

    ES93BinaryQuantizedVectorsWriter(ES818BinaryFlatVectorsScorer scorer, FlatVectorsWriter rawVectorDelegate, SegmentWriteState state)
        throws IOException {
        super(scorer, rawVectorDelegate, state);
    }

    @Override
    protected BinaryQuantizedVectorsFieldWriter createFieldWriter(
        FieldInfo fieldInfo,
        FlatFieldVectorsWriter<float[]> flatFieldVectorsWriter
    ) {
        if (flatFieldVectorsWriter instanceof ES93FlatFieldVectorsWriter<float[]> es93Writer) {
            return new ES93BinaryQuantizedFieldWriter(fieldInfo, es93Writer);
        }
        return super.createFieldWriter(fieldInfo, flatFieldVectorsWriter);
    }
}
