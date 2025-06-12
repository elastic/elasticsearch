/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Copied from Lucene99FlatVectorsFormat in Lucene 10.1
 *
 * This is copied to change the implementation of {@link #fieldsReader} only.
 * The codec format itself is not changed, so we keep the original {@link #NAME}
 */
public class DirectIOLucene99FlatVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "Lucene99FlatVectorsFormat";
    static final String META_CODEC_NAME = "Lucene99FlatVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "Lucene99FlatVectorsFormatData";
    static final String META_EXTENSION = "vemf";
    static final String VECTOR_DATA_EXTENSION = "vec";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public DirectIOLucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        if (DirectIOLucene99FlatVectorsReader.shouldUseDirectIO(state)) {
            // Use mmap for merges and direct I/O for searches.
            // TODO: Open the mmap file with sequential access instead of random (current behavior).
            return new MergeReaderWrapper(
                new DirectIOLucene99FlatVectorsReader(state, vectorsScorer),
                new Lucene99FlatVectorsReader(state, vectorsScorer)
            );
        } else {
            return new Lucene99FlatVectorsReader(state, vectorsScorer);
        }
    }

    @Override
    public String toString() {
        return "ES818FlatVectorsFormat(" + "vectorsScorer=" + vectorsScorer + ')';
    }
}
