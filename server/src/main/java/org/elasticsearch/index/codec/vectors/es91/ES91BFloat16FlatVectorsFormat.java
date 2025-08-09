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
package org.elasticsearch.index.codec.vectors.es91;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class ES91BFloat16FlatVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "ES91BFloat16FlatVectorsFormat";
    static final String META_CODEC_NAME = "ES91BFloat16FlatVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "ES91BFloat16FlatVectorsFormatData";
    static final String META_EXTENSION = "vemf";
    static final String VECTOR_DATA_EXTENSION = "vec";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public ES91BFloat16FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES91BFloat16FlatVectorsWriter(state, vectorsScorer);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES91BFloat16FlatVectorsReader(state, vectorsScorer);
    }

    @Override
    public String toString() {
        return "Lucene99FlatVectorsFormat(" + "vectorsScorer=" + vectorsScorer + ')';
    }
}
