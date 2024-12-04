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
package org.elasticsearch.index.codec.vectors.es816;

import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public class ES816BinaryQuantizedVectorsFormat extends FlatVectorsFormat {

    public static final String BINARIZED_VECTOR_COMPONENT = "BVEC";
    public static final String NAME = "ES816BinaryQuantizedVectorsFormat";

    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
    static final String META_CODEC_NAME = "ES816BinaryQuantizedVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "ES816BinaryQuantizedVectorsFormatData";
    static final String META_EXTENSION = "vemb";
    static final String VECTOR_DATA_EXTENSION = "veb";
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    private static final ES816BinaryFlatVectorsScorer scorer = new ES816BinaryFlatVectorsScorer(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    /** Creates a new instance with the default number of vectors per cluster. */
    public ES816BinaryQuantizedVectorsFormat() {
        super(NAME);
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES816BinaryQuantizedVectorsWriter(scorer, rawVectorFormat.fieldsWriter(state), state);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES816BinaryQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), scorer);
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return "ES816BinaryQuantizedVectorsFormat(name=" + NAME + ", flatVectorScorer=" + scorer + ")";
    }
}
