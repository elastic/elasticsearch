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
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10
 */
public class ES816BinaryQuantizedRWVectorsFormat extends ES816BinaryQuantizedVectorsFormat {

    private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    private static final ES816BinaryFlatRWVectorsScorer scorer = new ES816BinaryFlatRWVectorsScorer(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    /** Creates a new instance with the default number of vectors per cluster. */
    public ES816BinaryQuantizedRWVectorsFormat() {
        super();
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES816BinaryQuantizedVectorsWriter(scorer, rawVectorFormat.fieldsWriter(state), state);
    }
}
