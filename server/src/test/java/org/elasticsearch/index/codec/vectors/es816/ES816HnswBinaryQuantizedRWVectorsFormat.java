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

import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

class ES816HnswBinaryQuantizedRWVectorsFormat extends ES816HnswBinaryQuantizedVectorsFormat {

    private static final FlatVectorsFormat flatVectorsFormat = new ES816BinaryQuantizedRWVectorsFormat();

    ES816HnswBinaryQuantizedRWVectorsFormat() {}

    ES816HnswBinaryQuantizedRWVectorsFormat(int maxConn, int beamWidth) {
        super(maxConn, beamWidth);
    }

    ES816HnswBinaryQuantizedRWVectorsFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
        super(maxConn, beamWidth, numMergeWorkers, mergeExec);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state), 1, null);
    }
}
