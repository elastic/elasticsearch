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
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es819.hnsw;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.InfoStream;

import java.io.IOException;

/**
 * Abstraction of merging multiple graphs into one on-heap graph
 *
 * @lucene.experimental
 */
public interface HnswGraphMerger {

    /**
     * Adds a reader to the graph merger to record the state
     *
     * @param reader KnnVectorsReader to add to the merger
     * @param docMap MergeState.DocMap for the reader
     * @param liveDocs Bits representing live docs, can be null
     * @return this
     * @throws IOException If an error occurs while reading from the merge state
     */
    HnswGraphMerger addReader(KnnVectorsReader reader, MergeState.DocMap docMap, Bits liveDocs) throws IOException;

    /**
     * Merge and produce the on heap graph
     *
     * @param mergedVectorValues view of the vectors in the merged segment
     * @param infoStream optional info stream to set to builder
     * @param maxOrd max number of vectors that will be added to the graph
     * @return merged graph
     * @throws IOException during merge
     */
    OnHeapHnswGraph merge(KnnVectorValues mergedVectorValues, InfoStream infoStream, int maxOrd) throws IOException;
}
