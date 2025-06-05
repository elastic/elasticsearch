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

package org.elasticsearch.index.codec.vectors.es910.hnsw;

import org.apache.lucene.util.InfoStream;

import java.io.IOException;

/**
 * Interface for builder building the {@link org.apache.lucene.util.hnsw.OnHeapHnswGraph}
 *
 * @lucene.experimental
 */
public interface HnswBuilder {

    /**
     * Adds all nodes to the graph up to the provided {@code maxOrd}.
     *
     * @param maxOrd The maximum ordinal (excluded) of the nodes to be added.
     */
    OnHeapHnswGraph build(int maxOrd) throws IOException;

    /** Inserts a doc with vector value to the graph */
    void addGraphNode(int node) throws IOException;

    /** Set info-stream to output debugging information */
    void setInfoStream(InfoStream infoStream);

    OnHeapHnswGraph getGraph();

    /**
     * Once this method is called no further updates to the graph are accepted (addGraphNode will
     * throw IllegalStateException). Final modifications to the graph (eg patching up disconnected
     * components, re-ordering node ids for better delta compression) may be triggered, so callers
     * should expect this call to take some time.
     */
    OnHeapHnswGraph getCompletedGraph() throws IOException;
}
