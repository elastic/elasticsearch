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

import java.io.IOException;

/**
 * An interface that provides an HNSW graph. This interface is useful when gathering multiple HNSW
 * graphs to bootstrap segment merging. The graph may be off the JVM heap.
 */
public interface HnswGraphProvider {
    /**
     * Return the stored HnswGraph for the given field.
     *
     * @param field the field containing the graph
     * @return the HnswGraph for the given field if found
     * @throws IOException when reading potentially off-heap graph fails
     */
    HnswGraph getGraph(String field) throws IOException;
}
