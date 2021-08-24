/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.List;

/**
 * Groups nodes by priority. Used to send requests to nodes with a higher priority with fallback
 * to lower priority nodes.
 * Use with {@link RestClientBuilder#setNodePriorityStrategy(NodePriorityStrategy)}.
 */
public interface NodePriorityStrategy {
    /**
     * Groups {@link Node}s to which to send requests by priority. This is called with
     * a mutable {@link List} of {@linkplain Node}s.
     *
     * Implementers should return mutable list objects.
     */
    List<List<Node>> groupByPriority(List<Node> nodes);

    /**
     * Strategy with no priority
     */
    NodePriorityStrategy NO_PRIORITY = new NodePriorityStrategy() {
        @Override
        public List<List<Node>> groupByPriority(List<Node> nodes) {
            throw new UnsupportedOperationException("Implementation used as marker");
        }

        @Override
        public String toString() {
            return "NO_PRIORITY";
        }
    };
}
