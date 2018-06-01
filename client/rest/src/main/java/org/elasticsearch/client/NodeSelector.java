/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Selects nodes that can receive requests. Used to keep requests away
 * from master nodes or to send them to nodes with a particular attribute.
 * Use with {@link RequestOptions.Builder#setNodeSelector(NodeSelector)}.
 */
public interface NodeSelector {
    /**
     * Select the {@link Node}s to which to send requests. This is called with
     * a list of {@linkplain Node}s in the order that the rest client would
     * prefer to use them and it should remove nodes from the list that should
     * not receive the request.
     * <p>
     * This may be called twice per request: first for "living" nodes that
     * have not been blacklisted by previous errors. In this case the order
     * of the nodes is the order in which the client thinks that they should
     * be tried next. If the selector removes all nodes from the list or if
     * there aren't any living nodes then the the client will call this method
     * with a list of "dead" nodes. In this case the list is sorted "soonest
     * to be revived" first. In this case the rest client will only attempt
     * the first node.
     * <p>
     * Implementations <strong>may</strong> reorder the list but they should
     * be careful in doing so as the original order is important (see above).
     * An implementation that sorts list consistently will consistently send
     * requests to s single node, overloading it. So implementations that
     * reorder the list should take the original order into account
     * <strong>somehow</strong>.
     *
     * @return a subset of the provided list of {@linkplain Node}s that the
     *      selector approves of, in the order that the selector would prefer
     *      to use them.
     */
    List<Node> select(List<Node> nodes);

    /**
     * Selector that matches any node.
     */
    NodeSelector ANY = new NodeSelector() {
        @Override
        public List<Node> select(List<Node> nodes) {
            return nodes;
        }

        @Override
        public String toString() {
            return "ANY";
        }
    };

    /**
     * Selector that matches any node that has metadata and doesn't
     * have the {@code master} role OR it has the data {@code data}
     * role. It does not reorder the nodes sent to it.
     */
    NodeSelector NOT_MASTER_ONLY = new NodeSelector() {
        @Override
        public List<Node> select(List<Node> nodes) {
            List<Node> subset = new ArrayList<>(nodes.size());
            for (Node node : nodes) {
                if (node.getRoles() == null) continue;
                if (false == node.getRoles().isMasterEligible() || node.getRoles().isData()) {
                    subset.add(node);
                }
            }
            return subset;
        }

        @Override
        public String toString() {
            return "NOT_MASTER_ONLY";
        }
    };
}
