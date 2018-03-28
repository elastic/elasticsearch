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
import java.util.Objects;

/**
 * Selects nodes that can receive requests. Use with
 * {@link RestClientActions#withNodeSelector withNodeSelector}.
 */
public interface NodeSelector {
    /**
     * Select the {@link Node}s to which to send requests. This may be called
     * twice per request, once for "living" nodes that have not had been
     * blacklisted by previous errors if there are any. If it returns an
     * empty list when sent the living nodes or if there aren't any living
     * nodes left then this will be called with a list of "dead" nodes that
     * have been blacklisted by previous failures. In both cases it should
     * return a list of nodes sorted by its preference for which node is used.
     * If it is operating on "living" nodes that it returns function as
     * fallbacks in case of request failures. If it is operating on dead nodes
     * then the dead node that it returns is attempted but no others.
     *
     * @param nodes an unmodifiable list of {@linkplain Node}s in the order
     *      that the {@link RestClient} would prefer to use them
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

    /**
     * Selector that composes two selectors, running the "right" most selector
     * first and then running the "left" selector on the results of the "right"
     * selector.
     */
    class Compose implements NodeSelector {
        private final NodeSelector lhs;
        private final NodeSelector rhs;

        public Compose(NodeSelector lhs, NodeSelector rhs) {
            this.lhs = Objects.requireNonNull(lhs, "lhs is required");
            this.rhs = Objects.requireNonNull(rhs, "rhs is required");
        }

        @Override
        public List<Node> select(List<Node> nodes) {
            return lhs.select(rhs.select(nodes));
        }

        @Override
        public String toString() {
            // . as in haskell's "compose" operator
            return lhs + "." + rhs;
        }
    }
}
