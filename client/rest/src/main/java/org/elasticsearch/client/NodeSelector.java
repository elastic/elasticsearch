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

import java.util.Iterator;

/**
 * Selects nodes that can receive requests. Used to keep requests away
 * from master nodes or to send them to nodes with a particular attribute.
 * Use with {@link RestClientBuilder#setNodeSelector(NodeSelector)}.
 */
public interface NodeSelector {
    /**
     * Select the {@link Node}s to which to send requests. This is called with
     * a mutable {@link Iterable} of {@linkplain Node}s in the order that the
     * rest client would prefer to use them and implementers should remove
     * nodes from the that should not receive the request. Implementers may
     * iterate the nodes as many times as they need.
     * <p>
     * This may be called twice per request: first for "living" nodes that
     * have not been blacklisted by previous errors. If the selector removes
     * all nodes from the list or if there aren't any living nodes then the
     * {@link RestClient} will call this method with a list of "dead" nodes.
     * <p>
     * Implementers should not rely on the ordering of the nodes.
     */
    void select(Iterable<Node> nodes);
    /*
     * We were fairly careful with our choice of Iterable here. The caller has
     * a List but reordering the list is likely to break round robin. Luckily
     * Iterable doesn't allow any reordering.
     */

    /**
     * Selector that matches any node.
     */
    NodeSelector ANY = new NodeSelector() {
        @Override
        public void select(Iterable<Node> nodes) {
            // Intentionally does nothing
        }

        @Override
        public String toString() {
            return "ANY";
        }
    };

    /**
     * Selector that matches any node that has metadata and doesn't
     * have the {@code master} role OR it has the data {@code data}
     * role.
     */
    NodeSelector SKIP_DEDICATED_MASTERS = new NodeSelector() {
        @Override
        public void select(Iterable<Node> nodes) {
            for (Iterator<Node> itr = nodes.iterator(); itr.hasNext();) {
                Node node = itr.next();
                if (node.getRoles() == null) continue;
                if (node.getRoles().isMasterEligible()
                        && false == node.getRoles().canContainData()
                        && false == node.getRoles().isIngest()) {
                    itr.remove();
                }
            }
        }

        @Override
        public String toString() {
            return "SKIP_DEDICATED_MASTERS";
        }
    };
}
