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

import java.util.Objects;

/**
 * Selects nodes that can receive requests. Use with
 * {@link RestClientActions#withNodeSelector withNodeSelector}.
 */
public interface NodeSelector {
    /**
     * Return {@code true} if the provided node should be used for requests,
     * {@code false} otherwise.
     */
    boolean select(Node node);

    /**
     * Selector that matches any node.
     */
    NodeSelector ANY = new NodeSelector() {
        @Override
        public boolean select(Node node) {
            return true;
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
    NodeSelector NOT_MASTER_ONLY = new NodeSelector() {
        @Override
        public boolean select(Node node) {
            return node.getRoles() != null
                && (false == node.getRoles().hasMaster() || node.getRoles().hasData());
        }

        @Override
        public String toString() {
            return "NOT_MASTER_ONLY";
        }
    };

    /**
     * Selector that returns {@code true} of both of its provided
     * selectors return {@code true}, otherwise {@code false}.
     */
    class And implements NodeSelector {
        private final NodeSelector lhs;
        private final NodeSelector rhs;

        public And(NodeSelector lhs, NodeSelector rhs) {
            this.lhs = Objects.requireNonNull(lhs, "lhs is required");
            this.rhs = Objects.requireNonNull(rhs, "rhs is required");
        }

        @Override
        public boolean select(Node node) {
            return lhs.select(node) && rhs.select(node);
        }

        @Override
        public String toString() {
            return lhs + " AND " + rhs;
        }
    }
}
