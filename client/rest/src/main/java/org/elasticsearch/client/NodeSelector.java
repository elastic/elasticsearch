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

public interface NodeSelector {
    /**
     * Selector that matches any node.
     */
    NodeSelector ANY = new NodeSelector() {
        @Override
        public boolean select(Node node) {
            return true;
        }
    };

    /**
     * Selector that matches any node that doesn't have the master roles
     * <strong>or</strong> doesn't have any information about roles.
     */
    NodeSelector NOT_MASTER = new NodeSelector() {
        @Override
        public boolean select(Node node) {
            return node.roles() == null || false == node.roles().master();
        }
    };

    /**
     * Return {@code true} if this node should be used for requests, {@code false}
     * otherwise.
     */
    boolean select(Node node);
}
