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

import org.apache.http.HttpHost;

public interface HostSelector {
    /**
     * Selector that matches any node.
     */
    HostSelector ANY = new HostSelector() {
        @Override
        public boolean select(HttpHost host, HostMetadata meta) {
            return true;
        }
    };

    /**
     * Selector that matches any node that doesn't have the master roles
     * <strong>or</strong> doesn't have any information about roles.
     */
    HostSelector NOT_MASTER = new HostSelector() {
        @Override
        public boolean select(HttpHost host, HostMetadata meta) {
            return meta != null && false == meta.roles().master();
        }
    };

    /**
     * Return {@code true} if the provided host should be used for requests, {@code false}
     * otherwise.
     */
    boolean select(HttpHost host, HostMetadata meta);
}
