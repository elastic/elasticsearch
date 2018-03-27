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

/**
 * Selects hosts that can receive requests. Use with
 * {@link RestClientActions#withHostSelector withHostSelector}.
 */
public interface HostSelector {
    /**
     * Selector that matches any node.
     */
    HostSelector ANY = new HostSelector() {
        @Override
        public boolean select(HttpHost host, HostMetadata meta) {
            return true;
        }

        @Override
        public String toString() {
            return "ANY";
        }
    };

    /**
     * Selector that matches any node that has metadata and doesn't
     * have the {@code master} role. These nodes cannot become a "master"
     * node for the Elasticsearch cluster.
     */
    HostSelector NOT_MASTER = new HostSelector() {
        @Override
        public boolean select(HttpHost host, HostMetadata meta) {
            return meta != null && false == meta.roles().master();
        }

        @Override
        public String toString() {
            return "NOT_MASTER";
        }
    };

    /**
     * Return {@code true} if the provided host should be used for requests,
     * {@code false} otherwise.
     * @param host the host being checked
     * @param meta metadata about the host being checked or {@code null} if
     *      the {@linkplain RestClient} doesn't have any metadata about the
     *      host.
     */
    boolean select(HttpHost host, HostMetadata meta);
}
