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

package org.elasticsearch.action.search;

import java.util.Locale;

/**
 * The reduce modes when executing a cross-cluster search request
 */
public enum CCSReduceMode {
    /**
     * The coordinating node sends one search shards request to each remote cluster to collect information about the remote indices
     * involved and their shards. From then on, the search executes as if all shards were part of the same cluster, meaning that
     * each shard will receive a shard search request, execute the query, after which reduction happens in one go and relevant documents
     * are fetched from their shards. To be preferred when network latency is very low between the coordinating node and the remote
     * clusters.
     */
    LOCAL,
    /**
     * The coordinating node sends one and only one search request to each cluster. Each cluster executes the search independently,
     * performs non final reduction and returns `from` + `size` fetched hits to the coordinating node, which will perform a final
     * reduction and merge the different search responses into one. To be preferred when there is network latency between the coordinating
     * node and the remote clusters.
     */
    REMOTE,
    /**
     * Default mode. {@link #REMOTE} is used whenever possible. When a scroll is provided or inner hits are requested as part of field
     * collapsing, {@link #LOCAL} is used instead.
     */
    AUTO;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static CCSReduceMode fromString(String executionMode) {
        for (CCSReduceMode value : CCSReduceMode.values()) {
            if (value.name().toLowerCase(Locale.ROOT).equals(executionMode)) {
                return value;
            }
        }
        throw new IllegalArgumentException("unknown ccs_reduce_mode: [" + executionMode + "]");
    }
}
