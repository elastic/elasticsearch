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

package org.elasticsearch.action.deletebyquery;

import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.Set;

/**
 * Delete by query request to execute on a specific index.
 */
class IndexDeleteByQueryRequest extends IndexReplicationOperationRequest<IndexDeleteByQueryRequest> {

    private final BytesReference source;
    private final String[] types;
    @Nullable
    private final Set<String> routing;
    @Nullable
    private final String[] filteringAliases;
    private final long nowInMillis;

    IndexDeleteByQueryRequest(DeleteByQueryRequest request, String index, @Nullable Set<String> routing, @Nullable String[] filteringAliases,
                              long nowInMillis) {
        super(index, request.timeout(), request.consistencyLevel(), request.indices(), request.indicesOptions(), request);
        this.source = request.source();
        this.types = request.types();
        this.routing = routing;
        this.filteringAliases = filteringAliases;
        this.nowInMillis = nowInMillis;
    }

    BytesReference source() {
        return source;
    }

    Set<String> routing() {
        return this.routing;
    }

    String[] types() {
        return this.types;
    }

    String[] filteringAliases() {
        return filteringAliases;
    }

    long nowInMillis() {
        return nowInMillis;
    }
}
