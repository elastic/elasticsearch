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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.support.replication.IndexReplicationOperationRequest;

/**
 * Delete request to execute on all shards that belong to a specific index.
 * Used when routing is required but not specified within the delete request.
 */
class IndexDeleteRequest extends IndexReplicationOperationRequest<IndexDeleteRequest> {

    private final String type;
    private final String id;
    private final boolean refresh;
    private final long version;
    private final String originalIndex;

    IndexDeleteRequest(DeleteRequest request, String concreteIndex) {
        super(concreteIndex, request.timeout(), request.replicationType(), request.consistencyLevel(),
                request.indices(), request.indicesOptions(), request);
        this.type = request.type();
        this.id = request.id();
        this.refresh = request.refresh();
        this.version = request.version();
        this.originalIndex = request.index();
    }

    String type() {
        return this.type;
    }

    String id() {
        return this.id;
    }

    boolean refresh() {
        return this.refresh;
    }

    long version() {
        return this.version;
    }

    String originalIndex() {
        return originalIndex;
    }
}
