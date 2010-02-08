/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.gateway.snapshot;

import org.elasticsearch.action.support.replication.IndicesReplicationOperationRequest;
import org.elasticsearch.util.TimeValue;

/**
 * @author kimchy (Shay Banon)
 */
public class GatewaySnapshotRequest extends IndicesReplicationOperationRequest {

    public GatewaySnapshotRequest(String index) {
        this(new String[]{index});
    }

    public GatewaySnapshotRequest(String... indices) {
        this.indices = indices;
    }

    GatewaySnapshotRequest() {

    }

    @Override public GatewaySnapshotRequest listenerThreaded(boolean threadedListener) {
        super.listenerThreaded(threadedListener);
        return this;
    }

    public GatewaySnapshotRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }
}