/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.cluster.ack.ClusterStateUpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * Cluster state update request that allows to add a warmer to the cluster state
 */
public class PutWarmerClusterStateUpdateRequest extends ClusterStateUpdateRequest<PutWarmerClusterStateUpdateRequest> {

    private final String name;

    private String[] indices;

    private String[] types;

    private BytesReference source;

    PutWarmerClusterStateUpdateRequest(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }

    public String[] indices() {
        return indices;
    }

    public PutWarmerClusterStateUpdateRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    public String[] types() {
        return types;
    }

    public PutWarmerClusterStateUpdateRequest types(String[] types) {
        this.types = types;
        return this;
    }

    public BytesReference source() {
        return source;
    }

    public PutWarmerClusterStateUpdateRequest source(BytesReference source) {
        this.source = source;
        return this;
    }
}
