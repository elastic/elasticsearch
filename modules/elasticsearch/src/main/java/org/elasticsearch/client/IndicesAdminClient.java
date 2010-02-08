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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequest;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotResponse;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.create.CreateMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequest;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;

/**
 * @author kimchy (Shay Banon)
 */
public interface IndicesAdminClient {

    ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request);

    ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener);

    void execStatus(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener);

    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request);

    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    void execCreate(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request);

    ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener);

    void execDelete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener);

    ActionFuture<RefreshResponse> refresh(RefreshRequest request);

    ActionFuture<RefreshResponse> refresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    void execRefresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    ActionFuture<FlushResponse> flush(FlushRequest request);

    ActionFuture<FlushResponse> flush(FlushRequest request, ActionListener<FlushResponse> listener);

    void execFlush(FlushRequest request, ActionListener<FlushResponse> listener);

    ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request);

    ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener);

    void execCreateMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener);

    ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request);

    ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener);

    void execGatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener);
}
