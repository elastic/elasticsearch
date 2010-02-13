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
 * Administrative actions/operations against indices.
 *
 * @author kimchy (Shay Banon)
 * @see AdminClient#indices()
 */
public interface IndicesAdminClient {

    /**
     * The status of one or more indices.
     *
     * @param request The indices status request
     * @return The result future
     * @see Requests#indicesStatus(String...)
     */
    ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request);

    /**
     * The status of one or more indices.
     *
     * @param request  The indices status request
     * @param listener A listener to be notified with a result
     * @return The result future
     * @see Requests#indicesStatus(String...)
     */
    ActionFuture<IndicesStatusResponse> status(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener);

    /**
     * The status of one or more indices.
     *
     * @param request  The indices status request
     * @param listener A listener to be notified with a result
     * @see Requests#indicesStatus(String...)
     */
    void execStatus(IndicesStatusRequest request, ActionListener<IndicesStatusResponse> listener);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request The create index request
     * @return The result future
     * @see org.elasticsearch.client.Requests#createIndexRequest(String)
     */
    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request  The create index request
     * @param listener A listener to be notified with a result
     * @return The result future
     * @see org.elasticsearch.client.Requests#createIndexRequest(String)
     */
    ActionFuture<CreateIndexResponse> create(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    /**
     * Creates an index using an explicit request allowing to specify the settings of the index.
     *
     * @param request  The create index request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#createIndexRequest(String)
     */
    void execCreate(CreateIndexRequest request, ActionListener<CreateIndexResponse> listener);

    /**
     * Deletes an index based on the index name.
     *
     * @param request The delete index request
     * @return The result future
     * @see org.elasticsearch.client.Requests#deleteIndexRequest(String)
     */
    ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request);

    /**
     * Deletes an index based on the index name.
     *
     * @param request  The delete index request
     * @param listener A listener to be notified with a result
     * @return The result future
     * @see org.elasticsearch.client.Requests#deleteIndexRequest(String)
     */
    ActionFuture<DeleteIndexResponse> delete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener);

    /**
     * Deletes an index based on the index name.
     *
     * @param request  The delete index request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#deleteIndexRequest(String)
     */
    void execDelete(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request The refresh request
     * @return The result future
     * @see org.elasticsearch.client.Requests#refreshRequest(String...)
     */
    ActionFuture<RefreshResponse> refresh(RefreshRequest request);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request  The refresh request
     * @param listener A listener to be notified with a result
     * @return The result future
     * @see org.elasticsearch.client.Requests#refreshRequest(String...)
     */
    ActionFuture<RefreshResponse> refresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    /**
     * Explicitly refresh one or more indices (making the content indexed since the last refresh searchable).
     *
     * @param request  The refresh request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#refreshRequest(String...)
     */
    void execRefresh(RefreshRequest request, ActionListener<RefreshResponse> listener);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request The flush request
     * @return A result future
     * @see org.elasticsearch.client.Requests#flushRequest(String...)
     */
    ActionFuture<FlushResponse> flush(FlushRequest request);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request  The flush request
     * @param listener A listener to be notified with a result
     * @return A result future
     * @see org.elasticsearch.client.Requests#flushRequest(String...)
     */
    ActionFuture<FlushResponse> flush(FlushRequest request, ActionListener<FlushResponse> listener);

    /**
     * Explicitly flush one or more indices (releasing memory from the node).
     *
     * @param request  The flush request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#flushRequest(String...)
     */
    void execFlush(FlushRequest request, ActionListener<FlushResponse> listener);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request The create mapping request
     * @return A result future
     * @see org.elasticsearch.client.Requests#createMappingRequest(String...)
     */
    ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request  The create mapping request
     * @param listener A listener to be notified with a result
     * @return A result future
     * @see org.elasticsearch.client.Requests#createMappingRequest(String...)
     */
    ActionFuture<CreateMappingResponse> createMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener);

    /**
     * Add mapping definition for a type into one or more indices.
     *
     * @param request  The create mapping request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#createMappingRequest(String...)
     */
    void execCreateMapping(CreateMappingRequest request, ActionListener<CreateMappingResponse> listener);

    /**
     * Explicitly perform gateway snapshot for one or more indices.
     *
     * @param request The gateway snapshot request
     * @return The result future
     * @see org.elasticsearch.client.Requests#gatewaySnapshotRequest(String...)
     */
    ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request);

    /**
     * Explicitly perform gateway snapshot for one or more indices.
     *
     * @param request  The gateway snapshot request
     * @param listener A listener to be notified with a result
     * @return The result future
     * @see org.elasticsearch.client.Requests#gatewaySnapshotRequest(String...)
     */
    ActionFuture<GatewaySnapshotResponse> gatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener);

    /**
     * Explicitly perform gateway snapshot for one or more indices.
     *
     * @param request  The gateway snapshot request
     * @param listener A listener to be notified with a result
     * @see org.elasticsearch.client.Requests#gatewaySnapshotRequest(String...)
     */
    void execGatewaySnapshot(GatewaySnapshotRequest request, ActionListener<GatewaySnapshotResponse> listener);
}
