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

package org.elasticsearch.legacy.action.admin.indices.mapping.put;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 * Put mapping action.
 */
public class TransportPutMappingAction extends TransportMasterNodeOperationAction<PutMappingRequest, PutMappingResponse> {

    private final MetaDataMappingService metaDataMappingService;

    @Inject
    public TransportPutMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataMappingService metaDataMappingService) {
        super(settings, PutMappingAction.NAME, transportService, clusterService, threadPool);
        this.metaDataMappingService = metaDataMappingService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutMappingRequest newRequest() {
        return new PutMappingRequest();
    }

    @Override
    protected PutMappingResponse newResponse() {
        return new PutMappingResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, clusterService.state().metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected void masterOperation(final PutMappingRequest request, final ClusterState state, final ActionListener<PutMappingResponse> listener) throws ElasticsearchException {
        final String[] concreteIndices = clusterService.state().metaData().concreteIndices(request.indicesOptions(), request.indices());
        PutMappingClusterStateUpdateRequest updateRequest = new PutMappingClusterStateUpdateRequest()
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .indices(concreteIndices).type(request.type())
                .source(request.source()).ignoreConflicts(request.ignoreConflicts());

        metaDataMappingService.putMapping(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new PutMappingResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to put mappings on indices [{}], type [{}]", t, concreteIndices, request.type());
                listener.onFailure(t);
            }
        });
    }
}
