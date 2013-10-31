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

package org.elasticsearch.action.support.master;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Base action to apply changes to the cluster state
 *
 * @param <UpdateRequest> request used for the cluster state update
 * @param <UpdateResponse> response obtained from the cluster state update operation
 * @param <Request> request received
 * @param <Response> response returned
 */
public abstract class TransportClusterStateUpdateAction<UpdateRequest extends ClusterStateUpdateRequest<UpdateRequest>, UpdateResponse extends ClusterStateUpdateResponse,
        Request extends AcknowledgedRequest<Request>, Response extends AcknowledgedResponse> extends TransportMasterNodeOperationAction<Request, Response> {

    protected TransportClusterStateUpdateAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws ElasticSearchException {

        UpdateRequest updateRequest = newClusterStateUpdateRequest(request)
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout());

        ClusterStateUpdateActionListener<UpdateResponse, Response> updateListener = newClusterStateUpdateActionListener(updateRequest, listener);

        updateClusterState(updateRequest, updateListener);
    }

    protected ClusterStateUpdateActionListener<UpdateResponse, Response> newClusterStateUpdateActionListener(final UpdateRequest request, ActionListener<Response> listener) {
        return new ClusterStateUpdateActionListener<UpdateResponse, Response>(listener) {
            @Override
            protected Response newResponse(UpdateResponse clusterStateUpdateResponse) {
                return TransportClusterStateUpdateAction.this.newResponse(clusterStateUpdateResponse);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to execute cluster state update {}", t, request);
                super.onFailure(t);
            }
        };
    }

    protected abstract UpdateRequest newClusterStateUpdateRequest(Request acknowledgedRequest);

    protected abstract Response newResponse(UpdateResponse updateResponse);

    protected abstract void updateClusterState(UpdateRequest updateRequest, ClusterStateUpdateActionListener<UpdateResponse, Response> listener);
}
