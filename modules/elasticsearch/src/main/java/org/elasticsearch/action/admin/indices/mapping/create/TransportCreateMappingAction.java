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

package org.elasticsearch.action.admin.indices.mapping.create;

import com.google.inject.Inject;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.Actions;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.BaseAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;
import org.elasticsearch.util.io.VoidStreamable;
import org.elasticsearch.util.settings.Settings;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class TransportCreateMappingAction extends BaseAction<CreateMappingRequest, CreateMappingResponse> {

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final MetaDataService metaDataService;

    private final ThreadPool threadPool;

    @Inject public TransportCreateMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                ThreadPool threadPool, MetaDataService metaDataService) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.metaDataService = metaDataService;

        transportService.registerHandler(TransportActions.Admin.Indices.Mapping.CREATE, new TransportHandler());
    }

    @Override protected void doExecute(final CreateMappingRequest request, final ActionListener<CreateMappingResponse> listener) {
        final String[] indices = Actions.processIndices(clusterService.state(), request.indices());
        if (clusterService.state().nodes().localNodeMaster()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        metaDataService.addMapping(indices, request.mappingType(), request.mappingSource());
                        listener.onResponse(new CreateMappingResponse());
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(), TransportActions.Admin.Indices.Mapping.CREATE, request,
                    new VoidTransportResponseHandler() {
                        @Override public void handleResponse(VoidStreamable response) {
                            listener.onResponse(new CreateMappingResponse());
                        }

                        @Override public void handleException(RemoteTransportException exp) {
                            listener.onFailure(exp);
                        }
                    });
        }
    }

    private class TransportHandler extends BaseTransportRequestHandler<CreateMappingRequest> {


        @Override public CreateMappingRequest newInstance() {
            return new CreateMappingRequest();
        }

        @Override public void messageReceived(final CreateMappingRequest request, final TransportChannel channel) throws Exception {
            String[] indices = Actions.processIndices(clusterService.state(), request.indices());
            if (clusterService.state().nodes().localNodeMaster()) {
                // handle the actual creation of a new index
                metaDataService.addMapping(indices, request.mappingType(), request.mappingSource());
                channel.sendResponse(VoidStreamable.INSTANCE);
            } else {
                transportService.sendRequest(clusterService.state().nodes().masterNode(), TransportActions.Admin.Indices.Mapping.CREATE, request, new VoidTransportResponseHandler() {

                    @Override public void handleResponse(VoidStreamable response) {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Failed to send response", e);
                        }
                    }

                    @Override public void handleException(RemoteTransportException exp) {
                        try {
                            channel.sendResponse(exp);
                        } catch (IOException e) {
                            logger.error("Failed to send response", e);
                        }
                    }
                });
            }
        }
    }
}