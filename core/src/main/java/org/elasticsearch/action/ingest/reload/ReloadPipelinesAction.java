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

package org.elasticsearch.action.ingest.reload;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.PipelineStore;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * An internal api that refreshes the in-memory representation of all the pipelines on all ingest nodes.
 */
public class ReloadPipelinesAction extends AbstractComponent implements TransportRequestHandler<ReloadPipelinesAction.ReloadPipelinesRequest> {

    public static final String ACTION_NAME = "internal:admin/ingest/reload/pipelines";

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final PipelineStore pipelineStore;

    public ReloadPipelinesAction(Settings settings, PipelineStore pipelineStore, ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.pipelineStore = pipelineStore;
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerRequestHandler(ACTION_NAME, ReloadPipelinesRequest::new, ThreadPool.Names.MANAGEMENT, this);
    }

    public void reloadPipelinesOnAllNodes(Consumer<Boolean> listener) {
        AtomicBoolean failed = new AtomicBoolean();
        DiscoveryNodes nodes = clusterService.state().getNodes();
        AtomicInteger expectedResponses = new AtomicInteger(nodes.size());
        for (DiscoveryNode node : nodes) {
            ReloadPipelinesRequest nodeRequest = new ReloadPipelinesRequest();
            transportService.sendRequest(node, ACTION_NAME, nodeRequest, new TransportResponseHandler<ReloadPipelinesResponse>() {
                @Override
                public ReloadPipelinesResponse newInstance() {
                    return new ReloadPipelinesResponse();
                }

                @Override
                public void handleResponse(ReloadPipelinesResponse response) {
                    decrementAndReturn();
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.warn("failed to update pipelines on remote node [{}]", exp, node);
                    failed.set(true);
                    decrementAndReturn();
                }

                void decrementAndReturn() {
                    if (expectedResponses.decrementAndGet() == 0) {
                        listener.accept(!failed.get());
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    @Override
    public void messageReceived(ReloadPipelinesRequest request, TransportChannel channel) throws Exception {
        try {
            pipelineStore.updatePipelines();
            channel.sendResponse(new ReloadPipelinesResponse());
        } catch (Throwable e) {
            logger.warn("failed to update pipelines", e);
            channel.sendResponse(e);
        }
    }

    final static class ReloadPipelinesRequest extends TransportRequest {

    }

    final static class ReloadPipelinesResponse extends TransportResponse {

    }

}
