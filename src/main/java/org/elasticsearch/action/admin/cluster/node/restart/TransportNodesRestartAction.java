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

package org.elasticsearch.action.admin.cluster.node.restart;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.unit.TimeValue.readTimeValue;

/**
 *
 */
public class TransportNodesRestartAction extends TransportNodesOperationAction<NodesRestartRequest, NodesRestartResponse, TransportNodesRestartAction.NodeRestartRequest, NodesRestartResponse.NodeRestartResponse> {

    private final Node node;

    private final boolean disabled;

    private AtomicBoolean restartRequested = new AtomicBoolean();

    @Inject
    public TransportNodesRestartAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                       ClusterService clusterService, TransportService transportService,
                                       Node node) {
        super(settings, NodesRestartAction.NAME, clusterName, threadPool, clusterService, transportService);
        this.node = node;
        disabled = componentSettings.getAsBoolean("disabled", false);
    }

    @Override
    protected void doExecute(NodesRestartRequest nodesRestartRequest, ActionListener<NodesRestartResponse> listener) {
        listener.onFailure(new ElasticsearchIllegalStateException("restart is disabled (for now) ...."));
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected NodesRestartResponse newResponse(NodesRestartRequest nodesShutdownRequest, AtomicReferenceArray responses) {
        final List<NodesRestartResponse.NodeRestartResponse> nodeRestartResponses = newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodesRestartResponse.NodeRestartResponse) {
                nodeRestartResponses.add((NodesRestartResponse.NodeRestartResponse) resp);
            }
        }
        return new NodesRestartResponse(clusterName, nodeRestartResponses.toArray(new NodesRestartResponse.NodeRestartResponse[nodeRestartResponses.size()]));
    }

    @Override
    protected NodesRestartRequest newRequest() {
        return new NodesRestartRequest();
    }

    @Override
    protected NodeRestartRequest newNodeRequest() {
        return new NodeRestartRequest();
    }

    @Override
    protected NodeRestartRequest newNodeRequest(String nodeId, NodesRestartRequest request) {
        return new NodeRestartRequest(nodeId, request);
    }

    @Override
    protected NodesRestartResponse.NodeRestartResponse newNodeResponse() {
        return new NodesRestartResponse.NodeRestartResponse();
    }

    @Override
    protected NodesRestartResponse.NodeRestartResponse nodeOperation(NodeRestartRequest request) throws ElasticsearchException {
        if (disabled) {
            throw new ElasticsearchIllegalStateException("Restart is disabled");
        }
        if (!restartRequested.compareAndSet(false, true)) {
            return new NodesRestartResponse.NodeRestartResponse(clusterService.localNode());
        }
        logger.info("Restarting in [{}]", request.delay);
        threadPool.schedule(request.delay, ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                boolean restartWithWrapper = false;
                if (System.getProperty("elasticsearch-service") != null) {
                    try {
                        Class wrapperManager = settings.getClassLoader().loadClass("org.tanukisoftware.wrapper.WrapperManager");
                        logger.info("Initiating requested restart (using service)");
                        wrapperManager.getMethod("restartAndReturn").invoke(null);
                        restartWithWrapper = true;
                    } catch (Throwable e) {
                        logger.error("failed to initial restart on service wrapper", e);
                    }
                }
                if (!restartWithWrapper) {
                    logger.info("Initiating requested restart");
                    try {
                        node.stop();
                        node.start();
                    } catch (Exception e) {
                        logger.warn("Failed to restart", e);
                    } finally {
                        restartRequested.set(false);
                    }
                }
            }
        });
        return new NodesRestartResponse.NodeRestartResponse(clusterService.localNode());
    }

    @Override
    protected boolean accumulateExceptions() {
        return false;
    }

    protected static class NodeRestartRequest extends NodeOperationRequest {

        TimeValue delay;

        private NodeRestartRequest() {
        }

        private NodeRestartRequest(String nodeId, NodesRestartRequest request) {
            super(request, nodeId);
            this.delay = request.delay;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            delay = readTimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            delay.writeTo(out);
        }
    }
}