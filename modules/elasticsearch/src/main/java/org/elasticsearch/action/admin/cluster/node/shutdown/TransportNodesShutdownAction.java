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

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.nodes.NodeOperationRequest;
import org.elasticsearch.action.support.nodes.TransportNodesOperationAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportNodesShutdownAction extends TransportNodesOperationAction<NodesShutdownRequest, NodesShutdownResponse, TransportNodesShutdownAction.NodeShutdownRequest, NodesShutdownResponse.NodeShutdownResponse> {

    private final Node node;

    private final boolean disabled;

    @Inject public TransportNodesShutdownAction(Settings settings, ClusterName clusterName, ThreadPool threadPool,
                                                ClusterService clusterService, TransportService transportService,
                                                Node node) {
        super(settings, clusterName, threadPool, clusterService, transportService);
        this.node = node;
        disabled = componentSettings.getAsBoolean("disabled", false);
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Cluster.Node.SHUTDOWN;
    }

    @Override protected String transportNodeAction() {
        return "/cluster/nodes/shutdown/node";
    }

    @Override protected NodesShutdownResponse newResponse(NodesShutdownRequest nodesShutdownRequest, AtomicReferenceArray responses) {
        final List<NodesShutdownResponse.NodeShutdownResponse> nodeShutdownResponses = newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodesShutdownResponse.NodeShutdownResponse) {
                nodeShutdownResponses.add((NodesShutdownResponse.NodeShutdownResponse) resp);
            }
        }
        return new NodesShutdownResponse(clusterName, nodeShutdownResponses.toArray(new NodesShutdownResponse.NodeShutdownResponse[nodeShutdownResponses.size()]));
    }

    @Override protected NodesShutdownRequest newRequest() {
        return new NodesShutdownRequest();
    }

    @Override protected NodeShutdownRequest newNodeRequest() {
        return new NodeShutdownRequest();
    }

    @Override protected NodeShutdownRequest newNodeRequest(String nodeId, NodesShutdownRequest request) {
        return new NodeShutdownRequest(nodeId, request.delay);
    }

    @Override protected NodesShutdownResponse.NodeShutdownResponse newNodeResponse() {
        return new NodesShutdownResponse.NodeShutdownResponse();
    }

    @Override protected NodesShutdownResponse.NodeShutdownResponse nodeOperation(final NodeShutdownRequest request) throws ElasticSearchException {
        if (disabled) {
            throw new ElasticSearchIllegalStateException("Shutdown is disabled");
        }
        logger.info("Shutting down in [{}]", request.delay);
        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(request.delay.millis());
                } catch (InterruptedException e) {
                    // ignore
                }
                boolean shutdownWithWrapper = false;
                if (System.getProperty("elasticsearch-service") != null) {
                    try {
                        Class wrapperManager = settings.getClassLoader().loadClass("org.tanukisoftware.wrapper.WrapperManager");
                        logger.info("Initiating requested shutdown (using service)");
                        wrapperManager.getMethod("stopAndReturn", int.class).invoke(null, 0);
                        shutdownWithWrapper = true;
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
                if (!shutdownWithWrapper) {
                    logger.info("Initiating requested shutdown");
                    try {
                        node.close();
                    } catch (Exception e) {
                        logger.warn("Failed to shutdown", e);
                    } finally {
                        // make sure we initiate the shutdown hooks, so the Bootstrap#main thread will exit
                        System.exit(0);
                    }
                }
            }
        });
        t.start();
        return new NodesShutdownResponse.NodeShutdownResponse(clusterService.state().nodes().localNode());
    }

    @Override protected boolean accumulateExceptions() {
        return false;
    }

    protected static class NodeShutdownRequest extends NodeOperationRequest {

        TimeValue delay;

        private NodeShutdownRequest() {
        }

        private NodeShutdownRequest(String nodeId, TimeValue delay) {
            super(nodeId);
            this.delay = delay;
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            delay = readTimeValue(in);
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            delay.writeTo(out);
        }
    }
}