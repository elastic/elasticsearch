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

package org.elasticsearch.cluster.action.index;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class NodeIndexCreatedAction extends AbstractComponent {

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final List<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject
    public NodeIndexCreatedAction(Settings settings, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        transportService.registerHandler(NodeIndexCreatedTransportHandler.ACTION, new NodeIndexCreatedTransportHandler());
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    public void nodeIndexCreated(final ClusterState clusterState, final String index, final String nodeId) throws ElasticSearchException {
        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes.localNodeMaster()) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    innerNodeIndexCreated(index, nodeId);
                }
            });
        } else {
            transportService.sendRequest(clusterState.nodes().masterNode(),
                    NodeIndexCreatedTransportHandler.ACTION, new NodeIndexCreatedMessage(index, nodeId), EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    private void innerNodeIndexCreated(String index, String nodeId) {
        for (Listener listener : listeners) {
            listener.onNodeIndexCreated(index, nodeId);
        }
    }

    public static interface Listener {
        void onNodeIndexCreated(String index, String nodeId);
    }

    private class NodeIndexCreatedTransportHandler extends BaseTransportRequestHandler<NodeIndexCreatedMessage> {

        static final String ACTION = "cluster/nodeIndexCreated";

        @Override
        public NodeIndexCreatedMessage newInstance() {
            return new NodeIndexCreatedMessage();
        }

        @Override
        public void messageReceived(NodeIndexCreatedMessage message, TransportChannel channel) throws Exception {
            innerNodeIndexCreated(message.index, message.nodeId);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class NodeIndexCreatedMessage extends TransportRequest {
        String index;
        String nodeId;

        NodeIndexCreatedMessage() {
        }

        NodeIndexCreatedMessage(String index, String nodeId) {
            this.index = index;
            this.nodeId = nodeId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(nodeId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
            nodeId = in.readString();
        }
    }
}
