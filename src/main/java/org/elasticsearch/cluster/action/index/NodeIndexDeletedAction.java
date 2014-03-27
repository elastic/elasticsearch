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

package org.elasticsearch.cluster.action.index;

import org.elasticsearch.ElasticsearchException;
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
public class NodeIndexDeletedAction extends AbstractComponent {

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public NodeIndexDeletedAction(Settings settings, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        transportService.registerHandler(NodeIndexDeletedTransportHandler.ACTION, new NodeIndexDeletedTransportHandler());
        transportService.registerHandler(NodeIndexStoreDeletedTransportHandler.ACTION, new NodeIndexStoreDeletedTransportHandler());
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    public void nodeIndexDeleted(final ClusterState clusterState, final String index, final String nodeId) throws ElasticsearchException {
        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes.localNodeMaster()) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    innerNodeIndexDeleted(index, nodeId);
                }
            });
        } else {
            transportService.sendRequest(clusterState.nodes().masterNode(),
                    NodeIndexDeletedTransportHandler.ACTION, new NodeIndexDeletedMessage(index, nodeId), EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    public void nodeIndexStoreDeleted(final ClusterState clusterState, final String index, final String nodeId) throws ElasticsearchException {
        DiscoveryNodes nodes = clusterState.nodes();
        if (nodes.localNodeMaster()) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    innerNodeIndexStoreDeleted(index, nodeId);
                }
            });
        } else {
            transportService.sendRequest(clusterState.nodes().masterNode(),
                    NodeIndexStoreDeletedTransportHandler.ACTION, new NodeIndexStoreDeletedMessage(index, nodeId), EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    private void innerNodeIndexDeleted(String index, String nodeId) {
        for (Listener listener : listeners) {
            listener.onNodeIndexDeleted(index, nodeId);
        }
    }

    private void innerNodeIndexStoreDeleted(String index, String nodeId) {
        for (Listener listener : listeners) {
            listener.onNodeIndexStoreDeleted(index, nodeId);
        }
    }

    public static interface Listener {
        void onNodeIndexDeleted(String index, String nodeId);

        void onNodeIndexStoreDeleted(String index, String nodeId);
    }

    private class NodeIndexDeletedTransportHandler extends BaseTransportRequestHandler<NodeIndexDeletedMessage> {

        static final String ACTION = "cluster/nodeIndexDeleted";

        @Override
        public NodeIndexDeletedMessage newInstance() {
            return new NodeIndexDeletedMessage();
        }

        @Override
        public void messageReceived(NodeIndexDeletedMessage message, TransportChannel channel) throws Exception {
            innerNodeIndexDeleted(message.index, message.nodeId);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    private class NodeIndexStoreDeletedTransportHandler extends BaseTransportRequestHandler<NodeIndexStoreDeletedMessage> {

        static final String ACTION = "cluster/nodeIndexStoreDeleted";

        @Override
        public NodeIndexStoreDeletedMessage newInstance() {
            return new NodeIndexStoreDeletedMessage();
        }

        @Override
        public void messageReceived(NodeIndexStoreDeletedMessage message, TransportChannel channel) throws Exception {
            innerNodeIndexStoreDeleted(message.index, message.nodeId);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class NodeIndexDeletedMessage extends TransportRequest {

        String index;
        String nodeId;

        NodeIndexDeletedMessage() {
        }

        NodeIndexDeletedMessage(String index, String nodeId) {
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

    static class NodeIndexStoreDeletedMessage extends TransportRequest {

        String index;
        String nodeId;

        NodeIndexStoreDeletedMessage() {
        }

        NodeIndexStoreDeletedMessage(String index, String nodeId) {
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