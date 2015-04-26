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

import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class NodeIndexDeletedAction extends AbstractComponent {

    public static final String INDEX_DELETED_ACTION_NAME = "internal:cluster/node/index/deleted";
    public static final String INDEX_STORE_DELETED_ACTION_NAME = "internal:cluster/node/index_store/deleted";

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final IndicesService indicesService;

    @Inject
    public NodeIndexDeletedAction(Settings settings, ThreadPool threadPool, TransportService transportService, IndicesService indicesService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        transportService.registerRequestHandler(INDEX_DELETED_ACTION_NAME, NodeIndexDeletedMessage.class, ThreadPool.Names.SAME, new NodeIndexDeletedTransportHandler());
        transportService.registerRequestHandler(INDEX_STORE_DELETED_ACTION_NAME, NodeIndexStoreDeletedMessage.class, ThreadPool.Names.SAME, new NodeIndexStoreDeletedTransportHandler());
        this.indicesService = indicesService;
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    public void nodeIndexDeleted(final ClusterState clusterState, final String index, final Settings indexSettings, final String nodeId) throws ElasticsearchException {
        final DiscoveryNodes nodes = clusterState.nodes();
        transportService.sendRequest(clusterState.nodes().masterNode(),
                INDEX_DELETED_ACTION_NAME, new NodeIndexDeletedMessage(index, nodeId), EmptyTransportResponseHandler.INSTANCE_SAME);
        if (nodes.localNode().isDataNode() == false) {
            logger.trace("[{}] not acking store deletion (not a data node)");
            return;
        }
        threadPool.generic().execute(new AbstractRunnable() {
            @Override
            public void onFailure(Throwable t) {
                logger.warn("[{}]failed to ack index store deleted for  index", t, index);
            }

            @Override
            protected void doRun() throws Exception {
                lockIndexAndAck(index, nodes, nodeId, clusterState, indexSettings);
            }
        });
    }

    private void lockIndexAndAck(String index, DiscoveryNodes nodes, String nodeId, ClusterState clusterState, Settings indexSettings) throws IOException {
        try {
            // we are waiting until we can lock the index / all shards on the node and then we ack the delete of the store to the
            // master. If we can't acquire the locks here immediately there might be a shard of this index still holding on to the lock
            // due to a "currently canceled recovery" or so. The shard will delete itself BEFORE the lock is released so it's guaranteed to be
            // deleted by the time we get the lock
            indicesService.processPendingDeletes(new Index(index), indexSettings, new TimeValue(30, TimeUnit.MINUTES));
            transportService.sendRequest(clusterState.nodes().masterNode(),
                    INDEX_STORE_DELETED_ACTION_NAME, new NodeIndexStoreDeletedMessage(index, nodeId), EmptyTransportResponseHandler.INSTANCE_SAME);
        } catch (LockObtainFailedException exc) {
            logger.warn("[{}] failed to lock all shards for index - timed out after 30 seconds", index);
        }
    }

    public interface Listener {
        void onNodeIndexDeleted(String index, String nodeId);

        void onNodeIndexStoreDeleted(String index, String nodeId);
    }

    private class NodeIndexDeletedTransportHandler implements TransportRequestHandler<NodeIndexDeletedMessage> {

        @Override
        public void messageReceived(NodeIndexDeletedMessage message, TransportChannel channel) throws Exception {
            for (Listener listener : listeners) {
                listener.onNodeIndexDeleted(message.index, message.nodeId);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private class NodeIndexStoreDeletedTransportHandler implements TransportRequestHandler<NodeIndexStoreDeletedMessage> {

        @Override
        public void messageReceived(NodeIndexStoreDeletedMessage message, TransportChannel channel) throws Exception {
            for (Listener listener : listeners) {
                listener.onNodeIndexStoreDeleted(message.index, message.nodeId);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
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