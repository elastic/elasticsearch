/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class NodeIndicesStateUpdatedAction extends AbstractComponent {

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final List<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject
    public NodeIndicesStateUpdatedAction(Settings settings, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        transportService.registerHandler(NodeIndexStateUpdatedTransportHandler.ACTION, new NodeIndexStateUpdatedTransportHandler());
    }

    public void add(final Listener listener, TimeValue timeout) {
        listeners.add(listener);
        threadPool.schedule(timeout, ThreadPool.Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                boolean removed = listeners.remove(listener);
                if (removed) {
                    listener.onTimeout();
                }
            }
        });
    }

    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    public void nodeIndexStateUpdated(final ClusterState state, final NodeIndexStateUpdatedResponse response) throws ElasticSearchException {
        DiscoveryNodes nodes = state.nodes();
        if (nodes.localNodeMaster()) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    innerNodeIndexStateUpdated(response);
                }
            });
        } else {
            transportService.sendRequest(state.nodes().masterNode(),
                    NodeIndexStateUpdatedTransportHandler.ACTION, response, EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    private void innerNodeIndexStateUpdated(NodeIndexStateUpdatedResponse response) {
        for (Listener listener : listeners) {
            listener.onIndexStateUpdated(response);
        }
    }

    private class NodeIndexStateUpdatedTransportHandler extends BaseTransportRequestHandler<NodeIndexStateUpdatedResponse> {

        static final String ACTION = "cluster/nodeIndexStateUpdated";

        @Override
        public NodeIndexStateUpdatedResponse newInstance() {
            return new NodeIndexStateUpdatedResponse();
        }

        @Override
        public void messageReceived(NodeIndexStateUpdatedResponse response, TransportChannel channel) throws Exception {
            innerNodeIndexStateUpdated(response);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    public static interface Listener {
        void onIndexStateUpdated(NodeIndexStateUpdatedResponse response);

        void onTimeout();
    }

    public static class NodeIndexStateUpdatedResponse extends TransportRequest {
        private String nodeId;
        private long version;

        NodeIndexStateUpdatedResponse() {
        }

        public NodeIndexStateUpdatedResponse(String nodeId, long version) {
            this.nodeId = nodeId;
            this.version = version;
        }

        public String nodeId() {
            return nodeId;
        }

        public long version() {
            return version;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(nodeId);
            out.writeLong(version);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodeId = in.readString();
            version = in.readLong();
        }
    }
}
