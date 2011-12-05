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

package org.elasticsearch.cluster.action.index;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.VoidTransportResponseHandler;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class NodeMappingRefreshAction extends AbstractComponent {

    private final ThreadPool threadPool;

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final MetaDataMappingService metaDataMappingService;

    @Inject public NodeMappingRefreshAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService,
                                            MetaDataMappingService metaDataMappingService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.metaDataMappingService = metaDataMappingService;
        transportService.registerHandler(NodeMappingRefreshTransportHandler.ACTION, new NodeMappingRefreshTransportHandler());
    }

    public void nodeMappingRefresh(final NodeMappingRefreshRequest request) throws ElasticSearchException {
        DiscoveryNodes nodes = clusterService.state().nodes();
        if (nodes.localNodeMaster()) {
            threadPool.cached().execute(new Runnable() {
                @Override public void run() {
                    innerMappingRefresh(request);
                }
            });
        } else {
            transportService.sendRequest(clusterService.state().nodes().masterNode(),
                    NodeMappingRefreshTransportHandler.ACTION, request, VoidTransportResponseHandler.INSTANCE_SAME);
        }
    }

    private void innerMappingRefresh(NodeMappingRefreshRequest request) {
        metaDataMappingService.refreshMapping(request.index(), request.types());
    }

    private class NodeMappingRefreshTransportHandler extends BaseTransportRequestHandler<NodeMappingRefreshRequest> {

        static final String ACTION = "cluster/nodeMappingRefresh";

        @Override public NodeMappingRefreshRequest newInstance() {
            return new NodeMappingRefreshRequest();
        }

        @Override public void messageReceived(NodeMappingRefreshRequest request, TransportChannel channel) throws Exception {
            innerMappingRefresh(request);
            channel.sendResponse(VoidStreamable.INSTANCE);
        }

        @Override public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    public static class NodeMappingRefreshRequest implements Streamable {

        private String index;

        private String[] types;

        private String nodeId;

        private NodeMappingRefreshRequest() {
        }

        public NodeMappingRefreshRequest(String index, String[] types, String nodeId) {
            this.index = index;
            this.types = types;
            this.nodeId = nodeId;
        }

        public String index() {
            return index;
        }

        public String[] types() {
            return types;
        }

        public String nodeId() {
            return nodeId;
        }

        @Override public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(index);
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeUTF(type);
            }
            out.writeUTF(nodeId);
        }

        @Override public void readFrom(StreamInput in) throws IOException {
            index = in.readUTF();
            types = new String[in.readVInt()];
            for (int i = 0; i < types.length; i++) {
                types[i] = in.readUTF();
            }
            nodeId = in.readUTF();
        }
    }
}
