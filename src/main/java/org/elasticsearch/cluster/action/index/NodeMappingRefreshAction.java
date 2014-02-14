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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

/**
 *
 */
public class NodeMappingRefreshAction extends AbstractComponent {

    private final TransportService transportService;
    private final MetaDataMappingService metaDataMappingService;

    @Inject
    public NodeMappingRefreshAction(Settings settings, TransportService transportService, MetaDataMappingService metaDataMappingService) {
        super(settings);
        this.transportService = transportService;
        this.metaDataMappingService = metaDataMappingService;
        transportService.registerHandler(NodeMappingRefreshTransportHandler.ACTION, new NodeMappingRefreshTransportHandler());
    }

    public void nodeMappingRefresh(final ClusterState state, final NodeMappingRefreshRequest request) throws ElasticsearchException {
        DiscoveryNodes nodes = state.nodes();
        if (nodes.localNodeMaster()) {
            innerMappingRefresh(request);
        } else {
            transportService.sendRequest(state.nodes().masterNode(),
                    NodeMappingRefreshTransportHandler.ACTION, request, EmptyTransportResponseHandler.INSTANCE_SAME);
        }
    }

    private void innerMappingRefresh(NodeMappingRefreshRequest request) {
        metaDataMappingService.refreshMapping(request.index(), request.indexUUID(), request.types());
    }

    private class NodeMappingRefreshTransportHandler extends BaseTransportRequestHandler<NodeMappingRefreshRequest> {

        static final String ACTION = "cluster/nodeMappingRefresh";

        @Override
        public NodeMappingRefreshRequest newInstance() {
            return new NodeMappingRefreshRequest();
        }

        @Override
        public void messageReceived(NodeMappingRefreshRequest request, TransportChannel channel) throws Exception {
            innerMappingRefresh(request);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    public static class NodeMappingRefreshRequest extends TransportRequest {

        private String index;
        private String indexUUID = IndexMetaData.INDEX_UUID_NA_VALUE;
        private String[] types;
        private String nodeId;

        NodeMappingRefreshRequest() {
        }

        public NodeMappingRefreshRequest(String index, String indexUUID, String[] types, String nodeId) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.types = types;
            this.nodeId = nodeId;
        }

        public String index() {
            return index;
        }

        public String indexUUID() {
            return indexUUID;
        }


        public String[] types() {
            return types;
        }

        public String nodeId() {
            return nodeId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeStringArray(types);
            out.writeString(nodeId);
            out.writeString(indexUUID);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
            types = in.readStringArray();
            nodeId = in.readString();
            indexUUID = in.readString();
        }
    }
}
