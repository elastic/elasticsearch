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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 *
 */
public class NodeMappingRefreshAction extends AbstractComponent {

    public static final String ACTION_NAME = "internal:cluster/node/mapping/refresh";

    private final TransportService transportService;
    private final MetaDataMappingService metaDataMappingService;

    @Inject
    public NodeMappingRefreshAction(Settings settings, TransportService transportService, MetaDataMappingService metaDataMappingService) {
        super(settings);
        this.transportService = transportService;
        this.metaDataMappingService = metaDataMappingService;
        transportService.registerRequestHandler(ACTION_NAME, NodeMappingRefreshRequest::new, ThreadPool.Names.SAME, new NodeMappingRefreshTransportHandler());
    }

    public void nodeMappingRefresh(final DiscoveryNode masterNode, final NodeMappingRefreshRequest request) {
        if (masterNode == null) {
            logger.warn("can't send mapping refresh for [{}], no master known.", request.index());
            return;
        }
        transportService.sendRequest(masterNode, ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
    }

    private class NodeMappingRefreshTransportHandler implements TransportRequestHandler<NodeMappingRefreshRequest> {

        @Override
        public void messageReceived(NodeMappingRefreshRequest request, TransportChannel channel) throws Exception {
            metaDataMappingService.refreshMapping(request.index(), request.indexUUID());
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    public static class NodeMappingRefreshRequest extends TransportRequest implements IndicesRequest {

        private String index;
        private String indexUUID = IndexMetaData.INDEX_UUID_NA_VALUE;
        private String nodeId;

        public NodeMappingRefreshRequest() {
        }

        public NodeMappingRefreshRequest(String index, String indexUUID, String nodeId) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.nodeId = nodeId;
        }

        @Override
        public String[] indices() {
            return new String[]{index};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }

        public String index() {
            return index;
        }

        public String indexUUID() {
            return indexUUID;
        }

        public String nodeId() {
            return nodeId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(nodeId);
            out.writeString(indexUUID);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
            nodeId = in.readString();
            indexUUID = in.readString();
        }
    }
}
