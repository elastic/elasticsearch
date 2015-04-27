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

package org.elasticsearch.indices.store;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.*;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportNodesListShardStoreMetaData extends TransportNodesOperationAction<TransportNodesListShardStoreMetaData.Request, TransportNodesListShardStoreMetaData.NodesStoreFilesMetaData, TransportNodesListShardStoreMetaData.NodeRequest, TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> {

    public static final String ACTION_NAME = "internal:cluster/nodes/indices/shard/store";

    private final IndicesService indicesService;

    private final NodeEnvironment nodeEnv;

    @Inject
    public TransportNodesListShardStoreMetaData(Settings settings, ClusterName clusterName, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, NodeEnvironment nodeEnv, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, clusterName, threadPool, clusterService, transportService, actionFilters,
                Request.class, NodeRequest.class, ThreadPool.Names.GENERIC);
        this.indicesService = indicesService;
        this.nodeEnv = nodeEnv;
    }

    public ActionFuture<NodesStoreFilesMetaData> list(ShardId shardId, boolean onlyUnallocated, String[] nodesIds, @Nullable TimeValue timeout) {
        return execute(new Request(shardId, onlyUnallocated, nodesIds).timeout(timeout));
    }

    @Override
    protected NodeRequest newNodeRequest(String nodeId, Request request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodeStoreFilesMetaData newNodeResponse() {
        return new NodeStoreFilesMetaData();
    }

    @Override
    protected NodesStoreFilesMetaData newResponse(Request request, AtomicReferenceArray responses) {
        final List<NodeStoreFilesMetaData> nodeStoreFilesMetaDatas = Lists.newArrayList();
        final List<FailedNodeException> failures = Lists.newArrayList();
        for (int i = 0; i < responses.length(); i++) {
            Object resp = responses.get(i);
            if (resp instanceof NodeStoreFilesMetaData) { // will also filter out null response for unallocated ones
                nodeStoreFilesMetaDatas.add((NodeStoreFilesMetaData) resp);
            } else if (resp instanceof FailedNodeException) {
                failures.add((FailedNodeException) resp);
            } else {
                logger.warn("unknown response type [{}], expected NodeStoreFilesMetaData or FailedNodeException", resp);
            }
        }
        return new NodesStoreFilesMetaData(clusterName, nodeStoreFilesMetaDatas.toArray(new NodeStoreFilesMetaData[nodeStoreFilesMetaDatas.size()]),
                failures.toArray(new FailedNodeException[failures.size()]));
    }

    @Override
    protected NodeStoreFilesMetaData nodeOperation(NodeRequest request) throws ElasticsearchException {
        if (request.unallocated) {
            IndexService indexService = indicesService.indexService(request.shardId.index().name());
            if (indexService == null) {
                return new NodeStoreFilesMetaData(clusterService.localNode(), null);
            }
            if (!indexService.hasShard(request.shardId.id())) {
                return new NodeStoreFilesMetaData(clusterService.localNode(), null);
            }
        }
        IndexMetaData metaData = clusterService.state().metaData().index(request.shardId.index().name());
        if (metaData == null) {
            return new NodeStoreFilesMetaData(clusterService.localNode(), null);
        }
        try {
            return new NodeStoreFilesMetaData(clusterService.localNode(), listStoreMetaData(request.shardId));
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to list store metadata for shard [" + request.shardId + "]", e);
        }
    }

    private StoreFilesMetaData listStoreMetaData(ShardId shardId) throws IOException {
        logger.trace("listing store meta data for {}", shardId);
        long startTime = System.currentTimeMillis();
        boolean exists = false;
        try {
            IndexService indexService = indicesService.indexService(shardId.index().name());
            if (indexService != null) {
                IndexShard indexShard = indexService.shard(shardId.id());
                if (indexShard != null) {
                    final Store store = indexShard.store();
                    store.incRef();
                    try {
                        exists = true;
                        return new StoreFilesMetaData(true, shardId, store.getMetadataOrEmpty().asMap());
                    } finally {
                        store.decRef();
                    }
                }
            }
            // try and see if we an list unallocated
            IndexMetaData metaData = clusterService.state().metaData().index(shardId.index().name());
            if (metaData == null) {
                return new StoreFilesMetaData(false, shardId, ImmutableMap.<String, StoreFileMetaData>of());
            }
            String storeType = metaData.settings().get(IndexStoreModule.STORE_TYPE, "fs");
            if (!storeType.contains("fs")) {
                return new StoreFilesMetaData(false, shardId, ImmutableMap.<String, StoreFileMetaData>of());
            }
            final ShardPath shardPath = ShardPath.loadShardPath(logger, nodeEnv, shardId, metaData.settings());
            if (shardPath == null) {
                return new StoreFilesMetaData(false, shardId, ImmutableMap.<String, StoreFileMetaData>of());
            }
            return new StoreFilesMetaData(false, shardId, Store.readMetadataSnapshot(shardPath.resolveIndex(), logger).asMap());
        } finally {
            TimeValue took = new TimeValue(System.currentTimeMillis() - startTime);
            if (exists) {
                logger.debug("{} loaded store meta data (took [{}])", shardId, took);
            } else {
                logger.trace("{} didn't find any store meta data to load (took [{}])", shardId, took);
            }
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    public static class StoreFilesMetaData implements Iterable<StoreFileMetaData>, Streamable {
        private boolean allocated;
        private ShardId shardId;
        private Map<String, StoreFileMetaData> files;

        StoreFilesMetaData() {
        }

        public StoreFilesMetaData(boolean allocated, ShardId shardId, Map<String, StoreFileMetaData> files) {
            this.allocated = allocated;
            this.shardId = shardId;
            this.files = files;
        }

        public boolean allocated() {
            return allocated;
        }

        public ShardId shardId() {
            return this.shardId;
        }

        @Override
        public Iterator<StoreFileMetaData> iterator() {
            return files.values().iterator();
        }

        public boolean fileExists(String name) {
            return files.containsKey(name);
        }

        public StoreFileMetaData file(String name) {
            return files.get(name);
        }

        public static StoreFilesMetaData readStoreFilesMetaData(StreamInput in) throws IOException {
            StoreFilesMetaData md = new StoreFilesMetaData();
            md.readFrom(in);
            return md;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            allocated = in.readBoolean();
            shardId = ShardId.readShardId(in);
            int size = in.readVInt();
            files = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                StoreFileMetaData md = StoreFileMetaData.readStoreFileMetaData(in);
                files.put(md.name(), md);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(allocated);
            shardId.writeTo(out);
            out.writeVInt(files.size());
            for (StoreFileMetaData md : files.values()) {
                md.writeTo(out);
            }
        }
    }


    static class Request extends NodesOperationRequest<Request> {

        private ShardId shardId;

        private boolean unallocated;

        public Request() {
        }

        public Request(ShardId shardId, boolean unallocated, Set<String> nodesIds) {
            super(nodesIds.toArray(new String[nodesIds.size()]));
            this.shardId = shardId;
            this.unallocated = unallocated;
        }

        public Request(ShardId shardId, boolean unallocated, String... nodesIds) {
            super(nodesIds);
            this.shardId = shardId;
            this.unallocated = unallocated;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            unallocated = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeBoolean(unallocated);
        }
    }

    public static class NodesStoreFilesMetaData extends NodesOperationResponse<NodeStoreFilesMetaData> {

        private FailedNodeException[] failures;

        NodesStoreFilesMetaData() {
        }

        public NodesStoreFilesMetaData(ClusterName clusterName, NodeStoreFilesMetaData[] nodes, FailedNodeException[] failures) {
            super(clusterName, nodes);
            this.failures = failures;
        }

        public FailedNodeException[] failures() {
            return failures;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            nodes = new NodeStoreFilesMetaData[in.readVInt()];
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = NodeStoreFilesMetaData.readListShardStoreNodeOperationResponse(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(nodes.length);
            for (NodeStoreFilesMetaData response : nodes) {
                response.writeTo(out);
            }
        }
    }


    static class NodeRequest extends NodeOperationRequest {

        private ShardId shardId;

        private boolean unallocated;

        NodeRequest() {
        }

        NodeRequest(String nodeId, TransportNodesListShardStoreMetaData.Request request) {
            super(request, nodeId);
            this.shardId = request.shardId;
            this.unallocated = request.unallocated;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            unallocated = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeBoolean(unallocated);
        }
    }

    public static class NodeStoreFilesMetaData extends NodeOperationResponse {

        private StoreFilesMetaData storeFilesMetaData;

        NodeStoreFilesMetaData() {
        }

        public NodeStoreFilesMetaData(DiscoveryNode node, StoreFilesMetaData storeFilesMetaData) {
            super(node);
            this.storeFilesMetaData = storeFilesMetaData;
        }

        public StoreFilesMetaData storeFilesMetaData() {
            return storeFilesMetaData;
        }

        public static NodeStoreFilesMetaData readListShardStoreNodeOperationResponse(StreamInput in) throws IOException {
            NodeStoreFilesMetaData resp = new NodeStoreFilesMetaData();
            resp.readFrom(in);
            return resp;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.readBoolean()) {
                storeFilesMetaData = StoreFilesMetaData.readStoreFilesMetaData(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (storeFilesMetaData == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                storeFilesMetaData.writeTo(out);
            }
        }
    }
}
