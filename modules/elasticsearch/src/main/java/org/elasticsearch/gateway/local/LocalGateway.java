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

package org.elasticsearch.gateway.local;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.index.gateway.local.LocalIndexGatewayModule;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGateway extends AbstractLifecycleComponent<Gateway> implements Gateway, ClusterStateListener {

    private File location;

    private final ClusterService clusterService;

    private final NodeEnvironment nodeEnv;

    private final MetaDataCreateIndexService createIndexService;

    private final TransportNodesListGatewayMetaState listGatewayMetaState;

    private final TransportNodesListGatewayStartedShards listGatewayStartedShards;

    private volatile LocalGatewayMetaState currentMetaState;

    private volatile LocalGatewayStartedShards currentStartedShards;

    private volatile ExecutorService executor;

    private volatile boolean initialized = false;

    @Inject public LocalGateway(Settings settings, ClusterService clusterService, MetaDataCreateIndexService createIndexService,
                                NodeEnvironment nodeEnv, TransportNodesListGatewayMetaState listGatewayMetaState, TransportNodesListGatewayStartedShards listGatewayStartedShards) {
        super(settings);
        this.clusterService = clusterService;
        this.createIndexService = createIndexService;
        this.nodeEnv = nodeEnv;
        this.listGatewayMetaState = listGatewayMetaState.initGateway(this);
        this.listGatewayStartedShards = listGatewayStartedShards.initGateway(this);
    }

    @Override public String type() {
        return "local";
    }

    public LocalGatewayMetaState currentMetaState() {
        lazyInitialize();
        return this.currentMetaState;
    }

    public LocalGatewayStartedShards currentStartedShards() {
        lazyInitialize();
        return this.currentStartedShards;
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.executor = newSingleThreadExecutor(daemonThreadFactory(settings, "gateway"));
        lazyInitialize();
        clusterService.add(this);
    }

    @Override protected void doStop() throws ElasticSearchException {
        clusterService.remove(this);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        Set<String> nodesIds = Sets.newHashSet();
        nodesIds.addAll(clusterService.state().nodes().masterNodes().keySet());
        TransportNodesListGatewayMetaState.NodesLocalGatewayMetaState nodesState = listGatewayMetaState.list(nodesIds, null).actionGet();

        if (nodesState.failures().length > 0) {
            for (FailedNodeException failedNodeException : nodesState.failures()) {
                logger.warn("failed to fetch state from node", failedNodeException);
            }
        }

        TransportNodesListGatewayMetaState.NodeLocalGatewayMetaState electedState = null;
        for (TransportNodesListGatewayMetaState.NodeLocalGatewayMetaState nodeState : nodesState) {
            if (nodeState.state() == null) {
                continue;
            }
            if (electedState == null) {
                electedState = nodeState;
            } else if (nodeState.state().version() > electedState.state().version()) {
                electedState = nodeState;
            }
        }
        if (electedState == null) {
            logger.debug("no state elected");
            listener.onSuccess();
            return;
        }
        if (electedState.state().metaData().indices().isEmpty()) {
            logger.debug("no indices in metadata");
            listener.onSuccess();
            return;
        }

        logger.debug("elected state from [{}]", electedState.node());
        final LocalGatewayMetaState state = electedState.state();
        final AtomicInteger indicesCounter = new AtomicInteger(state.metaData().indices().size());
        clusterService.submitStateUpdateTask("local-gateway-elected-state", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                        .metaData(currentState.metaData());
                // mark the metadata as read from gateway
                metaDataBuilder.markAsRecoveredFromGateway();

                // add the index templates
                for (Map.Entry<String, IndexTemplateMetaData> entry : state.metaData().templates().entrySet()) {
                    metaDataBuilder.put(entry.getValue());
                }

                return newClusterStateBuilder().state(currentState)
                        .version(state.version())
                        .metaData(metaDataBuilder).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                // go over the meta data and create indices, we don't really need to copy over
                // the meta data per index, since we create the index and it will be added automatically
                for (final IndexMetaData indexMetaData : state.metaData()) {
                    try {
                        createIndexService.createIndex(new MetaDataCreateIndexService.Request(MetaDataCreateIndexService.Request.Origin.GATEWAY, "gateway", indexMetaData.index())
                                .settings(indexMetaData.settings())
                                .mappingsMetaData(indexMetaData.mappings())
                                .state(indexMetaData.state())
                                .timeout(timeValueSeconds(30)),

                                new MetaDataCreateIndexService.Listener() {
                                    @Override public void onResponse(MetaDataCreateIndexService.Response response) {
                                        if (indicesCounter.decrementAndGet() == 0) {
                                            listener.onSuccess();
                                        }
                                    }

                                    @Override public void onFailure(Throwable t) {
                                        logger.error("failed to create index [{}]", t, indexMetaData.index());
                                    }
                                });
                    } catch (IOException e) {
                        logger.error("failed to create index [{}]", e, indexMetaData.index());
                    }
                }
            }
        });
    }

    @Override public Class<? extends Module> suggestIndexGateway() {
        return LocalIndexGatewayModule.class;
    }

    @Override public void reset() throws Exception {
        FileSystemUtils.deleteRecursively(nodeEnv.nodeDataLocation());
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        // nothing to do until we actually recover from the gateway
        if (!event.state().metaData().recoveredFromGateway()) {
            return;
        }

        // the location is set to null, so we should not store it (for example, its not a data/master node)
        if (location == null) {
            return;
        }

        // we only write the local metadata if this is a possible master node, the metadata has changed, and
        // we don't have a NO_MASTER block (in which case, the routing is cleaned, and we don't want to override what
        // we have now, since it might be needed when later on performing full state recovery)
        if (event.state().nodes().localNode().masterNode() && event.metaDataChanged() && !event.state().blocks().hasGlobalBlock(Discovery.NO_MASTER_BLOCK)) {
            executor.execute(new Runnable() {
                @Override public void run() {
                    LocalGatewayMetaState.Builder builder = LocalGatewayMetaState.builder();
                    if (currentMetaState != null) {
                        builder.state(currentMetaState);
                    }
                    builder.version(event.state().version());
                    builder.metaData(event.state().metaData());

                    try {
                        LocalGatewayMetaState stateToWrite = builder.build();
                        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                        xContentBuilder.prettyPrint();
                        xContentBuilder.startObject();
                        LocalGatewayMetaState.Builder.toXContent(stateToWrite, xContentBuilder, ToXContent.EMPTY_PARAMS);
                        xContentBuilder.endObject();

                        File stateFile = new File(location, "metadata-" + event.state().version());
                        FileOutputStream fos = new FileOutputStream(stateFile);
                        fos.write(xContentBuilder.unsafeBytes(), 0, xContentBuilder.unsafeBytesLength());
                        fos.close();

                        FileSystemUtils.syncFile(stateFile);

                        currentMetaState = stateToWrite;

                        // delete all the other files
                        File[] files = location.listFiles(new FilenameFilter() {
                            @Override public boolean accept(File dir, String name) {
                                return name.startsWith("metadata-") && !name.equals("metadata-" + event.state().version());
                            }
                        });
                        for (File file : files) {
                            file.delete();
                        }

                    } catch (IOException e) {
                        logger.warn("failed to write updated state", e);
                    }
                }
            });
        }

        if (event.state().nodes().localNode().dataNode() && event.routingTableChanged()) {
            executor.execute(new Runnable() {
                @Override public void run() {
                    LocalGatewayStartedShards.Builder builder = LocalGatewayStartedShards.builder();
                    if (currentStartedShards != null) {
                        builder.state(currentStartedShards);
                    }
                    builder.version(event.state().version());
                    // remove from the current state all the shards that are primary and started somewhere, we won't need them anymore
                    // and if they are still here, we will add them in the next phase

                    // Also note, this works well when closing an index, since a closed index will have no routing shards entries
                    // so they won't get removed (we want to keep the fact that those shards are allocated on this node if needed)
                    for (IndexRoutingTable indexRoutingTable : event.state().routingTable()) {
                        for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                            if (indexShardRoutingTable.primaryShard().active()) {
                                builder.remove(indexShardRoutingTable.shardId());
                            }
                        }
                    }
                    // now, add all the ones that are active and on this node
                    RoutingNode routingNode = event.state().readOnlyRoutingNodes().node(event.state().nodes().localNodeId());
                    if (routingNode != null) {
                        // out node is not in play yet...
                        for (MutableShardRouting shardRouting : routingNode) {
                            if (shardRouting.active()) {
                                builder.put(shardRouting.shardId(), event.state().version());
                            }
                        }
                    }

                    try {
                        LocalGatewayStartedShards stateToWrite = builder.build();
                        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
                        xContentBuilder.prettyPrint();
                        xContentBuilder.startObject();
                        LocalGatewayStartedShards.Builder.toXContent(stateToWrite, xContentBuilder, ToXContent.EMPTY_PARAMS);
                        xContentBuilder.endObject();

                        File stateFile = new File(location, "shards-" + event.state().version());
                        FileOutputStream fos = new FileOutputStream(stateFile);
                        fos.write(xContentBuilder.unsafeBytes(), 0, xContentBuilder.unsafeBytesLength());
                        fos.close();

                        FileSystemUtils.syncFile(stateFile);

                        currentStartedShards = stateToWrite;
                    } catch (IOException e) {
                        logger.warn("failed to write updated state", e);
                        return;
                    }

                    // delete all the other files
                    File[] files = location.listFiles(new FilenameFilter() {
                        @Override public boolean accept(File dir, String name) {
                            return name.startsWith("shards-") && !name.equals("shards-" + event.state().version());
                        }
                    });
                    for (File file : files) {
                        file.delete();
                    }
                }
            });
        }
    }

    /**
     * We do here lazy initialization on not only on start(), since we might be called before start by another node (really will
     * happen in term of timing in testing, but still), and we want to return the cluster state when we can.
     *
     * It is synchronized since we want to wait for it to be loaded if called concurrently. There should really be a nicer
     * solution here, but for now, its good enough.
     */
    private synchronized void lazyInitialize() {
        if (initialized) {
            return;
        }
        initialized = true;

        // if this is not a possible master node or data node, bail, we won't save anything here...
        if (!clusterService.localNode().masterNode() || !clusterService.localNode().dataNode()) {
            location = null;
        } else {
            // create the location where the state will be stored
            this.location = new File(nodeEnv.nodeDataLocation(), "_state");
            this.location.mkdirs();

            if (clusterService.localNode().masterNode()) {
                try {
                    long version = findLatestMetaStateVersion();
                    if (version != -1) {
                        this.currentMetaState = readMetaState(Streams.copyToByteArray(new FileInputStream(new File(location, "metadata-" + version))));
                    }
                } catch (Exception e) {
                    logger.warn("failed to read local state", e);
                }
            }

            if (clusterService.localNode().dataNode()) {
                try {
                    long version = findLatestStartedShardsVersion();
                    if (version != -1) {
                        this.currentStartedShards = readStartedShards(Streams.copyToByteArray(new FileInputStream(new File(location, "shards-" + version))));
                    }
                } catch (Exception e) {
                    logger.warn("failed to read local state", e);
                }
            }
        }
    }

    private long findLatestStartedShardsVersion() throws IOException {
        long index = -1;
        for (File stateFile : location.listFiles()) {
            if (logger.isTraceEnabled()) {
                logger.trace("[findLatestState]: Processing [" + stateFile.getName() + "]");
            }
            String name = stateFile.getName();
            if (!name.startsWith("shards-")) {
                continue;
            }
            long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                // try and read the meta data
                try {
                    readStartedShards(Streams.copyToByteArray(new FileInputStream(stateFile)));
                    index = fileIndex;
                } catch (IOException e) {
                    logger.warn("[findLatestState]: Failed to read state from [" + name + "], ignoring...", e);
                }
            }
        }

        return index;
    }

    private long findLatestMetaStateVersion() throws IOException {
        long index = -1;
        for (File stateFile : location.listFiles()) {
            if (logger.isTraceEnabled()) {
                logger.trace("[findLatestState]: Processing [" + stateFile.getName() + "]");
            }
            String name = stateFile.getName();
            if (!name.startsWith("metadata-")) {
                continue;
            }
            long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                // try and read the meta data
                try {
                    readMetaState(Streams.copyToByteArray(new FileInputStream(stateFile)));
                    index = fileIndex;
                } catch (IOException e) {
                    logger.warn("[findLatestState]: Failed to read state from [" + name + "], ignoring...", e);
                }
            }
        }

        return index;
    }

    private LocalGatewayMetaState readMetaState(byte[] data) throws IOException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(XContentType.JSON).createParser(data);
            return LocalGatewayMetaState.Builder.fromXContent(parser);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private LocalGatewayStartedShards readStartedShards(byte[] data) throws IOException {
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(XContentType.JSON).createParser(data);
            return LocalGatewayStartedShards.Builder.fromXContent(parser);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
