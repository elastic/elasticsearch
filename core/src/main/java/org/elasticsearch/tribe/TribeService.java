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

package org.elasticsearch.tribe;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportSettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static java.util.Collections.unmodifiableMap;

/**
 * The tribe service holds a list of node clients connected to a list of tribe members, and uses their
 * cluster state events to update this local node cluster state with the merged view of it.
 * <p>
 * The {@link #processSettings(org.elasticsearch.common.settings.Settings)} method should be called before
 * starting the node, so it will make sure to configure this current node properly with the relevant tribe node
 * settings.
 * <p>
 * The tribe node settings make sure the discovery used is "local", but with no master elected. This means no
 * write level master node operations will work ({@link org.elasticsearch.discovery.MasterNotDiscoveredException}
 * will be thrown), and state level metadata operations with automatically use the local flag.
 * <p>
 * The state merged from different clusters include the list of nodes, metadata, and routing table. Each node merged
 * will have in its tribe which tribe member it came from. Each index merged will have in its settings which tribe
 * member it came from. In case an index has already been merged from one cluster, and the same name index is discovered
 * in another cluster, the conflict one will be discarded. This happens because we need to have the correct index name
 * to propagate to the relevant cluster.
 */
public class TribeService extends AbstractLifecycleComponent<TribeService> {

    public static final ClusterBlock TRIBE_METADATA_BLOCK = new ClusterBlock(10, "tribe node, metadata not allowed", false, false,
            RestStatus.BAD_REQUEST, EnumSet.of(ClusterBlockLevel.METADATA_READ, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock TRIBE_WRITE_BLOCK = new ClusterBlock(11, "tribe node, write not allowed", false, false,
            RestStatus.BAD_REQUEST, EnumSet.of(ClusterBlockLevel.WRITE));

    public static Settings processSettings(Settings settings) {
        if (TRIBE_NAME_SETTING.exists(settings)) {
            // if its a node client started by this service as tribe, remove any tribe group setting
            // to avoid recursive configuration
            Settings.Builder sb = Settings.builder().put(settings);
            for (String s : settings.getAsMap().keySet()) {
                if (s.startsWith("tribe.") && !s.equals(TRIBE_NAME_SETTING.getKey())) {
                    sb.remove(s);
                }
            }
            return sb.build();
        }
        Map<String, Settings> nodesSettings = settings.getGroups("tribe", true);
        if (nodesSettings.isEmpty()) {
            return settings;
        }
        // its a tribe configured node..., force settings
        Settings.Builder sb = Settings.builder().put(settings);
        sb.put(Node.NODE_MASTER_SETTING.getKey(), false);
        sb.put(Node.NODE_DATA_SETTING.getKey(), false);
        sb.put(Node.NODE_INGEST_SETTING.getKey(), false);
        sb.put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), "local"); // a tribe node should not use zen discovery
        // nothing is going to be discovered, since no master will be elected
        sb.put(DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.getKey(), 0);
        if (sb.get("cluster.name") == null) {
            sb.put("cluster.name", "tribe_" + UUIDs.randomBase64UUID()); // make sure it won't join other tribe nodes in the same JVM
        }
        sb.put(TransportMasterNodeReadAction.FORCE_LOCAL_SETTING.getKey(), true);
        return sb.build();
    }

    // internal settings only
    public static final Setting<String> TRIBE_NAME_SETTING = Setting.simpleString("tribe.name", Property.NodeScope);
    private final ClusterService clusterService;
    private final String[] blockIndicesWrite;
    private final String[] blockIndicesRead;
    private final String[] blockIndicesMetadata;
    private static final String ON_CONFLICT_ANY = "any", ON_CONFLICT_DROP = "drop", ON_CONFLICT_PREFER = "prefer_";

    public static final Setting<String> ON_CONFLICT_SETTING = new Setting<>("tribe.on_conflict", ON_CONFLICT_ANY, (s) -> {
        switch (s) {
            case ON_CONFLICT_ANY:
            case ON_CONFLICT_DROP:
                return s;
            default:
                if (s.startsWith(ON_CONFLICT_PREFER) && s.length() > ON_CONFLICT_PREFER.length()) {
                    return s;
                }
                throw new IllegalArgumentException(
                        "Invalid value for [tribe.on_conflict] must be either [any, drop or start with prefer_] but was: [" + s + "]");
        }
    }, Property.NodeScope);

    public static final Setting<Boolean> BLOCKS_METADATA_SETTING =
        Setting.boolSetting("tribe.blocks.metadata", false, Property.NodeScope);
    public static final Setting<Boolean> BLOCKS_WRITE_SETTING =
        Setting.boolSetting("tribe.blocks.write", false, Property.NodeScope);
    public static final Setting<List<String>> BLOCKS_WRITE_INDICES_SETTING =
        Setting.listSetting("tribe.blocks.write.indices", Collections.emptyList(), Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> BLOCKS_READ_INDICES_SETTING =
        Setting.listSetting("tribe.blocks.read.indices", Collections.emptyList(), Function.identity(), Property.NodeScope);
    public static final Setting<List<String>> BLOCKS_METADATA_INDICES_SETTING =
        Setting.listSetting("tribe.blocks.metadata.indices", Collections.emptyList(), Function.identity(), Property.NodeScope);

    public static final Set<String> TRIBE_SETTING_KEYS = Sets.newHashSet(TRIBE_NAME_SETTING.getKey(), ON_CONFLICT_SETTING.getKey(),
            BLOCKS_METADATA_INDICES_SETTING.getKey(), BLOCKS_METADATA_SETTING.getKey(), BLOCKS_READ_INDICES_SETTING.getKey(), BLOCKS_WRITE_INDICES_SETTING.getKey(), BLOCKS_WRITE_SETTING.getKey());

    // these settings should be passed through to each tribe client, if they are not set explicitly
    private static final List<Setting<?>> PASS_THROUGH_SETTINGS = Arrays.asList(
        NetworkService.GLOBAL_NETWORK_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING,
        NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING,
        TransportSettings.HOST,
        TransportSettings.BIND_HOST,
        TransportSettings.PUBLISH_HOST
    );
    private final String onConflict;
    private final Set<String> droppedIndices = ConcurrentCollections.newConcurrentSet();

    private final List<Node> nodes = new CopyOnWriteArrayList<>();

    @Inject
    public TribeService(Settings settings, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        Map<String, Settings> nodesSettings = new HashMap<>(settings.getGroups("tribe", true));
        nodesSettings.remove("blocks"); // remove prefix settings that don't indicate a client
        nodesSettings.remove("on_conflict"); // remove prefix settings that don't indicate a client
        for (Map.Entry<String, Settings> entry : nodesSettings.entrySet()) {
            Settings clientSettings = buildClientSettings(entry.getKey(), settings, entry.getValue());
            nodes.add(new TribeClientNode(clientSettings));
        }

        this.blockIndicesMetadata = BLOCKS_METADATA_INDICES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);
        this.blockIndicesRead = BLOCKS_READ_INDICES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);
        this.blockIndicesWrite = BLOCKS_WRITE_INDICES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);

        if (!nodes.isEmpty()) {
            if (BLOCKS_WRITE_SETTING.get(settings)) {
                clusterService.addInitialStateBlock(TRIBE_WRITE_BLOCK);
            }
            if (BLOCKS_METADATA_SETTING.get(settings)) {
                clusterService.addInitialStateBlock(TRIBE_METADATA_BLOCK);
            }
        }

        this.onConflict = ON_CONFLICT_SETTING.get(settings);
    }

    // pkg private for testing
    /**
     * Builds node settings for a tribe client node from the tribe node's global settings,
     * combined with tribe specific settings.
     */
    static Settings buildClientSettings(String tribeName, Settings globalSettings, Settings tribeSettings) {
        for (String tribeKey : tribeSettings.getAsMap().keySet()) {
            if (tribeKey.startsWith("path.")) {
                throw new IllegalArgumentException("Setting [" + tribeKey + "] not allowed in tribe client [" + tribeName + "]");
            }
        }
        Settings.Builder sb = Settings.builder().put(tribeSettings);
        sb.put("node.name", globalSettings.get("node.name") + "/" + tribeName);
        sb.put(Environment.PATH_HOME_SETTING.getKey(), Environment.PATH_HOME_SETTING.get(globalSettings)); // pass through ES home dir
        if (Environment.PATH_CONF_SETTING.exists(globalSettings)) {
            sb.put(Environment.PATH_CONF_SETTING.getKey(), Environment.PATH_CONF_SETTING.get(globalSettings));
        }
        if (Environment.PATH_LOGS_SETTING.exists(globalSettings)) {
            sb.put(Environment.PATH_LOGS_SETTING.getKey(), Environment.PATH_LOGS_SETTING.get(globalSettings));
        }
        if (Environment.PATH_SCRIPTS_SETTING.exists(globalSettings)) {
            sb.put(Environment.PATH_SCRIPTS_SETTING.getKey(), Environment.PATH_SCRIPTS_SETTING.get(globalSettings));
        }
        for (Setting<?> passthrough : PASS_THROUGH_SETTINGS) {
            if (passthrough.exists(tribeSettings) == false && passthrough.exists(globalSettings)) {
                sb.put(passthrough.getKey(), globalSettings.get(passthrough.getKey()));
            }
        }
        sb.put(TRIBE_NAME_SETTING.getKey(), tribeName);
        if (sb.get(NetworkModule.HTTP_ENABLED.getKey()) == null) {
            sb.put(NetworkModule.HTTP_ENABLED.getKey(), false);
        }
        sb.put(Node.NODE_DATA_SETTING.getKey(), false);
        sb.put(Node.NODE_MASTER_SETTING.getKey(), false);
        sb.put(Node.NODE_INGEST_SETTING.getKey(), false);
        return sb.build();
    }


    @Override
    protected void doStart() {
        if (nodes.isEmpty() == false) {
            // remove the initial election / recovery blocks since we are not going to have a
            // master elected in this single tribe  node local "cluster"
            clusterService.removeInitialStateBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            clusterService.removeInitialStateBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
        }
    }

    public void startNodes() {
        for (Node node : nodes) {
            try {
                node.injector().getInstance(ClusterService.class).add(new TribeClusterStateListener(node));
                node.start();
            } catch (Throwable e) {
                // calling close is safe for non started nodes, we can just iterate over all
                for (Node otherNode : nodes) {
                    try {
                        otherNode.close();
                    } catch (Throwable t) {
                        logger.warn("failed to close node {} on failed start", t, otherNode);
                    }
                }
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new ElasticsearchException(e);
            }
        }
    }

    @Override
    protected void doStop() {
        doClose();
    }

    @Override
    protected void doClose() {
        for (Node node : nodes) {
            try {
                node.close();
            } catch (Throwable t) {
                logger.warn("failed to close node {}", t, node);
            }
        }
    }


    class TribeClusterStateListener implements ClusterStateListener {
        private final String tribeName;
        private final TribeNodeClusterStateTaskExecutor executor;

        TribeClusterStateListener(Node tribeNode) {
            String tribeName = TRIBE_NAME_SETTING.get(tribeNode.settings());
            this.tribeName = tribeName;
            executor = new TribeNodeClusterStateTaskExecutor(tribeName);
        }

        @Override
        public void clusterChanged(final ClusterChangedEvent event) {
            logger.debug("[{}] received cluster event, [{}]", tribeName, event.source());
            clusterService.submitStateUpdateTask(
                    "cluster event from " + tribeName + ", " + event.source(),
                    event,
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    executor,
                    (source, t) -> logger.warn("failed to process [{}]", t, source));
        }
    }

    class TribeNodeClusterStateTaskExecutor implements ClusterStateTaskExecutor<ClusterChangedEvent> {
        private final String tribeName;

        TribeNodeClusterStateTaskExecutor(String tribeName) {
            this.tribeName = tribeName;
        }


        @Override
        public boolean runOnlyOnMaster() {
            return false;
        }

        @Override
        public BatchResult<ClusterChangedEvent> execute(ClusterState currentState, List<ClusterChangedEvent> tasks) throws Exception {
            ClusterState accumulator = ClusterState.builder(currentState).build();
            BatchResult.Builder<ClusterChangedEvent> builder = BatchResult.builder();

            try {
                // we only need to apply the latest cluster state update
                accumulator = applyUpdate(accumulator, tasks.get(tasks.size() - 1));
                builder.successes(tasks);
            } catch (Throwable t) {
                builder.failures(tasks, t);
            }

            return builder.build(accumulator);
        }

        private ClusterState applyUpdate(ClusterState currentState, ClusterChangedEvent task) {
            boolean clusterStateChanged = false;
            ClusterState tribeState = task.state();
            DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(currentState.nodes());
            // -- merge nodes
            // go over existing nodes, and see if they need to be removed
            for (DiscoveryNode discoNode : currentState.nodes()) {
                String markedTribeName = discoNode.getAttributes().get(TRIBE_NAME_SETTING.getKey());
                if (markedTribeName != null && markedTribeName.equals(tribeName)) {
                    if (tribeState.nodes().get(discoNode.getId()) == null) {
                        clusterStateChanged = true;
                        logger.info("[{}] removing node [{}]", tribeName, discoNode);
                        nodes.remove(discoNode.getId());
                    }
                }
            }
            // go over tribe nodes, and see if they need to be added
            for (DiscoveryNode tribe : tribeState.nodes()) {
                if (currentState.nodes().get(tribe.getId()) == null) {
                    // a new node, add it, but also add the tribe name to the attributes
                    Map<String, String> tribeAttr = new HashMap<>(tribe.getAttributes());
                    tribeAttr.put(TRIBE_NAME_SETTING.getKey(), tribeName);
                    DiscoveryNode discoNode = new DiscoveryNode(tribe.getName(), tribe.getId(), tribe.getHostName(), tribe.getHostAddress(),
                            tribe.getAddress(), unmodifiableMap(tribeAttr), tribe.getRoles(), tribe.getVersion());
                    clusterStateChanged = true;
                    logger.info("[{}] adding node [{}]", tribeName, discoNode);
                    nodes.put(discoNode);
                }
            }

            // -- merge metadata
            ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
            MetaData.Builder metaData = MetaData.builder(currentState.metaData());
            RoutingTable.Builder routingTable = RoutingTable.builder(currentState.routingTable());
            // go over existing indices, and see if they need to be removed
            for (IndexMetaData index : currentState.metaData()) {
                String markedTribeName = TRIBE_NAME_SETTING.get(index.getSettings());
                if (markedTribeName != null && markedTribeName.equals(tribeName)) {
                    IndexMetaData tribeIndex = tribeState.metaData().index(index.getIndex());
                    clusterStateChanged = true;
                    if (tribeIndex == null || tribeIndex.getState() == IndexMetaData.State.CLOSE) {
                        logger.info("[{}] removing index {}", tribeName, index.getIndex());
                        removeIndex(blocks, metaData, routingTable, index);
                    } else {
                        // always make sure to update the metadata and routing table, in case
                        // there are changes in them (new mapping, shards moving from initializing to started)
                        routingTable.add(tribeState.routingTable().index(index.getIndex()));
                        Settings tribeSettings = Settings.builder().put(tribeIndex.getSettings())
                                .put(TRIBE_NAME_SETTING.getKey(), tribeName).build();
                        metaData.put(IndexMetaData.builder(tribeIndex).settings(tribeSettings));
                    }
                }
            }
            // go over tribe one, and see if they need to be added
            for (IndexMetaData tribeIndex : tribeState.metaData()) {
                // if there is no routing table yet, do nothing with it...
                IndexRoutingTable table = tribeState.routingTable().index(tribeIndex.getIndex());
                if (table == null) {
                    continue;
                }
                //NOTE: we have to use the index name here since UUID are different even if the name is the same
                final String indexName = tribeIndex.getIndex().getName();
                final IndexMetaData indexMetaData = currentState.metaData().index(indexName);
                if (indexMetaData == null) {
                    if (!droppedIndices.contains(indexName)) {
                        // a new index, add it, and add the tribe name as a setting
                        clusterStateChanged = true;
                        logger.info("[{}] adding index {}", tribeName, tribeIndex.getIndex());
                        addNewIndex(tribeState, blocks, metaData, routingTable, tribeIndex);
                    }
                } else {
                    String existingFromTribe = TRIBE_NAME_SETTING.get(indexMetaData.getSettings());
                    if (!tribeName.equals(existingFromTribe)) {
                        // we have a potential conflict on index names, decide what to do...
                        if (ON_CONFLICT_ANY.equals(onConflict)) {
                            // we chose any tribe, carry on
                        } else if (ON_CONFLICT_DROP.equals(onConflict)) {
                            // drop the indices, there is a conflict
                            clusterStateChanged = true;
                            logger.info("[{}] dropping index {} due to conflict with [{}]", tribeName, tribeIndex.getIndex(),
                                    existingFromTribe);
                            removeIndex(blocks, metaData, routingTable, tribeIndex);
                            droppedIndices.add(indexName);
                        } else if (onConflict.startsWith(ON_CONFLICT_PREFER)) {
                            // on conflict, prefer a tribe...
                            String preferredTribeName = onConflict.substring(ON_CONFLICT_PREFER.length());
                            if (tribeName.equals(preferredTribeName)) {
                                // the new one is hte preferred one, replace...
                                clusterStateChanged = true;
                                logger.info("[{}] adding index {}, preferred over [{}]", tribeName, tribeIndex.getIndex(),
                                        existingFromTribe);
                                removeIndex(blocks, metaData, routingTable, tribeIndex);
                                addNewIndex(tribeState, blocks, metaData, routingTable, tribeIndex);
                            } // else: either the existing one is the preferred one, or we haven't seen one, carry on
                        }
                    }
                }
            }

            if (!clusterStateChanged) {
                return currentState;
            } else {
                return ClusterState.builder(currentState).incrementVersion().blocks(blocks).nodes(nodes).metaData(metaData)
                        .routingTable(routingTable.build()).build();
            }
        }

        private void removeIndex(ClusterBlocks.Builder blocks, MetaData.Builder metaData, RoutingTable.Builder routingTable,
                                 IndexMetaData index) {
            metaData.remove(index.getIndex().getName());
            routingTable.remove(index.getIndex().getName());
            blocks.removeIndexBlocks(index.getIndex().getName());
        }

        private void addNewIndex(ClusterState tribeState, ClusterBlocks.Builder blocks, MetaData.Builder metaData,
                                 RoutingTable.Builder routingTable, IndexMetaData tribeIndex) {
            Settings tribeSettings = Settings.builder().put(tribeIndex.getSettings()).put(TRIBE_NAME_SETTING.getKey(), tribeName).build();
            metaData.put(IndexMetaData.builder(tribeIndex).settings(tribeSettings));
            routingTable.add(tribeState.routingTable().index(tribeIndex.getIndex()));
            if (Regex.simpleMatch(blockIndicesMetadata, tribeIndex.getIndex().getName())) {
                blocks.addIndexBlock(tribeIndex.getIndex().getName(), IndexMetaData.INDEX_METADATA_BLOCK);
            }
            if (Regex.simpleMatch(blockIndicesRead, tribeIndex.getIndex().getName())) {
                blocks.addIndexBlock(tribeIndex.getIndex().getName(), IndexMetaData.INDEX_READ_BLOCK);
            }
            if (Regex.simpleMatch(blockIndicesWrite, tribeIndex.getIndex().getName())) {
                blocks.addIndexBlock(tribeIndex.getIndex().getName(), IndexMetaData.INDEX_WRITE_BLOCK);
            }
        }
    }
}
