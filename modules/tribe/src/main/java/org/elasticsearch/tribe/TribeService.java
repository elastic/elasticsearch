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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.MergableCustomMetaData;
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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TcpTransport;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * The tribe service holds a list of node clients connected to a list of tribe members, and uses their
 * cluster state events to update this local node cluster state with the merged view of it.
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
public class TribeService extends AbstractLifecycleComponent {

    public static final ClusterBlock TRIBE_METADATA_BLOCK = new ClusterBlock(10, "tribe node, metadata not allowed", false, false,
        false, RestStatus.BAD_REQUEST, EnumSet.of(ClusterBlockLevel.METADATA_READ, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock TRIBE_WRITE_BLOCK = new ClusterBlock(11, "tribe node, write not allowed", false, false,
        false, RestStatus.BAD_REQUEST, EnumSet.of(ClusterBlockLevel.WRITE));

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
            BLOCKS_METADATA_INDICES_SETTING.getKey(), BLOCKS_METADATA_SETTING.getKey(), BLOCKS_READ_INDICES_SETTING.getKey(),
            BLOCKS_WRITE_INDICES_SETTING.getKey(), BLOCKS_WRITE_SETTING.getKey());

    // these settings should be passed through to each tribe client, if they are not set explicitly
    private static final List<Setting<?>> PASS_THROUGH_SETTINGS = Arrays.asList(
        NetworkService.GLOBAL_NETWORK_HOST_SETTING,
        NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING,
        NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING,
        TcpTransport.HOST,
        TcpTransport.BIND_HOST,
        TcpTransport.PUBLISH_HOST
    );
    private final String onConflict;
    private final Set<String> droppedIndices = ConcurrentCollections.newConcurrentSet();

    private final List<Node> nodes = new CopyOnWriteArrayList<>();

    private final NamedWriteableRegistry namedWriteableRegistry;

    public TribeService(Settings settings, NodeEnvironment nodeEnvironment, ClusterService clusterService,
                        NamedWriteableRegistry namedWriteableRegistry, Function<Settings, Node> clientNodeBuilder) {
        super(settings);
        this.clusterService = clusterService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        Map<String, Settings> nodesSettings = new HashMap<>(settings.getGroups("tribe", true));
        nodesSettings.remove("blocks"); // remove prefix settings that don't indicate a client
        nodesSettings.remove("on_conflict"); // remove prefix settings that don't indicate a client
        for (Map.Entry<String, Settings> entry : nodesSettings.entrySet()) {
            Settings clientSettings = buildClientSettings(entry.getKey(), nodeEnvironment.nodeId(), settings, entry.getValue());
            try {
                nodes.add(clientNodeBuilder.apply(clientSettings));
            } catch (Exception e) {
                // calling close is safe for non started nodes, we can just iterate over all
                for (Node otherNode : nodes) {
                    try {
                        otherNode.close();
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to close node {} on failed start", otherNode), inner);
                    }
                }
                throw ExceptionsHelper.convertToRuntime(e);
            }
        }

        this.blockIndicesMetadata = BLOCKS_METADATA_INDICES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);
        this.blockIndicesRead = BLOCKS_READ_INDICES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);
        this.blockIndicesWrite = BLOCKS_WRITE_INDICES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY);
        if (!nodes.isEmpty()) {
            new DeprecationLogger(Loggers.getLogger(TribeService.class))
                .deprecated("tribe nodes are deprecated in favor of cross-cluster search and will be removed in Elasticsearch 7.0.0");
        }
        this.onConflict = ON_CONFLICT_SETTING.get(settings);
    }

    // pkg private for testing
    /**
     * Builds node settings for a tribe client node from the tribe node's global settings,
     * combined with tribe specific settings.
     */
    static Settings buildClientSettings(String tribeName, String parentNodeId, Settings globalSettings, Settings tribeSettings) {
        for (String tribeKey : tribeSettings.keySet()) {
            if (tribeKey.startsWith("path.")) {
                throw new IllegalArgumentException("Setting [" + tribeKey + "] not allowed in tribe client [" + tribeName + "]");
            }
        }
        Settings.Builder sb = Settings.builder().put(tribeSettings);
        sb.put(Node.NODE_NAME_SETTING.getKey(), Node.NODE_NAME_SETTING.get(globalSettings) + "/" + tribeName);
        sb.put(Environment.PATH_HOME_SETTING.getKey(), Environment.PATH_HOME_SETTING.get(globalSettings)); // pass through ES home dir
        if (Environment.PATH_LOGS_SETTING.exists(globalSettings)) {
            sb.put(Environment.PATH_LOGS_SETTING.getKey(), Environment.PATH_LOGS_SETTING.get(globalSettings));
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

        // node id of a tribe client node is determined by node id of parent node and tribe name
        final BytesRef seedAsString = new BytesRef(parentNodeId + "/" + tribeName);
        long nodeIdSeed = MurmurHash3.hash128(seedAsString.bytes, seedAsString.offset, seedAsString.length, 0, new MurmurHash3.Hash128()).h1;
        sb.put(NodeEnvironment.NODE_ID_SEED_SETTING.getKey(), nodeIdSeed);
        sb.put(Node.NODE_LOCAL_STORAGE_SETTING.getKey(), false);
        return sb.build();
    }

    @Override
    protected void doStart() {

    }

    public void startNodes() {
        for (Node node : nodes) {
            try {
                getClusterService(node).addListener(new TribeClusterStateListener(node));
                node.start();
            } catch (Exception e) {
                // calling close is safe for non started nodes, we can just iterate over all
                for (Node otherNode : nodes) {
                    try {
                        otherNode.close();
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to close node {} on failed start", otherNode), inner);
                    }
                }
                throw ExceptionsHelper.convertToRuntime(e);
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
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to close node {}", node), e);
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
                    "cluster event from " + tribeName,
                    event,
                    ClusterStateTaskConfig.build(Priority.NORMAL),
                    executor,
                    (source, e) -> logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to process [{}]", source), e));
        }
    }

    class TribeNodeClusterStateTaskExecutor implements ClusterStateTaskExecutor<ClusterChangedEvent> {
        private final String tribeName;

        TribeNodeClusterStateTaskExecutor(String tribeName) {
            this.tribeName = tribeName;
        }

        @Override
        public String describeTasks(List<ClusterChangedEvent> tasks) {
            return tasks.stream().map(ClusterChangedEvent::source).reduce((s1, s2) -> s1 + ", " + s2).orElse("");
        }

        @Override
        public boolean runOnlyOnMaster() {
            return false;
        }

        @Override
        public ClusterTasksResult<ClusterChangedEvent> execute(ClusterState currentState, List<ClusterChangedEvent> tasks) throws Exception {
            ClusterTasksResult.Builder<ClusterChangedEvent> builder = ClusterTasksResult.builder();
            ClusterState.Builder newState = ClusterState.builder(currentState);
            boolean clusterStateChanged = updateNodes(currentState, tasks, newState);
            clusterStateChanged |= updateIndicesAndMetaData(currentState, tasks, newState);
            builder.successes(tasks);
            return builder.build(clusterStateChanged ? newState.build() : currentState);
        }

        private boolean updateNodes(ClusterState currentState, List<ClusterChangedEvent> tasks, ClusterState.Builder newState) {
            boolean clusterStateChanged = false;
            // we only need to apply the latest cluster state update
            ClusterChangedEvent latestTask = tasks.get(tasks.size() - 1);
            ClusterState tribeState = latestTask.state();
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
                if (currentState.nodes().nodeExists(tribe) == false) {
                    // a new node, add it, but also add the tribe name to the attributes
                    Map<String, String> tribeAttr = new HashMap<>(tribe.getAttributes());
                    tribeAttr.put(TRIBE_NAME_SETTING.getKey(), tribeName);
                    DiscoveryNode discoNode = new DiscoveryNode(tribe.getName(), tribe.getId(), tribe.getEphemeralId(),
                            tribe.getHostName(), tribe.getHostAddress(), tribe.getAddress(), unmodifiableMap(tribeAttr), tribe.getRoles(),
                            tribe.getVersion());
                    clusterStateChanged = true;
                    logger.info("[{}] adding node [{}]", tribeName, discoNode);
                    nodes.remove(tribe.getId()); // remove any existing node with the same id but different ephemeral id
                    nodes.add(discoNode);
                }
            }
            if (clusterStateChanged) {
                newState.nodes(nodes);
            }
            return clusterStateChanged;
        }

        private boolean updateIndicesAndMetaData(ClusterState currentState, List<ClusterChangedEvent> tasks, ClusterState.Builder newState) {
            // we only need to apply the latest cluster state update
            ClusterChangedEvent latestTask = tasks.get(tasks.size() - 1);
            ClusterState tribeState = latestTask.state();
            boolean clusterStateChanged = false;
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
            clusterStateChanged |= updateCustoms(currentState, tasks, metaData);
            if (clusterStateChanged) {
                newState.blocks(blocks);
                newState.metaData(metaData);
                newState.routingTable(routingTable.build());
            }
            return clusterStateChanged;
        }

        private boolean updateCustoms(ClusterState currentState, List<ClusterChangedEvent> tasks, MetaData.Builder metaData) {
            boolean clusterStateChanged = false;
            Set<String> changedCustomMetaDataTypeSet = tasks.stream()
                    .map(ClusterChangedEvent::changedCustomMetaDataSet)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            final List<Node> tribeClientNodes = TribeService.this.nodes;
            Map<String, MetaData.Custom> mergedCustomMetaDataMap = mergeChangedCustomMetaData(changedCustomMetaDataTypeSet,
                    customMetaDataType -> tribeClientNodes.stream()
                            .map(TribeService::getClusterService)
                            // cluster service might not have initial state yet (as tribeClientNodes are started after main node)
                            .filter(cs -> cs.lifecycleState() == Lifecycle.State.STARTED)
                            .map(ClusterService::state)
                            .map(ClusterState::metaData)
                            .map(clusterMetaData -> ((MetaData.Custom) clusterMetaData.custom(customMetaDataType)))
                            .filter(custom1 -> custom1 != null && custom1 instanceof MergableCustomMetaData)
                            .map(custom2 -> (MergableCustomMetaData) marshal(custom2))
                            .collect(Collectors.toList())
            );
            for (String changedCustomMetaDataType : changedCustomMetaDataTypeSet) {
                MetaData.Custom mergedCustomMetaData = mergedCustomMetaDataMap.get(changedCustomMetaDataType);
                if (mergedCustomMetaData == null) {
                    // we ignore merging custom md which doesn't implement MergableCustomMetaData interface
                    if (currentState.metaData().custom(changedCustomMetaDataType) instanceof MergableCustomMetaData) {
                        // custom md has been removed
                        clusterStateChanged = true;
                        logger.info("[{}] removing custom meta data type [{}]", tribeName, changedCustomMetaDataType);
                        metaData.removeCustom(changedCustomMetaDataType);
                    }
                } else {
                    // custom md has been changed
                    clusterStateChanged = true;
                    logger.info("[{}] updating custom meta data type [{}] data [{}]", tribeName, changedCustomMetaDataType, mergedCustomMetaData);
                    metaData.putCustom(changedCustomMetaDataType, mergedCustomMetaData);
                }
            }
            return clusterStateChanged;
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

    private static ClusterService getClusterService(Node node) {
        return node.injector().getInstance(ClusterService.class);
    }

    // pkg-private for testing
    static Map<String, MetaData.Custom> mergeChangedCustomMetaData(Set<String> changedCustomMetaDataTypeSet,
                                                                   Function<String, List<MergableCustomMetaData>> customMetaDataByTribeNode) {

        Map<String, MetaData.Custom> changedCustomMetaDataMap = new HashMap<>(changedCustomMetaDataTypeSet.size());
        for (String customMetaDataType : changedCustomMetaDataTypeSet) {
            customMetaDataByTribeNode.apply(customMetaDataType).stream()
                    .reduce((mergableCustomMD, mergableCustomMD2) ->
                            ((MergableCustomMetaData) mergableCustomMD.merge((MetaData.Custom) mergableCustomMD2)))
                    .ifPresent(mergedCustomMetaData ->
                            changedCustomMetaDataMap.put(customMetaDataType, ((MetaData.Custom) mergedCustomMetaData)));
        }
        return changedCustomMetaDataMap;
    }

    /**
     * Since custom metadata can be loaded by a plugin class loader that resides in a sub-node, we need to
     * marshal this object into something the tribe node can work with
     */
    private MetaData.Custom marshal(MetaData.Custom custom)  {
        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()){
            bytesStreamOutput.writeNamedWriteable(custom);
            try(StreamInput input = bytesStreamOutput.bytes().streamInput()) {
                StreamInput namedInput = new NamedWriteableAwareStreamInput(input, namedWriteableRegistry);
                MetaData.Custom marshaled = namedInput.readNamedWriteable(MetaData.Custom.class);
                return marshaled;
            }
        } catch (IOException ex) {
            throw new IllegalStateException("cannot marshal object with type " + custom.getWriteableName() + " to tribe node");
        }
    }
}
