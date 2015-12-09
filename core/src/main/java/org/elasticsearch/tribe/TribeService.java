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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.rest.RestStatus;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

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

    public static final ClusterBlock TRIBE_METADATA_BLOCK = new ClusterBlock(10, "tribe node, metadata not allowed", false, false, RestStatus.BAD_REQUEST, EnumSet.of(ClusterBlockLevel.METADATA_READ, ClusterBlockLevel.METADATA_WRITE));
    public static final ClusterBlock TRIBE_WRITE_BLOCK = new ClusterBlock(11, "tribe node, write not allowed", false, false, RestStatus.BAD_REQUEST, EnumSet.of(ClusterBlockLevel.WRITE));

    public static Settings processSettings(Settings settings) {
        if (settings.get(TRIBE_NAME) != null) {
            // if its a node client started by this service as tribe, remove any tribe group setting
            // to avoid recursive configuration
            Settings.Builder sb = Settings.builder().put(settings);
            for (String s : settings.getAsMap().keySet()) {
                if (s.startsWith("tribe.") && !s.equals(TRIBE_NAME)) {
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
        sb.put("node.client", true); // this node should just act as a node client
        sb.put("discovery.type", "local"); // a tribe node should not use zen discovery
        sb.put("discovery.initial_state_timeout", 0); // nothing is going to be discovered, since no master will be elected
        if (sb.get("cluster.name") == null) {
            sb.put("cluster.name", "tribe_" + Strings.randomBase64UUID()); // make sure it won't join other tribe nodes in the same JVM
        }
        sb.put(TransportMasterNodeReadAction.FORCE_LOCAL_SETTING, true);
        return sb.build();
    }

    public static final String TRIBE_NAME = "tribe.name";

    private final ClusterService clusterService;
    private final String[] blockIndicesWrite;
    private final String[] blockIndicesRead;
    private final String[] blockIndicesMetadata;

    private static final String ON_CONFLICT_ANY = "any", ON_CONFLICT_DROP = "drop", ON_CONFLICT_PREFER = "prefer_";
    private final String onConflict;
    private final Set<String> droppedIndices = ConcurrentCollections.newConcurrentSet();

    private final List<Node> nodes = new CopyOnWriteArrayList<>();

    @Inject
    public TribeService(Settings settings, ClusterService clusterService, DiscoveryService discoveryService) {
        super(settings);
        this.clusterService = clusterService;
        Map<String, Settings> nodesSettings = new HashMap<>(settings.getGroups("tribe", true));
        nodesSettings.remove("blocks"); // remove prefix settings that don't indicate a client
        nodesSettings.remove("on_conflict"); // remove prefix settings that don't indicate a client
        for (Map.Entry<String, Settings> entry : nodesSettings.entrySet()) {
            Settings.Builder sb = Settings.builder().put(entry.getValue());
            sb.put("name", settings.get("name") + "/" + entry.getKey());
            sb.put("path.home", settings.get("path.home")); // pass through ES home dir
            sb.put(TRIBE_NAME, entry.getKey());
            if (sb.get("http.enabled") == null) {
                sb.put("http.enabled", false);
            }
            sb.put("node.client", true);
            nodes.add(new TribeClientNode(sb.build()));
        }

        String[] blockIndicesWrite = Strings.EMPTY_ARRAY;
        String[] blockIndicesRead = Strings.EMPTY_ARRAY;
        String[] blockIndicesMetadata = Strings.EMPTY_ARRAY;
        if (!nodes.isEmpty()) {
            // remove the initial election / recovery blocks since we are not going to have a
            // master elected in this single tribe  node local "cluster"
            clusterService.removeInitialStateBlock(discoveryService.getNoMasterBlock());
            clusterService.removeInitialStateBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
            if (settings.getAsBoolean("tribe.blocks.write", false)) {
                clusterService.addInitialStateBlock(TRIBE_WRITE_BLOCK);
            }
            blockIndicesWrite = settings.getAsArray("tribe.blocks.write.indices", Strings.EMPTY_ARRAY);
            if (settings.getAsBoolean("tribe.blocks.metadata", false)) {
                clusterService.addInitialStateBlock(TRIBE_METADATA_BLOCK);
            }
            blockIndicesMetadata = settings.getAsArray("tribe.blocks.metadata.indices", Strings.EMPTY_ARRAY);
            blockIndicesRead = settings.getAsArray("tribe.blocks.read.indices", Strings.EMPTY_ARRAY);
            for (Node node : nodes) {
                node.injector().getInstance(ClusterService.class).add(new TribeClusterStateListener(node));
            }
        }
        this.blockIndicesMetadata = blockIndicesMetadata;
        this.blockIndicesRead = blockIndicesRead;
        this.blockIndicesWrite = blockIndicesWrite;

        this.onConflict = settings.get("tribe.on_conflict", ON_CONFLICT_ANY);
    }

    @Override
    protected void doStart() {
        for (Node node : nodes) {
            try {
                node.start();
            } catch (Throwable e) {
                // calling close is safe for non started nodes, we can just iterate over all
                for (Node otherNode : nodes) {
                    try {
                        otherNode.close();
                    } catch (Throwable t) {
                        logger.warn("failed to close node {} on failed start", otherNode, t);
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

        TribeClusterStateListener(Node tribeNode) {
            this.tribeName = tribeNode.settings().get(TRIBE_NAME);
        }

        @Override
        public void clusterChanged(final ClusterChangedEvent event) {
            logger.debug("[{}] received cluster event, [{}]", tribeName, event.source());
            clusterService.submitStateUpdateTask("cluster event from " + tribeName + ", " + event.source(), new ClusterStateUpdateTask() {
                @Override
                public boolean runOnlyOnMaster() {
                    return false;
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState tribeState = event.state();
                    DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(currentState.nodes());
                    // -- merge nodes
                    // go over existing nodes, and see if they need to be removed
                    for (DiscoveryNode discoNode : currentState.nodes()) {
                        String markedTribeName = discoNode.attributes().get(TRIBE_NAME);
                        if (markedTribeName != null && markedTribeName.equals(tribeName)) {
                            if (tribeState.nodes().get(discoNode.id()) == null) {
                                logger.info("[{}] removing node [{}]", tribeName, discoNode);
                                nodes.remove(discoNode.id());
                            }
                        }
                    }
                    // go over tribe nodes, and see if they need to be added
                    for (DiscoveryNode tribe : tribeState.nodes()) {
                        if (currentState.nodes().get(tribe.id()) == null) {
                            // a new node, add it, but also add the tribe name to the attributes
                            Map<String, String> tribeAttr = new HashMap<>();
                            for (ObjectObjectCursor<String, String> attr : tribe.attributes()) {
                                tribeAttr.put(attr.key, attr.value);
                            }
                            tribeAttr.put(TRIBE_NAME, tribeName);
                            DiscoveryNode discoNode = new DiscoveryNode(tribe.name(), tribe.id(), tribe.getHostName(), tribe.getHostAddress(), tribe.address(), unmodifiableMap(tribeAttr), tribe.version());
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
                        String markedTribeName = index.getSettings().get(TRIBE_NAME);
                        if (markedTribeName != null && markedTribeName.equals(tribeName)) {
                            IndexMetaData tribeIndex = tribeState.metaData().index(index.getIndex());
                            if (tribeIndex == null || tribeIndex.getState() == IndexMetaData.State.CLOSE) {
                                logger.info("[{}] removing index [{}]", tribeName, index.getIndex());
                                removeIndex(blocks, metaData, routingTable, index);
                            } else {
                                // always make sure to update the metadata and routing table, in case
                                // there are changes in them (new mapping, shards moving from initializing to started)
                                routingTable.add(tribeState.routingTable().index(index.getIndex()));
                                Settings tribeSettings = Settings.builder().put(tribeIndex.getSettings()).put(TRIBE_NAME, tribeName).build();
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
                        final IndexMetaData indexMetaData = currentState.metaData().index(tribeIndex.getIndex());
                        if (indexMetaData == null) {
                            if (!droppedIndices.contains(tribeIndex.getIndex())) {
                                // a new index, add it, and add the tribe name as a setting
                                logger.info("[{}] adding index [{}]", tribeName, tribeIndex.getIndex());
                                addNewIndex(tribeState, blocks, metaData, routingTable, tribeIndex);
                            }
                        } else {
                            String existingFromTribe = indexMetaData.getSettings().get(TRIBE_NAME);
                            if (!tribeName.equals(existingFromTribe)) {
                                // we have a potential conflict on index names, decide what to do...
                                if (ON_CONFLICT_ANY.equals(onConflict)) {
                                    // we chose any tribe, carry on
                                } else if (ON_CONFLICT_DROP.equals(onConflict)) {
                                    // drop the indices, there is a conflict
                                    logger.info("[{}] dropping index [{}] due to conflict with [{}]", tribeName, tribeIndex.getIndex(), existingFromTribe);
                                    removeIndex(blocks, metaData, routingTable, tribeIndex);
                                    droppedIndices.add(tribeIndex.getIndex());
                                } else if (onConflict.startsWith(ON_CONFLICT_PREFER)) {
                                    // on conflict, prefer a tribe...
                                    String preferredTribeName = onConflict.substring(ON_CONFLICT_PREFER.length());
                                    if (tribeName.equals(preferredTribeName)) {
                                        // the new one is hte preferred one, replace...
                                        logger.info("[{}] adding index [{}], preferred over [{}]", tribeName, tribeIndex.getIndex(), existingFromTribe);
                                        removeIndex(blocks, metaData, routingTable, tribeIndex);
                                        addNewIndex(tribeState, blocks, metaData, routingTable, tribeIndex);
                                    } // else: either the existing one is the preferred one, or we haven't seen one, carry on
                                }
                            }
                        }
                    }

                    return ClusterState.builder(currentState).incrementVersion().blocks(blocks).nodes(nodes).metaData(metaData).routingTable(routingTable.build()).build();
                }

                private void removeIndex(ClusterBlocks.Builder blocks, MetaData.Builder metaData, RoutingTable.Builder routingTable, IndexMetaData index) {
                    metaData.remove(index.getIndex());
                    routingTable.remove(index.getIndex());
                    blocks.removeIndexBlocks(index.getIndex());
                }

                private void addNewIndex(ClusterState tribeState, ClusterBlocks.Builder blocks, MetaData.Builder metaData, RoutingTable.Builder routingTable, IndexMetaData tribeIndex) {
                    Settings tribeSettings = Settings.builder().put(tribeIndex.getSettings()).put(TRIBE_NAME, tribeName).build();
                    metaData.put(IndexMetaData.builder(tribeIndex).settings(tribeSettings));
                    routingTable.add(tribeState.routingTable().index(tribeIndex.getIndex()));
                    if (Regex.simpleMatch(blockIndicesMetadata, tribeIndex.getIndex())) {
                        blocks.addIndexBlock(tribeIndex.getIndex(), IndexMetaData.INDEX_METADATA_BLOCK);
                    }
                    if (Regex.simpleMatch(blockIndicesRead, tribeIndex.getIndex())) {
                        blocks.addIndexBlock(tribeIndex.getIndex(), IndexMetaData.INDEX_READ_BLOCK);
                    }
                    if (Regex.simpleMatch(blockIndicesWrite, tribeIndex.getIndex())) {
                        blocks.addIndexBlock(tribeIndex.getIndex(), IndexMetaData.INDEX_WRITE_BLOCK);
                    }
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.warn("failed to process [{}]", t, source);
                }
            });
        }
    }
}
