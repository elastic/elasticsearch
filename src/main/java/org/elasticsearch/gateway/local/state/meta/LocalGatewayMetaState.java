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

package org.elasticsearch.gateway.local.state.meta;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

/**
 *
 */
public class LocalGatewayMetaState extends AbstractComponent implements ClusterStateListener {

    static final String GLOBAL_STATE_FILE_PREFIX = "global-";
    private static final String INDEX_STATE_FILE_PREFIX = "state-";
    private static final String GLOBAL_STATE_LOG_TYPE = "[_global]";

    static enum AutoImportDangledState {
        NO() {
            @Override
            public boolean shouldImport() {
                return false;
            }
        },
        YES() {
            @Override
            public boolean shouldImport() {
                return true;
            }
        },
        CLOSED() {
            @Override
            public boolean shouldImport() {
                return true;
            }
        };

        public abstract boolean shouldImport();

        public static AutoImportDangledState fromString(String value) {
            if ("no".equalsIgnoreCase(value)) {
                return NO;
            } else if ("yes".equalsIgnoreCase(value)) {
                return YES;
            } else if ("closed".equalsIgnoreCase(value)) {
                return CLOSED;
            } else {
                throw new ElasticsearchIllegalArgumentException("failed to parse [" + value + "], not a valid auto dangling import type");
            }
        }
    }

    private final NodeEnvironment nodeEnv;
    private final ThreadPool threadPool;

    private final LocalAllocateDangledIndices allocateDangledIndices;

    @Nullable
    private volatile MetaData previousMetaData;

    private volatile ImmutableSet<String> previouslyWrittenIndices = ImmutableSet.of();

    private final XContentType format;
    private final ToXContent.Params formatParams;
    private final ToXContent.Params gatewayModeFormatParams;


    private final AutoImportDangledState autoImportDangled;
    private final TimeValue danglingTimeout;
    private final TimeValue deleteTimeout;
    private final Map<String, DanglingIndex> danglingIndices = ConcurrentCollections.newConcurrentMap();
    private final Object danglingMutex = new Object();
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final MetaDataStateFormat<IndexMetaData> indexStateFormat;
    private final MetaDataStateFormat<MetaData> globalStateFormat;

    @Inject
    public LocalGatewayMetaState(Settings settings, ThreadPool threadPool, NodeEnvironment nodeEnv,
                                 TransportNodesListGatewayMetaState nodesListGatewayMetaState, LocalAllocateDangledIndices allocateDangledIndices,
                                 IndicesService indicesService, ClusterService clusterService) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.threadPool = threadPool;
        this.format = XContentType.fromRestContentType(settings.get("format", "smile"));
        this.allocateDangledIndices = allocateDangledIndices;
        nodesListGatewayMetaState.init(this);

        if (this.format == XContentType.SMILE) {
            Map<String, String> params = Maps.newHashMap();
            params.put("binary", "true");
            formatParams = new ToXContent.MapParams(params);
            Map<String, String> gatewayModeParams = Maps.newHashMap();
            gatewayModeParams.put("binary", "true");
            gatewayModeParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
            gatewayModeFormatParams = new ToXContent.MapParams(gatewayModeParams);
        } else {
            formatParams = ToXContent.EMPTY_PARAMS;
            Map<String, String> gatewayModeParams = Maps.newHashMap();
            gatewayModeParams.put(MetaData.CONTEXT_MODE_PARAM, MetaData.CONTEXT_MODE_GATEWAY);
            gatewayModeFormatParams = new ToXContent.MapParams(gatewayModeParams);
        }

        this.autoImportDangled = AutoImportDangledState.fromString(settings.get("gateway.local.auto_import_dangled", AutoImportDangledState.YES.toString()));
        this.danglingTimeout = settings.getAsTime("gateway.local.dangling_timeout", TimeValue.timeValueHours(2));
        this.deleteTimeout = settings.getAsTime("gateway.local.delete_timeout", TimeValue.timeValueSeconds(30));

        logger.debug("using gateway.local.auto_import_dangled [{}], gateway.local.delete_timeout [{}], with gateway.local.dangling_timeout [{}]",
                this.autoImportDangled, this.deleteTimeout, this.danglingTimeout);
        indexStateFormat = indexStateFormat(format, formatParams);
        globalStateFormat = globalStateFormat(format, gatewayModeFormatParams);
        if (DiscoveryNode.masterNode(settings) || DiscoveryNode.dataNode(settings)) {
            nodeEnv.ensureAtomicMoveSupported();
        }
        if (DiscoveryNode.masterNode(settings) || DiscoveryNode.dataNode(settings)) {
            try {
                pre019Upgrade();
                long start = System.currentTimeMillis();
                loadState();
                logger.debug("took {} to load state", TimeValue.timeValueMillis(System.currentTimeMillis() - start));
            } catch (Exception e) {
                logger.error("failed to read local state, exiting...", e);
                throw e;
            }
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    public MetaData loadMetaState() throws Exception {
        return loadState();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        Set<String> relevantIndices = new HashSet<>();
        final ClusterState state = event.state();
        if (state.blocks().disableStatePersistence()) {
            // reset the current metadata, we need to start fresh...
            this.previousMetaData = null;
            previouslyWrittenIndices = ImmutableSet.of();
            return;
        }

        MetaData newMetaData = state.metaData();
        // we don't check if metaData changed, since we might be called several times and we need to check dangling...

        boolean success = true;
        // write the state if this node is a master eligible node or if it is a data node and has shards allocated on it
        if (state.nodes().localNode().masterNode() || state.nodes().localNode().dataNode()) {
            // check if the global state changed?
            if (previousMetaData == null || !MetaData.isGlobalStateEquals(previousMetaData, newMetaData)) {
                try {
                    writeGlobalState("changed", newMetaData);
                    // we determine if or if not we write meta data on data only nodes by looking at the shard routing
                    // and only write if a shard of this index is allocated on this node
                    // however, closed indices do not appear in the shard routing. if the meta data for a closed index is
                    // updated it will therefore not be written in case the list of previouslyWrittenIndices is empty (because state
                    // persistence was disabled or the node was restarted), see getRelevantIndicesOnDataOnlyNode().
                    // we therefore have to check here if we have shards on disk and add their indices to the previouslyWrittenIndices list
                    if (isDataOnlyNode(state)) {
                        ImmutableSet.Builder<String> previouslyWrittenIndicesBuilder = ImmutableSet.builder();
                        for (IndexMetaData indexMetaData : newMetaData) {
                            IndexMetaData indexMetaDataOnDisk = null;
                            if (indexMetaData.state().equals(IndexMetaData.State.CLOSE)) {
                                indexMetaDataOnDisk = loadIndexState(indexMetaData.index());

                            }
                            if (indexMetaDataOnDisk != null) {
                                previouslyWrittenIndicesBuilder.add(indexMetaDataOnDisk.index());
                            }
                        }
                        previouslyWrittenIndices = previouslyWrittenIndicesBuilder.addAll(previouslyWrittenIndices).build();
                    }
                } catch (Throwable e) {
                    success = false;
                }
            }


            Iterable<IndexMetaWriteInfo> writeInfo;
            relevantIndices = getRelevantIndices(event.state(), previouslyWrittenIndices);
            writeInfo = resolveStatesToBeWritten(previouslyWrittenIndices, relevantIndices, previousMetaData, event.state().metaData());
            for (IndexMetaWriteInfo indexMetaWrite : writeInfo) {
                try {
                    writeIndex(indexMetaWrite.reason, indexMetaWrite.newMetaData, indexMetaWrite.previousMetaData);
                } catch (Throwable e) {
                    success = false;
                }
            }
        }

        // handle dangling indices, we handle those for all nodes that have a node file (data or master)
        if (nodeEnv.hasNodeFile()) {
            if (danglingTimeout.millis() >= 0) {
                synchronized (danglingMutex) {
                    for (String danglingIndex : danglingIndices.keySet()) {
                        if (newMetaData.hasIndex(danglingIndex)) {
                            logger.debug("[{}] no longer dangling (created), removing", danglingIndex);
                            DanglingIndex removed = danglingIndices.remove(danglingIndex);
                            FutureUtils.cancel(removed.future);
                        }
                    }
                    // delete indices that are no longer part of the metadata
                    try {
                        for (String indexName : nodeEnv.findAllIndices()) {
                            // if we have the index on the metadata, don't delete it
                            if (newMetaData.hasIndex(indexName)) {
                                continue;
                            }
                            if (danglingIndices.containsKey(indexName)) {
                                // already dangling, continue
                                continue;
                            }
                            final IndexMetaData indexMetaData = loadIndexState(indexName);
                            if (indexMetaData != null) {
                                if (autoImportDangled.shouldImport()) {
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, auto import to cluster state [{}]", indexName, autoImportDangled);
                                    danglingIndices.put(indexName, new DanglingIndex(indexName, null));
                                } else if (danglingTimeout.millis() == 0) {
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, timeout set to 0, deleting now", indexName);
                                    indicesService.deleteIndexStore("dangling index with timeout set to 0", indexMetaData, state);
                                } else {
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, scheduling to delete in [{}], auto import to cluster state [{}]", indexName, danglingTimeout, autoImportDangled);
                                    danglingIndices.put(indexName,
                                            new DanglingIndex(indexName,
                                                    threadPool.schedule(danglingTimeout,
                                                            ThreadPool.Names.SAME,
                                                            new RemoveDanglingIndex(indexMetaData))));
                                }
                            }
                        }
                    } catch (Throwable e) {
                        logger.warn("failed to find dangling indices", e);
                    }
                }
            }
            if (autoImportDangled.shouldImport() && !danglingIndices.isEmpty()) {
                final List<IndexMetaData> dangled = Lists.newArrayList();
                for (String indexName : danglingIndices.keySet()) {
                    IndexMetaData indexMetaData = loadIndexState(indexName);
                    if (indexMetaData == null) {
                        logger.debug("failed to find state for dangling index [{}]", indexName);
                        continue;
                    }
                    // we might have someone copying over an index, renaming the directory, handle that
                    if (!indexMetaData.index().equals(indexName)) {
                        logger.info("dangled index directory name is [{}], state name is [{}], renaming to directory name", indexName, indexMetaData.index());
                        indexMetaData = IndexMetaData.builder(indexMetaData).index(indexName).build();
                    }
                    if (autoImportDangled == AutoImportDangledState.CLOSED) {
                        indexMetaData = IndexMetaData.builder(indexMetaData).state(IndexMetaData.State.CLOSE).build();
                    }
                    if (indexMetaData != null) {
                        dangled.add(indexMetaData);
                    }
                }
                IndexMetaData[] dangledIndices = dangled.toArray(new IndexMetaData[dangled.size()]);
                try {
                    allocateDangledIndices.allocateDangled(dangledIndices, new LocalAllocateDangledIndices.Listener() {
                        @Override
                        public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse response) {
                            logger.trace("allocated dangled");
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.info("failed to send allocated dangled", e);
                        }
                    });
                } catch (Throwable e) {
                    logger.warn("failed to send allocate dangled", e);
                }
            }
        }

        if (success) {
            previousMetaData = newMetaData;
            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            previouslyWrittenIndices = builder.addAll(relevantIndices).build();
        }
    }

    public static Set<String> getRelevantIndices(ClusterState state, ImmutableSet<String> previouslyWrittenIndices) {
        Set<String> relevantIndices;
        if (isDataOnlyNode(state)) {
            relevantIndices = getRelevantIndicesOnDataOnlyNode(state, previouslyWrittenIndices);
        } else if (state.nodes().localNode().masterNode() == true) {
            relevantIndices = getRelevantIndicesForMasterEligibleNode(state);
        } else {
            relevantIndices = Collections.emptySet();
        }
        return relevantIndices;
    }

    /**
     * Loads the current meta state for each index in the new cluster state and checks if it has to be persisted.
     * Each index state that should be written to disk will be returned. This is only run for data only nodes.
     * It will return only the states for indices that actually have a shard allocated on the current node.
     *
     * @param previouslyWrittenIndices    A list of indices for which the state was already written before
     * @param potentiallyUnwrittenIndices The list of indices for which state should potentially be written
     * @param previousMetaData            The last meta data we know of. meta data for all indices in previouslyWrittenIndices list is persisted now
     * @param newMetaData                 The new metadata
     * @return iterable over all indices states that should be written to disk
     */
    public static Iterable<LocalGatewayMetaState.IndexMetaWriteInfo> resolveStatesToBeWritten(ImmutableSet<String> previouslyWrittenIndices, Set<String> potentiallyUnwrittenIndices, MetaData previousMetaData, MetaData newMetaData) {
        List<LocalGatewayMetaState.IndexMetaWriteInfo> indicesToWrite = new ArrayList<>();
        for (String index : potentiallyUnwrittenIndices) {
            IndexMetaData newIndexMetaData = newMetaData.index(index);
            IndexMetaData previousIndexMetaData = previousMetaData == null ? null : previousMetaData.index(index);
            String writeReason = null;
            if (previouslyWrittenIndices.contains(index) == false || previousIndexMetaData == null) {
                writeReason = "freshly created";
            } else if (previousIndexMetaData.version() != newIndexMetaData.version()) {
                writeReason = "version changed from [" + previousIndexMetaData.version() + "] to [" + newIndexMetaData.version() + "]";
            }
            if (writeReason != null) {
                indicesToWrite.add(new LocalGatewayMetaState.IndexMetaWriteInfo(newIndexMetaData, previousIndexMetaData, writeReason));
            }
        }
        return indicesToWrite;
    }

    public static Set<String> getRelevantIndicesOnDataOnlyNode(ClusterState state, ImmutableSet<String> previouslyWrittenIndices) {
        RoutingNode newRoutingNode = state.getRoutingNodes().node(state.nodes().localNodeId());
        if (newRoutingNode == null) {
            throw new IllegalStateException("cluster state does not contain this node - cannot write index meta state");
        }
        Set<String> indices = new HashSet<>();
        for (MutableShardRouting routing : newRoutingNode) {
            indices.add(routing.index());
        }
        // we have to check the meta data also: closed indices will not appear in the routing table, but we must still write the state if we have it written on disk previously
        for (IndexMetaData indexMetaData : state.metaData()) {
            if (previouslyWrittenIndices.contains(indexMetaData.getIndex()) && state.metaData().getIndices().get(indexMetaData.getIndex()).state().equals(IndexMetaData.State.CLOSE)) {
                indices.add(indexMetaData.getIndex());
            }
        }
        return indices;
    }

    public static Set<String> getRelevantIndicesForMasterEligibleNode(ClusterState state) {
        Set<String> relevantIndices;
        relevantIndices = new HashSet<>();
        // we have to iterate over the metadata to make sure we also capture closed indices
        for (IndexMetaData indexMetaData : state.metaData()) {
            relevantIndices.add(indexMetaData.getIndex());
        }
        return relevantIndices;
    }


    public static class IndexMetaWriteInfo {
        final IndexMetaData newMetaData;
        final String reason;
        final IndexMetaData previousMetaData;

        public IndexMetaWriteInfo(IndexMetaData newMetaData, IndexMetaData previousMetaData, String reason) {
            this.newMetaData = newMetaData;
            this.reason = reason;
            this.previousMetaData = previousMetaData;
        }

        public IndexMetaData getNewMetaData() {
            return newMetaData;
        }

        public String getReason() {
            return reason;
        }
    }

    protected static boolean isDataOnlyNode(ClusterState state) {
        return ((state.nodes().localNode().masterNode() == false) && state.nodes().localNode().dataNode());
    }

    /**
     * Returns a StateFormat that can read and write {@link MetaData}
     */
    static MetaDataStateFormat<MetaData> globalStateFormat(XContentType format, final ToXContent.Params formatParams) {
        return new MetaDataStateFormat<MetaData>(format, GLOBAL_STATE_FILE_PREFIX) {

            @Override
            public void toXContent(XContentBuilder builder, MetaData state) throws IOException {
                MetaData.Builder.toXContent(state, builder, formatParams);
            }

            @Override
            public MetaData fromXContent(XContentParser parser) throws IOException {
                return MetaData.Builder.fromXContent(parser);
            }
        };
    }

    /**
     * Returns a StateFormat that can read and write {@link IndexMetaData}
     */
    static MetaDataStateFormat<IndexMetaData> indexStateFormat(XContentType format, final ToXContent.Params formatParams) {
        return new MetaDataStateFormat<IndexMetaData>(format, INDEX_STATE_FILE_PREFIX) {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                IndexMetaData.Builder.toXContent(state, builder, formatParams);
            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
    }

    private void writeIndex(String reason, IndexMetaData indexMetaData, @Nullable IndexMetaData previousIndexMetaData) throws Exception {
        logger.trace("[{}] writing state, reason [{}]", indexMetaData.index(), reason);
        try {
            indexStateFormat.write(indexMetaData, indexMetaData.version(),
                    nodeEnv.indexLocations(new Index(indexMetaData.index())));
        } catch (Throwable ex) {
            logger.warn("[{}]: failed to write index state", ex, indexMetaData.index());
            throw new IOException("failed to write state for [" + indexMetaData.index() + "]", ex);
        }
    }

    private void writeGlobalState(String reason, MetaData metaData) throws Exception {
        logger.trace("{} writing state, reason [{}]", GLOBAL_STATE_LOG_TYPE, reason);
        try {
            globalStateFormat.write(metaData, metaData.version(), nodeEnv.nodeDataLocations());
        } catch (Throwable ex) {
            logger.warn("{}: failed to write global state", ex, GLOBAL_STATE_LOG_TYPE);
            throw new IOException("failed to write global state", ex);
        }
    }

    private MetaData loadState() throws Exception {
        MetaData globalMetaData = loadGlobalState();
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }

        final Set<String> indices = nodeEnv.findAllIndices();
        for (String index : indices) {
            IndexMetaData indexMetaData = loadIndexState(index);
            if (indexMetaData == null) {
                logger.debug("[{}] failed to find metadata for existing index location", index);
            } else {
                metaDataBuilder.put(indexMetaData, false);
            }
        }
        return metaDataBuilder.build();
    }

    @Nullable
    private IndexMetaData loadIndexState(String index) {
        return indexStateFormat.loadLatestState(logger, nodeEnv.indexLocations(new Index(index)));
    }

    private MetaData loadGlobalState() {
        return globalStateFormat.loadLatestState(logger, nodeEnv.nodeDataLocations());
    }


    private void pre019Upgrade() throws Exception {
        long index = -1;
        File metaDataFile = null;
        MetaData metaData = null;
        long version = -1;
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File stateLocation = new File(dataLocation, "_state");
            File[] stateFiles = stateLocation.listFiles();
            if (stateFiles == null) {
                continue;
            }
            for (File stateFile : stateFiles) {
                if (logger.isTraceEnabled()) {
                    logger.trace("[upgrade]: processing [" + stateFile.getName() + "]");
                }
                String name = stateFile.getName();
                if (!name.startsWith("metadata-")) {
                    continue;
                }
                long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
                if (fileIndex >= index) {
                    // try and read the meta data
                    try {
                        byte[] data = Streams.copyToByteArray(new FileInputStream(stateFile));
                        if (data.length == 0) {
                            continue;
                        }
                        try (XContentParser parser = XContentHelper.createParser(data, 0, data.length)) {
                            String currentFieldName = null;
                            XContentParser.Token token = parser.nextToken();
                            if (token != null) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        currentFieldName = parser.currentName();
                                    } else if (token == XContentParser.Token.START_OBJECT) {
                                        if ("meta-data".equals(currentFieldName)) {
                                            metaData = MetaData.Builder.fromXContent(parser);
                                        }
                                    } else if (token.isValue()) {
                                        if ("version".equals(currentFieldName)) {
                                            version = parser.longValue();
                                        }
                                    }
                                }
                            }
                        }
                        index = fileIndex;
                        metaDataFile = stateFile;
                    } catch (IOException e) {
                        logger.warn("failed to read pre 0.19 state from [" + name + "], ignoring...", e);
                    }
                }
            }
        }
        if (metaData == null) {
            return;
        }

        logger.info("found old metadata state, loading metadata from [{}] and converting to new metadata location and structure...", metaDataFile.getAbsolutePath());

        writeGlobalState("upgrade", MetaData.builder(metaData).version(version).build());
        for (IndexMetaData indexMetaData : metaData) {
            IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexMetaData).version(version);
            // set the created version to 0.18
            indexMetaDataBuilder.settings(ImmutableSettings.settingsBuilder().put(indexMetaData.settings()).put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_0_18_0));
            writeIndex("upgrade", indexMetaDataBuilder.build(), null);
        }

        // rename shards state to backup state
        File backupFile = new File(metaDataFile.getParentFile(), "backup-" + metaDataFile.getName());
        if (!metaDataFile.renameTo(backupFile)) {
            throw new IOException("failed to rename old state to backup state [" + metaDataFile.getAbsolutePath() + "]");
        }

        // delete all other shards state files
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File stateLocation = new File(dataLocation, "_state");
            File[] stateFiles = stateLocation.listFiles();
            if (stateFiles == null) {
                continue;
            }
            for (File stateFile : stateFiles) {
                String name = stateFile.getName();
                if (!name.startsWith("metadata-")) {
                    continue;
                }
                stateFile.delete();
            }
        }

        logger.info("conversion to new metadata location and format done, backup create at [{}]", backupFile.getAbsolutePath());
    }

    class RemoveDanglingIndex implements Runnable {

        private final IndexMetaData metaData;

        RemoveDanglingIndex(IndexMetaData metaData) {
            this.metaData = metaData;
        }

        @Override
        public void run() {
            synchronized (danglingMutex) {
                DanglingIndex remove = danglingIndices.remove(metaData.index());
                // no longer there...
                if (remove == null) {
                    return;
                }
                logger.warn("[{}] deleting dangling index", metaData.index());
                try {
                    indicesService.deleteIndexStore("deleting dangling index", metaData, clusterService.state());
                } catch (Exception ex) {
                    logger.debug("failed to delete dangling index", ex);
                }
            }
        }
    }

    static class DanglingIndex {
        public final String index;
        public final ScheduledFuture future;

        DanglingIndex(String index, ScheduledFuture future) {
            this.index = index;
            this.future = future;
        }
    }
}
