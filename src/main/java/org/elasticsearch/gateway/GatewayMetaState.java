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

package org.elasticsearch.gateway;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.cluster.routing.SimpleHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.regex.Pattern;

/**
 *
 */
public class GatewayMetaState extends AbstractComponent implements ClusterStateListener {

    static final String GLOBAL_STATE_FILE_PREFIX = "global-";
    private static final String INDEX_STATE_FILE_PREFIX = "state-";
    static final Pattern GLOBAL_STATE_FILE_PATTERN = Pattern.compile(GLOBAL_STATE_FILE_PREFIX + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    static final Pattern INDEX_STATE_FILE_PATTERN = Pattern.compile(INDEX_STATE_FILE_PREFIX + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    private static final String GLOBAL_STATE_LOG_TYPE = "[_global]";
    private static final String DEPRECATED_SETTING_ROUTING_HASH_FUNCTION = "cluster.routing.operation.hash.type";
    private static final String DEPRECATED_SETTING_ROUTING_USE_TYPE = "cluster.routing.operation.use_type";
    public static final String GATEWAY_DANGLING_TIMEOUT = "gateway.dangling_timeout";
    public static final String GATEWAY_DELETE_TIMEOUT = "gateway.delete_timeout";
    public static final String GATEWAY_AUTO_IMPORT_DANGLED = "gateway.auto_import_dangled";
    // legacy - this used to be in a different package
    private static final String GATEWAY_LOCAL_DANGLING_TIMEOUT = "gateway.local.dangling_timeout";
    private static final String GATEWAY_LOCAL_AUTO_IMPORT_DANGLED = "gateway.local.auto_import_dangled";

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
    private volatile MetaData currentMetaData;

    private final XContentType format;
    private final ToXContent.Params formatParams;
    private final ToXContent.Params gatewayModeFormatParams;


    private final AutoImportDangledState autoImportDangled;
    private final TimeValue danglingTimeout;
    private final TimeValue deleteTimeout;
    private final Map<String, DanglingIndex> danglingIndices = ConcurrentCollections.newConcurrentMap();
    private final Object danglingMutex = new Object();
    private final IndicesService indicesService;

    @Inject
    public GatewayMetaState(Settings settings, ThreadPool threadPool, NodeEnvironment nodeEnv,
                            TransportNodesListGatewayMetaState nodesListGatewayMetaState, LocalAllocateDangledIndices allocateDangledIndices,
                            IndicesService indicesService) throws Exception {
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

        this.autoImportDangled = AutoImportDangledState.fromString(settings.get(GATEWAY_AUTO_IMPORT_DANGLED, settings.get(GATEWAY_LOCAL_AUTO_IMPORT_DANGLED, AutoImportDangledState.YES.toString())));
        this.danglingTimeout = settings.getAsTime(GATEWAY_DANGLING_TIMEOUT, settings.getAsTime(GATEWAY_LOCAL_DANGLING_TIMEOUT, TimeValue.timeValueHours(2)));
        this.deleteTimeout = settings.getAsTime(GATEWAY_DELETE_TIMEOUT, TimeValue.timeValueSeconds(30));

        logger.debug("using {} [{}],  {} [{}], with {} [{}]",
                GATEWAY_AUTO_IMPORT_DANGLED, this.autoImportDangled,
                GATEWAY_DELETE_TIMEOUT, this.deleteTimeout,
                GATEWAY_DANGLING_TIMEOUT, this.danglingTimeout);
        if (DiscoveryNode.masterNode(settings) || DiscoveryNode.dataNode(settings)) {
            nodeEnv.ensureAtomicMoveSupported();
        }
        if (DiscoveryNode.masterNode(settings)) {
            try {
                ensureNoPre019State();
                pre20Upgrade();
                long start = System.currentTimeMillis();
                loadState();
                logger.debug("took {} to load state", TimeValue.timeValueMillis(System.currentTimeMillis() - start));
            } catch (Exception e) {
                logger.error("failed to read local state, exiting...", e);
                throw e;
            }
        }
        this.indicesService = indicesService;
    }

    public MetaData loadMetaState() throws Exception {
        return loadState();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().disableStatePersistence()) {
            // reset the current metadata, we need to start fresh...
            this.currentMetaData = null;
            return;
        }

        MetaData newMetaData = state.metaData();
        // we don't check if metaData changed, since we might be called several times and we need to check dangling...

        boolean success = true;
        // only applied to master node, writing the global and index level states
        if (state.nodes().localNode().masterNode()) {
            // check if the global state changed?
            if (currentMetaData == null || !MetaData.isGlobalStateEquals(currentMetaData, newMetaData)) {
                try {
                    writeGlobalState("changed", newMetaData);
                } catch (Throwable e) {
                    success = false;
                }
            }

            // check and write changes in indices
            for (IndexMetaData indexMetaData : newMetaData) {
                String writeReason = null;
                IndexMetaData currentIndexMetaData;
                if (currentMetaData == null) {
                    // a new event..., check from the state stored
                    try {
                        currentIndexMetaData = loadIndexState(indexMetaData.index());
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to load index state", ex);
                    }
                } else {
                    currentIndexMetaData = currentMetaData.index(indexMetaData.index());
                }
                if (currentIndexMetaData == null) {
                    writeReason = "freshly created";
                } else if (currentIndexMetaData.version() != indexMetaData.version()) {
                    writeReason = "version changed from [" + currentIndexMetaData.version() + "] to [" + indexMetaData.version() + "]";
                }

                // we update the writeReason only if we really need to write it
                if (writeReason == null) {
                    continue;
                }

                try {
                    writeIndex(writeReason, indexMetaData, currentIndexMetaData);
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
                                if(autoImportDangled.shouldImport()){
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, auto import to cluster state [{}]", indexName, autoImportDangled);
                                    danglingIndices.put(indexName, new DanglingIndex(indexName, null));
                                } else if (danglingTimeout.millis() == 0) {
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, timeout set to 0, deleting now", indexName);
                                    indicesService.deleteIndexStore("dangling index with timeout set to 0", indexMetaData);
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
                    IndexMetaData indexMetaData;
                    try {
                        indexMetaData = loadIndexState(indexName);
                    } catch (IOException ex) {
                        throw new ElasticsearchException("failed to load index state", ex);
                    }
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
            currentMetaData = newMetaData;
        }
    }

    /**
     * Returns a StateFormat that can read and write {@link MetaData}
     */
    static MetaDataStateFormat<MetaData> globalStateFormat(XContentType format, final ToXContent.Params formatParams, final boolean deleteOldFiles) {
        return new MetaDataStateFormat<MetaData>(format, deleteOldFiles) {

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
    static MetaDataStateFormat<IndexMetaData> indexStateFormat(XContentType format, final ToXContent.Params formatParams, boolean deleteOldFiles) {
        return new MetaDataStateFormat<IndexMetaData>(format, deleteOldFiles) {

            @Override
            public void toXContent(XContentBuilder builder, IndexMetaData state) throws IOException {
                IndexMetaData.Builder.toXContent(state, builder, formatParams);            }

            @Override
            public IndexMetaData fromXContent(XContentParser parser) throws IOException {
                return IndexMetaData.Builder.fromXContent(parser);
            }
        };
    }

    private void writeIndex(String reason, IndexMetaData indexMetaData, @Nullable IndexMetaData previousIndexMetaData) throws Exception {
        logger.trace("[{}] writing state, reason [{}]", indexMetaData.index(), reason);
        final boolean deleteOldFiles = previousIndexMetaData != null && previousIndexMetaData.version() != indexMetaData.version();
        final MetaDataStateFormat<IndexMetaData> writer = indexStateFormat(format, formatParams, deleteOldFiles);
        try {
            writer.write(indexMetaData, INDEX_STATE_FILE_PREFIX, indexMetaData.version(),
                    nodeEnv.indexPaths(new Index(indexMetaData.index())));
        } catch (Throwable ex) {
            logger.warn("[{}]: failed to write index state", ex, indexMetaData.index());
            throw new IOException("failed to write state for [" + indexMetaData.index() + "]", ex);
        }
    }

    private void writeGlobalState(String reason, MetaData metaData) throws Exception {
        logger.trace("{} writing state, reason [{}]", GLOBAL_STATE_LOG_TYPE, reason);
        final MetaDataStateFormat<MetaData> writer = globalStateFormat(format, gatewayModeFormatParams, true);
        try {
            writer.write(metaData, GLOBAL_STATE_FILE_PREFIX, metaData.version(), nodeEnv.nodeDataPaths());
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
    private IndexMetaData loadIndexState(String index) throws IOException {
        return MetaDataStateFormat.loadLatestState(logger, indexStateFormat(format, formatParams, true),
                INDEX_STATE_FILE_PATTERN, "[" + index + "]", nodeEnv.indexPaths(new Index(index)));
    }

    private MetaData loadGlobalState() throws IOException {
        return MetaDataStateFormat.loadLatestState(logger, globalStateFormat(format, gatewayModeFormatParams, true), GLOBAL_STATE_FILE_PATTERN, GLOBAL_STATE_LOG_TYPE, nodeEnv.nodeDataPaths());
    }


    /**
     * Throws an IAE if a pre 0.19 state is detected
     */
    private void ensureNoPre019State() throws Exception {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (!Files.exists(stateLocation)) {
                continue;
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation)) {
                for (Path stateFile : stream) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[upgrade]: processing [" + stateFile.getFileName() + "]");
                    }
                    final String name = stateFile.getFileName().toString();
                    if (name.startsWith("metadata-")) {
                        throw new ElasticsearchIllegalStateException("Detected pre 0.19 metadata file please upgrade to a version before "
                                + Version.CURRENT.minimumCompatibilityVersion()
                                + " first to upgrade state structures - metadata found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }

    /**
     * Elasticsearch 2.0 deprecated custom routing hash functions. So what we do here is that for old indices, we
     * move this old & deprecated node setting to an index setting so that we can keep things backward compatible.
     */
    private void pre20Upgrade() throws Exception {
        final Class<? extends HashFunction> pre20HashFunction;
        final String pre20HashFunctionName = settings.get(DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, null);
        final boolean hasCustomPre20HashFunction = pre20HashFunctionName != null;
        // the hash function package has changed we replace the two hash functions if their fully qualified name is used.
        if (hasCustomPre20HashFunction) {
            switch (pre20HashFunctionName) {
                case "org.elasticsearch.cluster.routing.operation.hash.simple.SimpleHashFunction":
                    pre20HashFunction = SimpleHashFunction.class;
                    break;
                case "org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction":
                    pre20HashFunction = DjbHashFunction.class;
                    break;
                default:
                    pre20HashFunction = settings.getAsClass(DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, DjbHashFunction.class, "org.elasticsearch.cluster.routing.", "HashFunction");
            }
        } else {
            pre20HashFunction = DjbHashFunction.class;
        }
        final Boolean pre20UseType = settings.getAsBoolean(DEPRECATED_SETTING_ROUTING_USE_TYPE, null);
        MetaData metaData = loadMetaState();
        for (IndexMetaData indexMetaData : metaData) {
            if (indexMetaData.settings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION) == null
                    && indexMetaData.getCreationVersion().before(Version.V_2_0_0)) {
                // these settings need an upgrade
                Settings indexSettings = ImmutableSettings.builder().put(indexMetaData.settings())
                        .put(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION, pre20HashFunction)
                        .put(IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE, pre20UseType == null ? false : pre20UseType)
                        .build();
                IndexMetaData newMetaData = IndexMetaData.builder(indexMetaData)
                        .version(indexMetaData.version())
                        .settings(indexSettings)
                        .build();
                writeIndex("upgrade", newMetaData, null);
            } else if (indexMetaData.getCreationVersion().onOrAfter(Version.V_2_0_0)) {
                if (indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION) != null
                        || indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE) != null) {
                    throw new ElasticsearchIllegalStateException("Indices created on or after 2.0 should NOT contain [" + IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION
                            + "] + or [" + IndexMetaData.SETTING_LEGACY_ROUTING_USE_TYPE + "] in their index settings");
                }
            }
        }
        if (hasCustomPre20HashFunction|| pre20UseType != null) {
            logger.warn("Settings [{}] and [{}] are deprecated. Index settings from your old indices have been updated to record the fact that they "
                    + "used some custom routing logic, you can now remove these settings from your `elasticsearch.yml` file", DEPRECATED_SETTING_ROUTING_HASH_FUNCTION, DEPRECATED_SETTING_ROUTING_USE_TYPE);
        }
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
                    indicesService.deleteIndexStore("deleting dangling index", metaData);
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
