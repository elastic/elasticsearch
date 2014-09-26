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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

/**
 *
 */
public class LocalGatewayMetaState extends AbstractComponent implements ClusterStateListener {

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
    private final NodeIndexDeletedAction nodeIndexDeletedAction;

    @Nullable
    private volatile MetaData currentMetaData;

    private final XContentType format;
    private final ToXContent.Params formatParams;
    private final ToXContent.Params gatewayModeFormatParams;


    private final AutoImportDangledState autoImportDangled;
    private final TimeValue danglingTimeout;
    private final Map<String, DanglingIndex> danglingIndices = ConcurrentCollections.newConcurrentMap();
    private final Object danglingMutex = new Object();

    @Inject
    public LocalGatewayMetaState(Settings settings, ThreadPool threadPool, NodeEnvironment nodeEnv,
                                 TransportNodesListGatewayMetaState nodesListGatewayMetaState, LocalAllocateDangledIndices allocateDangledIndices,
                                 NodeIndexDeletedAction nodeIndexDeletedAction) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.threadPool = threadPool;
        this.format = XContentType.fromRestContentType(settings.get("format", "smile"));
        this.allocateDangledIndices = allocateDangledIndices;
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
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

        logger.debug("using gateway.local.auto_import_dangled [{}], with gateway.local.dangling_timeout [{}]", this.autoImportDangled, this.danglingTimeout);

        if (DiscoveryNode.masterNode(settings)) {
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
    }

    public MetaData loadMetaState() throws Exception {
        return loadState();
    }

    public boolean isDangling(String index) {
        return danglingIndices.containsKey(index);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence()) {
            // reset the current metadata, we need to start fresh...
            this.currentMetaData = null;
            return;
        }

        MetaData newMetaData = event.state().metaData();
        // we don't check if metaData changed, since we might be called several times and we need to check dangling...

        boolean success = true;
        // only applied to master node, writing the global and index level states
        if (event.state().nodes().localNode().masterNode()) {
            // check if the global state changed?
            if (currentMetaData == null || !MetaData.isGlobalStateEquals(currentMetaData, newMetaData)) {
                try {
                    writeGlobalState("changed", newMetaData, currentMetaData);
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
                    currentIndexMetaData = loadIndex(indexMetaData.index());
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

        // delete indices that were there before, but are deleted now
        // we need to do it so they won't be detected as dangling
        if (currentMetaData != null) {
            // only delete indices when we already received a state (currentMetaData != null)
            // and we had a go at processing dangling indices at least once
            // this will also delete the _state of the index itself
            for (IndexMetaData current : currentMetaData) {
                if (danglingIndices.containsKey(current.index())) {
                    continue;
                }
                if (!newMetaData.hasIndex(current.index())) {
                    logger.debug("[{}] deleting index that is no longer part of the metadata (indices: [{}])", current.index(), newMetaData.indices().keys());
                    if (nodeEnv.hasNodeFile()) {
                        FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(current.index())));
                    }
                    try {
                        nodeIndexDeletedAction.nodeIndexStoreDeleted(event.state(), current.index(), event.state().nodes().localNodeId());
                    } catch (Throwable e) {
                        logger.debug("[{}] failed to notify master on local index store deletion", e, current.index());
                    }
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
                            removed.future.cancel(false);
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
                            IndexMetaData indexMetaData = loadIndex(indexName);
                            if (indexMetaData != null) {
                                if (danglingTimeout.millis() == 0) {
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, timeout set to 0, deleting now", indexName);
                                    FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(indexName)));
                                } else {
                                    logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, scheduling to delete in [{}], auto import to cluster state [{}]", indexName, danglingTimeout, autoImportDangled);
                                    danglingIndices.put(indexName, new DanglingIndex(indexName, threadPool.schedule(danglingTimeout, ThreadPool.Names.SAME, new RemoveDanglingIndex(indexName))));
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
                    IndexMetaData indexMetaData = loadIndex(indexName);
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

    private void deleteIndex(String index) {
        logger.trace("[{}] delete index state", index);
        File[] indexLocations = nodeEnv.indexLocations(new Index(index));
        for (File indexLocation : indexLocations) {
            if (!indexLocation.exists()) {
                continue;
            }
            FileSystemUtils.deleteRecursively(new File(indexLocation, "_state"));
        }
    }

    private void writeIndex(String reason, IndexMetaData indexMetaData, @Nullable IndexMetaData previousIndexMetaData) throws Exception {
        logger.trace("[{}] writing state, reason [{}]", indexMetaData.index(), reason);
        XContentBuilder builder = XContentFactory.contentBuilder(format, new BytesStreamOutput());
        builder.startObject();
        IndexMetaData.Builder.toXContent(indexMetaData, builder, formatParams);
        builder.endObject();
        builder.flush();

        String stateFileName = "state-" + indexMetaData.version();
        Throwable lastFailure = null;
        boolean wroteAtLeastOnce = false;
        for (File indexLocation : nodeEnv.indexLocations(new Index(indexMetaData.index()))) {
            File stateLocation = new File(indexLocation, "_state");
            FileSystemUtils.mkdirs(stateLocation);
            File stateFile = new File(stateLocation, stateFileName);

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(stateFile);
                BytesReference bytes = builder.bytes();
                bytes.writeTo(fos);
                fos.getChannel().force(true);
                fos.close();
                wroteAtLeastOnce = true;
            } catch (Throwable e) {
                lastFailure = e;
            } finally {
                IOUtils.closeWhileHandlingException(fos);
            }
        }

        if (!wroteAtLeastOnce) {
            logger.warn("[{}]: failed to write index state", lastFailure, indexMetaData.index());
            throw new IOException("failed to write state for [" + indexMetaData.index() + "]", lastFailure);
        }

        // delete the old files
        if (previousIndexMetaData != null && previousIndexMetaData.version() != indexMetaData.version()) {
            for (File indexLocation : nodeEnv.indexLocations(new Index(indexMetaData.index()))) {
                File[] files = new File(indexLocation, "_state").listFiles();
                if (files == null) {
                    continue;
                }
                for (File file : files) {
                    if (!file.getName().startsWith("state-")) {
                        continue;
                    }
                    if (file.getName().equals(stateFileName)) {
                        continue;
                    }
                    file.delete();
                }
            }
        }
    }

    private void writeGlobalState(String reason, MetaData metaData, @Nullable MetaData previousMetaData) throws Exception {
        logger.trace("[_global] writing state, reason [{}]", reason);

        XContentBuilder builder = XContentFactory.contentBuilder(format);
        builder.startObject();
        MetaData.Builder.toXContent(metaData, builder, gatewayModeFormatParams);
        builder.endObject();
        builder.flush();
        String globalFileName = "global-" + metaData.version();
        Throwable lastFailure = null;
        boolean wroteAtLeastOnce = false;
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File stateLocation = new File(dataLocation, "_state");
            FileSystemUtils.mkdirs(stateLocation);
            File stateFile = new File(stateLocation, globalFileName);

            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(stateFile);
                BytesReference bytes = builder.bytes();
                bytes.writeTo(fos);
                fos.getChannel().force(true);
                fos.close();
                wroteAtLeastOnce = true;
            } catch (Throwable e) {
                lastFailure = e;
            } finally {
                IOUtils.closeWhileHandlingException(fos);
            }
        }

        if (!wroteAtLeastOnce) {
            logger.warn("[_global]: failed to write global state", lastFailure);
            throw new IOException("failed to write global state", lastFailure);
        }

        // delete the old files
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File[] files = new File(dataLocation, "_state").listFiles();
            if (files == null) {
                continue;
            }
            for (File file : files) {
                if (!file.getName().startsWith("global-")) {
                    continue;
                }
                if (file.getName().equals(globalFileName)) {
                    continue;
                }
                file.delete();
            }
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

        Set<String> indices = nodeEnv.findAllIndices();
        for (String index : indices) {
            IndexMetaData indexMetaData = loadIndex(index);
            if (indexMetaData == null) {
                logger.debug("[{}] failed to find metadata for existing index location", index);
            } else {
                metaDataBuilder.put(indexMetaData, false);
            }
        }
        return metaDataBuilder.build();
    }

    @Nullable
    private IndexMetaData loadIndex(String index) {
        long highestVersion = -1;
        IndexMetaData indexMetaData = null;
        for (File indexLocation : nodeEnv.indexLocations(new Index(index))) {
            File stateDir = new File(indexLocation, "_state");
            if (!stateDir.exists() || !stateDir.isDirectory()) {
                continue;
            }
            // now, iterate over the current versions, and find latest one
            File[] stateFiles = stateDir.listFiles();
            if (stateFiles == null) {
                continue;
            }
            for (File stateFile : stateFiles) {
                if (!stateFile.getName().startsWith("state-")) {
                    continue;
                }
                try {
                    long version = Long.parseLong(stateFile.getName().substring("state-".length()));
                    if (version > highestVersion) {
                        byte[] data = Streams.copyToByteArray(new FileInputStream(stateFile));
                        if (data.length == 0) {
                            logger.debug("[{}]: no data for [" + stateFile.getAbsolutePath() + "], ignoring...", index);
                            continue;
                        }
                        XContentParser parser = null;
                        try {
                            parser = XContentHelper.createParser(data, 0, data.length);
                            parser.nextToken(); // move to START_OBJECT
                            indexMetaData = IndexMetaData.Builder.fromXContent(parser);
                            highestVersion = version;
                        } finally {
                            if (parser != null) {
                                parser.close();
                            }
                        }
                    }
                } catch (Throwable e) {
                    logger.debug("[{}]: failed to read [" + stateFile.getAbsolutePath() + "], ignoring...", e, index);
                }
            }
        }
        return indexMetaData;
    }

    private MetaData loadGlobalState() {
        long highestVersion = -1;
        MetaData metaData = null;
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File stateLocation = new File(dataLocation, "_state");
            if (!stateLocation.exists()) {
                continue;
            }
            File[] stateFiles = stateLocation.listFiles();
            if (stateFiles == null) {
                continue;
            }
            for (File stateFile : stateFiles) {
                String name = stateFile.getName();
                if (!name.startsWith("global-")) {
                    continue;
                }
                try {
                    long version = Long.parseLong(stateFile.getName().substring("global-".length()));
                    if (version > highestVersion) {
                        byte[] data = Streams.copyToByteArray(new FileInputStream(stateFile));
                        if (data.length == 0) {
                            logger.debug("[_global] no data for [" + stateFile.getAbsolutePath() + "], ignoring...");
                            continue;
                        }

                        XContentParser parser = null;
                        try {
                            parser = XContentHelper.createParser(data, 0, data.length);
                            metaData = MetaData.Builder.fromXContent(parser);
                            highestVersion = version;
                        } finally {
                            if (parser != null) {
                                parser.close();
                            }
                        }
                    }
                } catch (Throwable e) {
                    logger.debug("failed to load global state from [{}]", e, stateFile.getAbsolutePath());
                }
            }
        }

        return metaData;
    }

    private void pre019Upgrade() throws Exception {
        long index = -1;
        File metaDataFile = null;
        MetaData metaData = null;
        long version = -1;
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File stateLocation = new File(dataLocation, "_state");
            if (!stateLocation.exists()) {
                continue;
            }
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

        logger.info("found old metadata state, loading metadata from [{}] and converting to new metadata location and strucutre...", metaDataFile.getAbsolutePath());

        writeGlobalState("upgrade", MetaData.builder(metaData).version(version).build(), null);
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
            if (!stateLocation.exists()) {
                continue;
            }
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

        private final String index;

        RemoveDanglingIndex(String index) {
            this.index = index;
        }

        @Override
        public void run() {
            synchronized (danglingMutex) {
                DanglingIndex remove = danglingIndices.remove(index);
                // no longer there...
                if (remove == null) {
                    return;
                }
                logger.info("[{}] deleting dangling index", index);
                FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(index)));
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
