/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.gateway.local.state.meta;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LocalGatewayMetaState extends AbstractComponent implements ClusterStateListener {

    private final NodeEnvironment nodeEnv;

    private volatile MetaData currentMetaData;

    private final XContentType format;
    private final ToXContent.Params formatParams;

    @Inject
    public LocalGatewayMetaState(Settings settings, NodeEnvironment nodeEnv, TransportNodesListGatewayMetaState nodesListGatewayMetaState) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.format = XContentType.fromRestContentType(settings.get("format", "smile"));
        nodesListGatewayMetaState.init(this);

        if (this.format == XContentType.SMILE) {
            Map<String, String> params = Maps.newHashMap();
            params.put("binary", "true");
            formatParams = new ToXContent.MapParams(params);
        } else {
            formatParams = ToXContent.EMPTY_PARAMS;
        }

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

    public MetaData currentMetaData() {
        return currentMetaData;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        if (!event.state().nodes().localNode().masterNode()) {
            return;
        }

        if (!event.metaDataChanged()) {
            return;
        }

        // check if the global state changed?
        boolean success = true;
        if (currentMetaData == null || !MetaData.isGlobalStateEquals(currentMetaData, event.state().metaData())) {
            try {
                writeGlobalState("changed", event.state().metaData(), currentMetaData);
            } catch (Exception e) {
                success = false;
            }
        }

        // check and write changes in indices
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            String writeReason = null;
            IndexMetaData currentIndexMetaData = currentMetaData == null ? null : currentMetaData.index(indexMetaData.index());
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
            } catch (Exception e) {
                success = false;
            }
        }

        // delete indices that are no longer there...
        if (currentMetaData != null) {
            for (IndexMetaData current : currentMetaData) {
                if (event.state().metaData().index(current.index()) == null) {
                    deleteIndex(current.index());
                }
            }
        }

        if (success) {
            currentMetaData = event.state().metaData();
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
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(format, cachedEntry.cachedBytes());
            builder.startObject();
            IndexMetaData.Builder.toXContent(indexMetaData, builder, formatParams);
            builder.endObject();
            builder.flush();

            Exception lastFailure = null;
            boolean wroteAtLeastOnce = false;
            for (File indexLocation : nodeEnv.indexLocations(new Index(indexMetaData.index()))) {
                File stateLocation = new File(indexLocation, "_state");
                FileSystemUtils.mkdirs(stateLocation);
                File stateFile = new File(stateLocation, "state-" + indexMetaData.version());

                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(stateFile);
                    fos.write(cachedEntry.bytes().underlyingBytes(), 0, cachedEntry.bytes().size());
                    fos.getChannel().force(true);
                    Closeables.closeQuietly(fos);
                    wroteAtLeastOnce = true;
                } catch (Exception e) {
                    lastFailure = e;
                } finally {
                    Closeables.closeQuietly(fos);
                }
            }

            if (!wroteAtLeastOnce) {
                logger.warn("[{}]: failed to state", lastFailure, indexMetaData.index());
                throw new IOException("failed to write state for [" + indexMetaData.index() + "]", lastFailure);
            }

            // delete the old files
            if (previousIndexMetaData != null && previousIndexMetaData.version() != indexMetaData.version()) {
                for (File indexLocation : nodeEnv.indexLocations(new Index(indexMetaData.index()))) {
                    File stateFile = new File(new File(indexLocation, "_state"), "state-" + previousIndexMetaData.version());
                    stateFile.delete();
                }
            }
        } finally {
            CachedStreamOutput.pushEntry(cachedEntry);
        }
    }

    private void writeGlobalState(String reason, MetaData metaData, @Nullable MetaData previousMetaData) throws Exception {
        logger.trace("[_global] writing state, reason [{}]", reason);
        // create metadata to write with just the global state
        MetaData globalMetaData = MetaData.builder().metaData(metaData).removeAllIndices().build();

        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(format, cachedEntry.cachedBytes());
            builder.startObject();
            MetaData.Builder.toXContent(globalMetaData, builder, formatParams);
            builder.endObject();
            builder.flush();

            Exception lastFailure = null;
            boolean wroteAtLeastOnce = false;
            for (File dataLocation : nodeEnv.nodeDataLocations()) {
                File stateLocation = new File(dataLocation, "_state");
                FileSystemUtils.mkdirs(stateLocation);
                File stateFile = new File(stateLocation, "global-" + globalMetaData.version());

                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(stateFile);
                    fos.write(cachedEntry.bytes().underlyingBytes(), 0, cachedEntry.bytes().size());
                    fos.getChannel().force(true);
                    Closeables.closeQuietly(fos);
                    wroteAtLeastOnce = true;
                } catch (Exception e) {
                    lastFailure = e;
                } finally {
                    Closeables.closeQuietly(fos);
                }
            }

            if (!wroteAtLeastOnce) {
                logger.warn("[_global]: failed to write global state", lastFailure);
                throw new IOException("failed to write global state", lastFailure);
            }

            // delete the old files
            if (previousMetaData != null && previousMetaData.version() != currentMetaData.version()) {
                for (File dataLocation : nodeEnv.nodeDataLocations()) {
                    File stateFile = new File(new File(dataLocation, "_state"), "global-" + previousMetaData.version());
                    stateFile.delete();
                }
            }
        } finally {
            CachedStreamOutput.pushEntry(cachedEntry);
        }
    }

    private void loadState() throws Exception {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        MetaData globalMetaData = loadGlobalState();
        if (globalMetaData != null) {
            metaDataBuilder.metaData(globalMetaData);
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
        currentMetaData = metaDataBuilder.build();
    }

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
                } catch (Exception e) {
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
                } catch (Exception e) {
                    logger.debug("");
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
                        XContentParser parser = XContentHelper.createParser(data, 0, data.length);
                        try {
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
                        } finally {
                            parser.close();
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

        writeGlobalState("upgrade", MetaData.builder().metaData(metaData).version(version).build(), null);
        for (IndexMetaData indexMetaData : metaData) {
            IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.newIndexMetaDataBuilder(indexMetaData).version(version);
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
}
