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

package org.elasticsearch.gateway.local.state.shards;

import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.local.state.meta.LocalGatewayMetaState;
import org.elasticsearch.index.shard.ShardId;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 */
public class LocalGatewayShardsState extends AbstractComponent implements ClusterStateListener {

    private final NodeEnvironment nodeEnv;
    private final LocalGatewayMetaState metaState;

    private volatile Map<ShardId, ShardStateInfo> currentState = Maps.newHashMap();

    @Inject
    public LocalGatewayShardsState(Settings settings, NodeEnvironment nodeEnv, TransportNodesListGatewayStartedShards listGatewayStartedShards, LocalGatewayMetaState metaState) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaState = metaState;
        listGatewayStartedShards.initGateway(this);

        if (DiscoveryNode.dataNode(settings)) {
            try {
                pre019Upgrade();
                long start = System.currentTimeMillis();
                currentState = loadShardsStateInfo();
                logger.debug("took {} to load started shards state", TimeValue.timeValueMillis(System.currentTimeMillis() - start));
            } catch (Exception e) {
                logger.error("failed to read local state (started shards), exiting...", e);
                throw e;
            }
        }
    }

    public Map<ShardId, ShardStateInfo> currentStartedShards() {
        return this.currentState;
    }

    public ShardStateInfo loadShardInfo(ShardId shardId) throws Exception {
        return loadShardStateInfo(shardId);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        if (!event.state().nodes().localNode().dataNode()) {
            return;
        }

        if (!event.routingTableChanged()) {
            return;
        }

        Map<ShardId, ShardStateInfo> newState = Maps.newHashMap();
        newState.putAll(this.currentState);


        // remove from the current state all the shards that are completely started somewhere, we won't need them anymore
        // and if they are still here, we will add them in the next phase
        // Also note, this works well when closing an index, since a closed index will have no routing shards entries
        // so they won't get removed (we want to keep the fact that those shards are allocated on this node if needed)
        for (IndexRoutingTable indexRoutingTable : event.state().routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                if (indexShardRoutingTable.countWithState(ShardRoutingState.STARTED) == indexShardRoutingTable.size()) {
                    newState.remove(indexShardRoutingTable.shardId());
                }
            }
        }
        // remove deleted indices from the started shards
        for (ShardId shardId : currentState.keySet()) {
            if (!event.state().metaData().hasIndex(shardId.index().name())) {
                newState.remove(shardId);
            }
        }
        // now, add all the ones that are active and on this node
        RoutingNode routingNode = event.state().readOnlyRoutingNodes().node(event.state().nodes().localNodeId());
        if (routingNode != null) {
            // our node is not in play yet...
            for (MutableShardRouting shardRouting : routingNode) {
                if (shardRouting.active()) {
                    newState.put(shardRouting.shardId(), new ShardStateInfo(shardRouting.version(), shardRouting.primary()));
                }
            }
        }

        // go over the write started shards if needed
        for (Iterator<Map.Entry<ShardId, ShardStateInfo>> it = newState.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<ShardId, ShardStateInfo> entry = it.next();
            ShardId shardId = entry.getKey();
            ShardStateInfo shardStateInfo = entry.getValue();

            String writeReason = null;
            ShardStateInfo currentShardStateInfo = currentState.get(shardId);
            if (currentShardStateInfo == null) {
                writeReason = "freshly started, version [" + shardStateInfo.version + "]";
            } else if (currentShardStateInfo.version != shardStateInfo.version) {
                writeReason = "version changed from [" + currentShardStateInfo.version + "] to [" + shardStateInfo.version + "]";
            }

            // we update the write reason if we really need to write a new one...
            if (writeReason == null) {
                continue;
            }

            try {
                writeShardState(writeReason, shardId, shardStateInfo, currentShardStateInfo);
            } catch (Exception e) {
                // we failed to write the shard state, remove it from our builder, we will try and write
                // it next time...
                it.remove();
            }
        }

        // REMOVED: don't delete shard state, rely on IndicesStore to delete the shard location
        //          only once all shards are allocated on another node
        // now, go over the current ones and delete ones that are not in the new one
//        for (Map.Entry<ShardId, ShardStateInfo> entry : currentState.entrySet()) {
//            ShardId shardId = entry.getKey();
//            if (!newState.containsKey(shardId)) {
//                if (!metaState.isDangling(shardId.index().name())) {
//                    deleteShardState(shardId);
//                }
//            }
//        }

        this.currentState = newState;
    }

    private Map<ShardId, ShardStateInfo> loadShardsStateInfo() throws Exception {
        Set<ShardId> shardIds = nodeEnv.findAllShardIds();
        long highestVersion = -1;
        Map<ShardId, ShardStateInfo> shardsState = Maps.newHashMap();
        for (ShardId shardId : shardIds) {
            ShardStateInfo shardStateInfo = loadShardStateInfo(shardId);
            if (shardStateInfo == null) {
                continue;
            }
            shardsState.put(shardId, shardStateInfo);

            // update the global version
            if (shardStateInfo.version > highestVersion) {
                highestVersion = shardStateInfo.version;
            }
        }
        return shardsState;
    }

    private ShardStateInfo loadShardStateInfo(ShardId shardId) {
        long highestShardVersion = -1;
        ShardStateInfo highestShardState = null;
        for (File shardLocation : nodeEnv.shardLocations(shardId)) {
            File shardStateDir = new File(shardLocation, "_state");
            if (!shardStateDir.exists() || !shardStateDir.isDirectory()) {
                continue;
            }
            // now, iterate over the current versions, and find latest one
            File[] stateFiles = shardStateDir.listFiles();
            if (stateFiles == null) {
                continue;
            }
            for (File stateFile : stateFiles) {
                if (!stateFile.getName().startsWith("state-")) {
                    continue;
                }
                try {
                    long version = Long.parseLong(stateFile.getName().substring("state-".length()));
                    if (version > highestShardVersion) {
                        byte[] data = Streams.copyToByteArray(new FileInputStream(stateFile));
                        if (data.length == 0) {
                            logger.debug("[{}][{}]: not data for [" + stateFile.getAbsolutePath() + "], ignoring...", shardId.index().name(), shardId.id());
                            continue;
                        }
                        ShardStateInfo readState = readShardState(data);
                        if (readState == null) {
                            logger.debug("[{}][{}]: not data for [" + stateFile.getAbsolutePath() + "], ignoring...", shardId.index().name(), shardId.id());
                            continue;
                        }
                        assert readState.version == version;
                        highestShardState = readState;
                        highestShardVersion = version;
                    }
                } catch (Exception e) {
                    logger.debug("[{}][{}]: failed to read [" + stateFile.getAbsolutePath() + "], ignoring...", e, shardId.index().name(), shardId.id());
                }
            }
        }
        return highestShardState;
    }

    @Nullable
    private ShardStateInfo readShardState(byte[] data) throws Exception {
        XContentParser parser = null;
        try {
            parser = XContentHelper.createParser(data, 0, data.length);
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                return null;
            }
            long version = -1;
            Boolean primary = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        version = parser.longValue();
                    } else if ("primary".equals(currentFieldName)) {
                        primary = parser.booleanValue();
                    }
                }
            }
            return new ShardStateInfo(version, primary);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    private void writeShardState(String reason, ShardId shardId, ShardStateInfo shardStateInfo, @Nullable ShardStateInfo previousStateInfo) throws Exception {
        logger.trace("[{}][{}] writing shard state, reason [{}]", shardId.index().name(), shardId.id(), reason);
        CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, cachedEntry.bytes());
            builder.prettyPrint();
            builder.startObject();
            builder.field("version", shardStateInfo.version);
            if (shardStateInfo.primary != null) {
                builder.field("primary", shardStateInfo.primary);
            }
            builder.endObject();
            builder.flush();

            Exception lastFailure = null;
            boolean wroteAtLeastOnce = false;
            for (File shardLocation : nodeEnv.shardLocations(shardId)) {
                File shardStateDir = new File(shardLocation, "_state");
                FileSystemUtils.mkdirs(shardStateDir);
                File stateFile = new File(shardStateDir, "state-" + shardStateInfo.version);


                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(stateFile);
                    BytesReference bytes = cachedEntry.bytes().bytes();
                    fos.write(bytes.array(), bytes.arrayOffset(), bytes.length());
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
                logger.warn("[{}][{}]: failed to write shard state", shardId.index().name(), shardId.id(), lastFailure);
                throw new IOException("failed to write shard state for " + shardId, lastFailure);
            }

            // delete the old files
            if (previousStateInfo != null && previousStateInfo.version != shardStateInfo.version) {
                for (File shardLocation : nodeEnv.shardLocations(shardId)) {
                    File stateFile = new File(new File(shardLocation, "_state"), "state-" + previousStateInfo.version);
                    stateFile.delete();
                }
            }
        } finally {
            CachedStreamOutput.pushEntry(cachedEntry);
        }
    }

    private void deleteShardState(ShardId shardId) {
        logger.trace("[{}][{}] delete shard state", shardId.index().name(), shardId.id());
        File[] shardLocations = nodeEnv.shardLocations(shardId);
        for (File shardLocation : shardLocations) {
            if (!shardLocation.exists()) {
                continue;
            }
            FileSystemUtils.deleteRecursively(new File(shardLocation, "_state"));
        }
    }

    private void pre019Upgrade() throws Exception {
        long index = -1;
        File latest = null;
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
                    logger.trace("[find_latest_state]: processing [" + stateFile.getName() + "]");
                }
                String name = stateFile.getName();
                if (!name.startsWith("shards-")) {
                    continue;
                }
                long fileIndex = Long.parseLong(name.substring(name.indexOf('-') + 1));
                if (fileIndex >= index) {
                    // try and read the meta data
                    try {
                        byte[] data = Streams.copyToByteArray(new FileInputStream(stateFile));
                        if (data.length == 0) {
                            logger.debug("[upgrade]: not data for [" + name + "], ignoring...");
                        }
                        pre09ReadState(data);
                        index = fileIndex;
                        latest = stateFile;
                    } catch (IOException e) {
                        logger.warn("[upgrade]: failed to read state from [" + name + "], ignoring...", e);
                    }
                }
            }
        }
        if (latest == null) {
            return;
        }

        logger.info("found old shards state, loading started shards from [{}] and converting to new shards state locations...", latest.getAbsolutePath());
        Map<ShardId, ShardStateInfo> shardsState = pre09ReadState(Streams.copyToByteArray(new FileInputStream(latest)));

        for (Map.Entry<ShardId, ShardStateInfo> entry : shardsState.entrySet()) {
            writeShardState("upgrade", entry.getKey(), entry.getValue(), null);
        }

        // rename shards state to backup state
        File backupFile = new File(latest.getParentFile(), "backup-" + latest.getName());
        if (!latest.renameTo(backupFile)) {
            throw new IOException("failed to rename old state to backup state [" + latest.getAbsolutePath() + "]");
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
                if (!name.startsWith("shards-")) {
                    continue;
                }
                stateFile.delete();
            }
        }

        logger.info("conversion to new shards state location and format done, backup create at [{}]", backupFile.getAbsolutePath());
    }

    private Map<ShardId, ShardStateInfo> pre09ReadState(byte[] data) throws IOException {
        XContentParser parser = null;
        try {
            Map<ShardId, ShardStateInfo> shardsState = Maps.newHashMap();

            parser = XContentHelper.createParser(data, 0, data.length);

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                // no data...
                return shardsState;
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("shards".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.START_OBJECT) {
                                String shardIndex = null;
                                int shardId = -1;
                                long version = -1;
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        currentFieldName = parser.currentName();
                                    } else if (token.isValue()) {
                                        if ("index".equals(currentFieldName)) {
                                            shardIndex = parser.text();
                                        } else if ("id".equals(currentFieldName)) {
                                            shardId = parser.intValue();
                                        } else if ("version".equals(currentFieldName)) {
                                            version = parser.longValue();
                                        }
                                    }
                                }
                                shardsState.put(new ShardId(shardIndex, shardId), new ShardStateInfo(version, null));
                            }
                        }
                    }
                }
            }
            return shardsState;
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
