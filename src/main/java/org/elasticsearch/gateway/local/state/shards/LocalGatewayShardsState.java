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

package org.elasticsearch.gateway.local.state.shards;

import com.google.common.collect.Maps;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.local.state.meta.CorruptStateException;
import org.elasticsearch.gateway.local.state.meta.MetaDataStateFormat;
import org.elasticsearch.index.shard.ShardId;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 */
public class LocalGatewayShardsState extends AbstractComponent implements ClusterStateListener {

    private static final String SHARD_STATE_FILE_PREFIX = "state-";
    private static final String PRIMARY_KEY = "primary";
    private static final String VERSION_KEY = "version";

    private final NodeEnvironment nodeEnv;

    private volatile Map<ShardId, ShardStateInfo> currentState = Maps.newHashMap();
    public static final MetaDataStateFormat<ShardStateInfo> FORMAT = newShardStateInfoFormat();
    @Inject
    public LocalGatewayShardsState(Settings settings, NodeEnvironment nodeEnv, TransportNodesListGatewayStartedShards listGatewayStartedShards) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
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
        return FORMAT.loadLatestState(logger, nodeEnv.shardLocations(shardId));
    }

    private void writeShardState(String reason, ShardId shardId, ShardStateInfo shardStateInfo, @Nullable ShardStateInfo previousStateInfo) throws Exception {
        logger.trace("{} writing shard state, reason [{}]", shardId, reason);
        FORMAT.write(shardStateInfo, shardStateInfo.version, nodeEnv.shardLocations(shardId));
    }

    private static MetaDataStateFormat<ShardStateInfo> newShardStateInfoFormat() {
        return new MetaDataStateFormat<ShardStateInfo>(XContentType.JSON, SHARD_STATE_FILE_PREFIX) {

            @Override
            protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
                XContentBuilder xContentBuilder = super.newXContentBuilder(type, stream);
                xContentBuilder.prettyPrint();
                return xContentBuilder;
            }

            @Override
            public void toXContent(XContentBuilder builder, ShardStateInfo shardStateInfo) throws IOException {
                builder.field(VERSION_KEY, shardStateInfo.version);
                if (shardStateInfo.primary != null) {
                    builder.field(PRIMARY_KEY, shardStateInfo.primary);
                }
            }

            @Override
            public ShardStateInfo fromXContent(XContentParser parser) throws IOException {
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
                        if (VERSION_KEY.equals(currentFieldName)) {
                            version = parser.longValue();
                        } else if (PRIMARY_KEY.equals(currentFieldName)) {
                            primary = parser.booleanValue();
                        } else {
                            throw new CorruptStateException("unexpected field in shard state [" + currentFieldName + "]");
                        }
                    } else {
                        throw new CorruptStateException("unexpected token in shard state [" + token.name() + "]");
                    }
                }
                if (primary == null) {
                    throw new CorruptStateException("missing value for [primary] in shard state");
                }
                if (version == -1) {
                    throw new CorruptStateException("missing value for [version] in shard state");
                }
                return new ShardStateInfo(version, primary);
            }
        };
    }

    private void pre019Upgrade() throws Exception {
        long index = -1;
        File latest = null;
        for (File dataLocation : nodeEnv.nodeDataLocations()) {
            File stateLocation = new File(dataLocation, "_state");
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
        final Map<ShardId, ShardStateInfo> shardsState = Maps.newHashMap();
        try (XContentParser parser = XContentHelper.createParser(data, 0, data.length)) {
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
                                        } else if (VERSION_KEY.equals(currentFieldName)) {
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
        }
    }
}
