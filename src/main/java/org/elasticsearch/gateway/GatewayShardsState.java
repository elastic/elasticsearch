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

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 */
public class GatewayShardsState extends AbstractComponent implements ClusterStateListener {

    private static final String SHARD_STATE_FILE_PREFIX = "state-";
    private static final Pattern SHARD_STATE_FILE_PATTERN = Pattern.compile(SHARD_STATE_FILE_PREFIX + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    private static final String PRIMARY_KEY = "primary";
    private static final String VERSION_KEY = "version";

    private final NodeEnvironment nodeEnv;

    private volatile Map<ShardId, ShardStateInfo> currentState = Maps.newHashMap();

    @Inject
    public GatewayShardsState(Settings settings, NodeEnvironment nodeEnv, TransportNodesListGatewayStartedShards listGatewayStartedShards) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        if (listGatewayStartedShards != null) { // for testing
            listGatewayStartedShards.initGateway(this);
        }
        if (DiscoveryNode.dataNode(settings)) {
            try {
                ensureNoPre019State();
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
        final ClusterState state = event.state();
        if (state.blocks().disableStatePersistence() == false
            && state.nodes().localNode().dataNode()
            && event.routingTableChanged()) {
            // now, add all the ones that are active and on this node
            RoutingNode routingNode = state.readOnlyRoutingNodes().node(state.nodes().localNodeId());
            final Map<ShardId, ShardStateInfo> newState;
            if (routingNode != null) {
               newState = persistRoutingNodeState(routingNode);
            } else {
               newState = Maps.newHashMap();
            }

            // preserve all shards that:
            //   * are not already in the new map AND
            //   * belong to an active index AND
            //   * used to be on this node but are not yet completely stated on any other node
            // since these shards are NOT active on this node the won't need to be written above - we just preserve these
            // in this map until they are fully started anywhere else or are re-assigned and we need to update the state
            final RoutingTable indexRoutingTables = state.routingTable();
            for (Map.Entry<ShardId, ShardStateInfo> entry : this.currentState.entrySet()) {
                ShardId shardId = entry.getKey();
                if (newState.containsKey(shardId) == false) { // this shard used to be here
                    String indexName = shardId.index().getName();
                    if (state.metaData().hasIndex(indexName)) { // it's index is not deleted
                        IndexRoutingTable index = indexRoutingTables.index(indexName);
                        if (index != null && index.shard(shardId.id()).allShardsStarted() == false) {
                           // not all shards are active on another node so we put it back until they are active
                           newState.put(shardId, entry.getValue());
                        }
                    }
                }
            }
            this.currentState = newState;
        }
    }

    Map<ShardId, ShardStateInfo> persistRoutingNodeState(RoutingNode routingNode) {
        final Map<ShardId, ShardStateInfo> newState = Maps.newHashMap();
        for (MutableShardRouting shardRouting : routingNode) {
            if (shardRouting.active()) {
                ShardId shardId = shardRouting.shardId();
                ShardStateInfo shardStateInfo = new ShardStateInfo(shardRouting.version(), shardRouting.primary());
                final ShardStateInfo previous = currentState.get(shardId);
                if(maybeWriteShardState(shardId, shardStateInfo, previous) ) {
                    newState.put(shardId, shardStateInfo);
                } else if (previous != null) {
                    currentState.put(shardId, previous);
                }
            }
        }
        return newState;
    }

    Map<ShardId, ShardStateInfo> getCurrentState() {
        return currentState;
    }

    boolean maybeWriteShardState(ShardId shardId, ShardStateInfo shardStateInfo, ShardStateInfo previousState) {
        final String writeReason;
        if (previousState == null) {
            writeReason = "freshly started, version [" + shardStateInfo.version + "]";
        } else if (previousState.version < shardStateInfo.version) {
            writeReason = "version changed from [" + previousState.version + "] to [" + shardStateInfo.version + "]";
        } else {
            logger.trace("skip writing shard state - has been written before shardID: " + shardId + " previous version:  [" + previousState.version + "] current version [" + shardStateInfo.version + "]");
            assert previousState.version <= shardStateInfo.version : "version should not go backwards for shardID: " + shardId + " previous version:  [" + previousState.version + "] current version [" + shardStateInfo.version + "]";
            return previousState.version == shardStateInfo.version;
        }

        try {
            writeShardState(writeReason, shardId, shardStateInfo, previousState);
        } catch (Exception e) {
            logger.warn("failed to write shard state for shard " + shardId, e);
            // we failed to write the shard state, we will try and write
            // it next time...
        }
        return true;
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

    private ShardStateInfo loadShardStateInfo(ShardId shardId) throws IOException {
        return MetaDataStateFormat.loadLatestState(logger, newShardStateInfoFormat(false), SHARD_STATE_FILE_PATTERN, shardId.toString(), nodeEnv.shardPaths(shardId));
    }

    private void writeShardState(String reason, ShardId shardId, ShardStateInfo shardStateInfo, @Nullable ShardStateInfo previousStateInfo) throws Exception {
        logger.trace("{} writing shard state, reason [{}]", shardId, reason);
        final boolean deleteOldFiles = previousStateInfo != null && previousStateInfo.version != shardStateInfo.version;
        newShardStateInfoFormat(deleteOldFiles).write(shardStateInfo, SHARD_STATE_FILE_PREFIX, shardStateInfo.version, nodeEnv.shardPaths(shardId));
    }

    private MetaDataStateFormat<ShardStateInfo> newShardStateInfoFormat(boolean deleteOldFiles) {
        return new MetaDataStateFormat<ShardStateInfo>(XContentType.JSON, deleteOldFiles) {

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

    private void ensureNoPre019State() throws Exception {
        for (Path dataLocation : nodeEnv.nodeDataPaths()) {
            final Path stateLocation = dataLocation.resolve(MetaDataStateFormat.STATE_DIR_NAME);
            if (Files.exists(stateLocation)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateLocation, "shards-*")) {
                    for (Path stateFile : stream) {
                        throw new ElasticsearchIllegalStateException("Detected pre 0.19 shard state file please upgrade to a version before "
                                + Version.CURRENT.minimumCompatibilityVersion()
                                + " first to upgrade state structures - shard state found: [" + stateFile.getParent().toAbsolutePath());
                    }
                }
            }
        }
    }
}
