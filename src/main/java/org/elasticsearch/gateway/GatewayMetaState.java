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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.SimpleHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.MultiDataPathUpgrader;
import org.elasticsearch.env.NodeEnvironment;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 *
 */
public class GatewayMetaState extends AbstractComponent implements ClusterStateListener {

    private static final String DEPRECATED_SETTING_ROUTING_HASH_FUNCTION = "cluster.routing.operation.hash.type";
    private static final String DEPRECATED_SETTING_ROUTING_USE_TYPE = "cluster.routing.operation.use_type";

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final DanglingIndicesState danglingIndicesState;

    @Nullable
    private volatile MetaData currentMetaData;

    @Inject
    public GatewayMetaState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                            DanglingIndicesState danglingIndicesState, TransportNodesListGatewayMetaState nodesListGatewayMetaState) throws Exception {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.danglingIndicesState = danglingIndicesState;
        nodesListGatewayMetaState.init(this);

        if (DiscoveryNode.dataNode(settings)) {
            ensureNoPre019ShardState(nodeEnv);
            MultiDataPathUpgrader.upgradeMultiDataPath(nodeEnv, logger);
        }

        if (DiscoveryNode.masterNode(settings) || DiscoveryNode.dataNode(settings)) {
            nodeEnv.ensureAtomicMoveSupported();
        }
        if (DiscoveryNode.masterNode(settings)) {
            try {
                ensureNoPre019State();
                pre20Upgrade();
                long start = System.currentTimeMillis();
                metaStateService.loadFullState();
                logger.debug("took {} to load state", TimeValue.timeValueMillis(System.currentTimeMillis() - start));
            } catch (Exception e) {
                logger.error("failed to read local state, exiting...", e);
                throw e;
            }
        }
    }

    public MetaData loadMetaState() throws Exception {
        return metaStateService.loadFullState();
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
                    metaStateService.writeGlobalState("changed", newMetaData);
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
                        currentIndexMetaData = metaStateService.loadIndexState(indexMetaData.index());
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
                    metaStateService.writeIndex(writeReason, indexMetaData, currentIndexMetaData);
                } catch (Throwable e) {
                    success = false;
                }
            }
        }

        danglingIndicesState.processDanglingIndices(newMetaData);

        if (success) {
            currentMetaData = newMetaData;
        }
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
                metaStateService.writeIndex("upgrade", newMetaData, null);
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


    // shard state BWC
    private void ensureNoPre019ShardState(NodeEnvironment nodeEnv) throws Exception {
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
