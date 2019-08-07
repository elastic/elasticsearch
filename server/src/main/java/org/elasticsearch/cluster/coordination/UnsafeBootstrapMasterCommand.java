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
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Locale;

public class UnsafeBootstrapMasterCommand extends ElasticsearchNodeCommand {

    private static final Logger logger = LogManager.getLogger(UnsafeBootstrapMasterCommand.class);

    static final String CLUSTER_STATE_TERM_VERSION_MSG_FORMAT =
            "Current node cluster state (term, version) pair is (%s, %s)";
    static final String CONFIRMATION_MSG =
        DELIMITER +
            "\n" +
            "You should only run this tool if you have permanently lost half or more\n" +
            "of the master-eligible nodes in this cluster, and you cannot restore the\n" +
            "cluster from a snapshot. This tool can cause arbitrary data loss and its\n" +
            "use should be your last resort. If you have multiple surviving master\n" +
            "eligible nodes, you should run this tool on the node with the highest\n" +
            "cluster state (term, version) pair.\n" +
            "\n" +
            "Do you want to proceed?\n";

    static final String NOT_MASTER_NODE_MSG = "unsafe-bootstrap tool can only be run on master eligible node";

    static final String NO_NODE_METADATA_FOUND_MSG = "no node meta data is found, node has not been started yet?";

    static final String EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG =
            "last committed voting voting configuration is empty, cluster has never been bootstrapped?";

    static final String MASTER_NODE_BOOTSTRAPPED_MSG = "Master node was successfully bootstrapped";
    static final Setting<String> UNSAFE_BOOTSTRAP =
            ClusterService.USER_DEFINED_META_DATA.getConcreteSetting("cluster.metadata.unsafe-bootstrap");

    UnsafeBootstrapMasterCommand() {
        super("Forces the successful election of the current node after the permanent loss of the half or more master-eligible nodes");
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        terminal.println(Terminal.Verbosity.VERBOSE, "Checking node.master setting");
        Boolean master = Node.NODE_MASTER_SETTING.get(settings);
        if (master == false) {
            throw new ElasticsearchException(NOT_MASTER_NODE_MSG);
        }

        return true;
    }

    protected void processNodePaths(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading node metadata");
        final NodeMetaData nodeMetaData = NodeMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, dataPaths);
        if (nodeMetaData == null) {
            throw new ElasticsearchException(NO_NODE_METADATA_FOUND_MSG);
        }

        String nodeId = nodeMetaData.nodeId();
        terminal.println(Terminal.Verbosity.VERBOSE, "Current nodeId is " + nodeId);

        final Tuple<Manifest, MetaData> manifestMetaDataTuple = loadMetaData(terminal, dataPaths);
        final Manifest manifest = manifestMetaDataTuple.v1();
        final MetaData metaData = manifestMetaDataTuple.v2();
        final CoordinationMetaData coordinationMetaData = metaData.coordinationMetaData();
        if (coordinationMetaData == null ||
                coordinationMetaData.getLastCommittedConfiguration() == null ||
                coordinationMetaData.getLastCommittedConfiguration().isEmpty()) {
            throw new ElasticsearchException(EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
        }
        terminal.println(String.format(Locale.ROOT, CLUSTER_STATE_TERM_VERSION_MSG_FORMAT, coordinationMetaData.term(),
                metaData.version()));

        confirm(terminal, CONFIRMATION_MSG);

        CoordinationMetaData newCoordinationMetaData = CoordinationMetaData.builder(coordinationMetaData)
                .clearVotingConfigExclusions()
                .lastAcceptedConfiguration(new CoordinationMetaData.VotingConfiguration(Collections.singleton(nodeId)))
                .lastCommittedConfiguration(new CoordinationMetaData.VotingConfiguration(Collections.singleton(nodeId)))
                .build();

        Settings persistentSettings = Settings.builder()
                .put(metaData.persistentSettings())
                .put(UNSAFE_BOOTSTRAP.getKey(), true)
                .build();
        MetaData newMetaData = MetaData.builder(metaData)
                .clusterUUID(MetaData.UNKNOWN_CLUSTER_UUID)
                .generateClusterUuidIfNeeded()
                .clusterUUIDCommitted(true)
                .persistentSettings(persistentSettings)
                .coordinationMetaData(newCoordinationMetaData)
                .build();

        writeNewMetaData(terminal, manifest, manifest.getCurrentTerm(), metaData, newMetaData, dataPaths);

        terminal.println(MASTER_NODE_BOOTSTRAPPED_MSG);
    }
}
