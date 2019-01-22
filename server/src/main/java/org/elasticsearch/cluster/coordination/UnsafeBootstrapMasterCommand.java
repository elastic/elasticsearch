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

import joptsimple.OptionSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

public class UnsafeBootstrapMasterCommand extends EnvironmentAwareCommand {

    private final NamedXContentRegistry namedXContentRegistry;

    static final String WARNING_MSG =
            "-----------------------------------------------------------------------\n" +
            "\n" +
            "    WARNING: Elasticsearch MUST be stopped before running this tool.\n" +
            "\n" +
            "You should run this tool only if you've lost the majority of master eligible nodes.\n" +
            "If you have a backup, restore from the backup instead.\n" +
            "Running this tool should be the last resort.\n" +
            "Running this tool may cause arbitrary data loss and may render your cluster completely non functional.\n" +
            "Do you accept this risk?\n";
    static final String ABORTED_BY_USER_MSG = "aborted by user";
    static final String NOT_MASTER_NODE_MSG = "unsafe-bootstrap tool can only be run on master eligible node";
    static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = "failed to lock node's directory, is Elasticsearch still running?";
    static final String NO_NODE_FOLDER_FOUND_MSG = "no node folder is found in data folder(s), node has not been started yet?";
    static final String NO_NODE_METADATA_FOUND_MSG = "no node meta data is found, node has not been started yet?";
    static final String NO_MANIFEST_FILE_FOUND_MSG = "no manifest file is found, do you run pre 7.0 Elasticsearch?";
    static final String GLOBAL_GENERATION_MISSING_MSG = "no metadata is referenced from the manifest file, cluster has never been " +
            "bootstrapped?";
    static final String NO_GLOBAL_METADATA_MSG = "failed to find global metadata, metadata corrupted?";
    static final String EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG =
            "last committed voting voting configuration is empty, cluster has never been bootstrapped?";
    static final String WRITE_METADATA_EXCEPTION_MSG = "exception occurred when writing new metadata to disk";
    static final String MASTER_NODE_BOOTSTRAPPED_MSG = "Master node was successfully bootstrapped";
    static final Setting<String> UNSAFE_BOOTSTRAP =
            ClusterService.USER_DEFINED_META_DATA.getConcreteSetting("cluster.metadata.unsafe-bootstrap");

    UnsafeBootstrapMasterCommand() {
        super("Unsafely bootstraps the master node if the majority of master eligible nodes is lost");
        namedXContentRegistry = new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        // We don't use classical "private static final Logger", because log4j2 initialization happens
        // after UnsafeBootstrapMasterCommand instance creation.
        final Logger logger = LogManager.getLogger(UnsafeBootstrapMasterCommand.class);
        showWarning(terminal);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException(ABORTED_BY_USER_MSG);
        }

        Settings settings = env.settings();
        terminal.println(Terminal.Verbosity.VERBOSE, "Checking node.master setting");
        Boolean master = Node.NODE_MASTER_SETTING.get(settings);
        if (master == false) {
            throw new ElasticsearchException(NOT_MASTER_NODE_MSG);
        }
        final int nodeOrdinal = 0;

        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining lock for node");

        try (NodeEnvironment.NodeLock lock = new NodeEnvironment.NodeLock(nodeOrdinal, logger, env, Files::exists)) {
            processNodePaths(logger, terminal, lock.getNodePaths());
        } catch (LockObtainFailedException ex) {
            throw new ElasticsearchException(
                    FAILED_TO_OBTAIN_NODE_LOCK_MSG + " [" + ex.getMessage() + "]");
        }

        terminal.println(MASTER_NODE_BOOTSTRAPPED_MSG);
    }

    private void processNodePaths(Logger logger, Terminal terminal, NodeEnvironment.NodePath[] nodePaths) throws IOException {
        final Path[] dataPaths =
                Arrays.stream(nodePaths).filter(Objects::nonNull).map(p -> p.path).toArray(Path[]::new);
        if (dataPaths.length == 0) {
            throw new ElasticsearchException(NO_NODE_FOLDER_FOUND_MSG);
        }

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading node metadata");
        final NodeMetaData nodeMetaData = NodeMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, dataPaths);
        if (nodeMetaData == null) {
            throw new ElasticsearchException(NO_NODE_METADATA_FOUND_MSG);
        }

        String nodeId = nodeMetaData.nodeId();
        terminal.println(Terminal.Verbosity.VERBOSE, "Current nodeId is " + nodeId);
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading manifest file");
        final Manifest manifest = Manifest.FORMAT.loadLatestState(logger, namedXContentRegistry, dataPaths);

        if (manifest == null) {
            throw new ElasticsearchException(NO_MANIFEST_FILE_FOUND_MSG);
        }
        if (manifest.isGlobalGenerationMissing()) {
            throw new ElasticsearchException(GLOBAL_GENERATION_MISSING_MSG);
        }
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading global metadata file");
        final MetaData metaData = MetaData.FORMAT.loadGeneration(logger, namedXContentRegistry, manifest.getGlobalGeneration(),
                dataPaths);
        if (metaData == null) {
            throw new ElasticsearchException(NO_GLOBAL_METADATA_MSG + " [generation = " + manifest.getGlobalGeneration() + "]");
        }
        final CoordinationMetaData coordinationMetaData = metaData.coordinationMetaData();
        if (coordinationMetaData == null ||
                coordinationMetaData.getLastCommittedConfiguration() == null ||
                coordinationMetaData.getLastCommittedConfiguration().isEmpty()) {
            throw new ElasticsearchException(EMPTY_LAST_COMMITTED_VOTING_CONFIG_MSG);
        }

        CoordinationMetaData newCoordinationMetaData = CoordinationMetaData.builder(coordinationMetaData)
                .clearVotingConfigExclusions()
                .lastAcceptedConfiguration(new CoordinationMetaData.VotingConfiguration(Collections.singleton(nodeId)))
                .lastCommittedConfiguration(new CoordinationMetaData.VotingConfiguration(Collections.singleton(nodeId)))
                .build();
        terminal.println(Terminal.Verbosity.VERBOSE, "New coordination metadata is constructed " + newCoordinationMetaData);
        Settings persistentSettings = Settings.builder()
                .put(metaData.persistentSettings())
                .put(UNSAFE_BOOTSTRAP.getKey(), true)
                .build();
        MetaData newMetaData = MetaData.builder(metaData)
                .persistentSettings(persistentSettings)
                .coordinationMetaData(newCoordinationMetaData)
                .build();
        writeNewMetaData(terminal, manifest, newMetaData, dataPaths);
    }

    private void writeNewMetaData(Terminal terminal, Manifest manifest, MetaData newMetaData, Path[] dataPaths) {
        try {
            terminal.println(Terminal.Verbosity.VERBOSE, "Writing new global metadata to disk");
            long newGeneration = MetaData.FORMAT.write(newMetaData, dataPaths);
            Manifest newManifest = new Manifest(manifest.getCurrentTerm(), manifest.getClusterStateVersion(), newGeneration,
                    manifest.getIndexGenerations());
            terminal.println(Terminal.Verbosity.VERBOSE, "Writing new manifest file to disk");
            Manifest.FORMAT.writeAndCleanup(newManifest, dataPaths);
            terminal.println(Terminal.Verbosity.VERBOSE, "Cleaning up old metadata");
            MetaData.FORMAT.cleanupOldFiles(newGeneration, dataPaths);
        } catch (Exception e) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Cleaning up new metadata");
            MetaData.FORMAT.cleanupOldFiles(manifest.getGlobalGeneration(), dataPaths);
            throw new ElasticsearchException(WRITE_METADATA_EXCEPTION_MSG, e);
        }
    }

    private void showWarning(Terminal terminal) {
        terminal.println(WARNING_MSG);
    }
}
