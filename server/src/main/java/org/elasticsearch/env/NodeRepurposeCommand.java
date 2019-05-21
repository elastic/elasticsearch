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
package org.elasticsearch.env;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.gateway.WriteStateException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NodeRepurposeCommand extends ElasticsearchNodeCommand {

    private static final Logger logger = LogManager.getLogger(NodeRepurposeCommand.class);

    static final String ABORTED_BY_USER_MSG = ElasticsearchNodeCommand.ABORTED_BY_USER_MSG;
    static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = ElasticsearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG;
    static final String NO_CLEANUP = "Node has node.data=true -> no clean up necessary";
    static final String NO_DATA_TO_CLEAN_UP_FOUND = "No data to clean-up found";
    static final String NO_SHARD_DATA_TO_CLEAN_UP_FOUND = "No shard data to clean-up found";
    static final String PRE_V7_MESSAGE =
        "No manifest file found. If you were previously running this node on Elasticsearch version 6, please proceed.\n" +
            "If this node was ever started on Elasticsearch version 7 or higher, it might mean metadata corruption, please abort.";

    public NodeRepurposeCommand() {
        super("Repurpose this node to another master/data role, cleaning up any excess persisted data");
    }

    void testExecute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        execute(terminal, options, env);
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        if (DiscoveryNode.isDataNode(settings)) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_CLEANUP);
            return false;
        }

        return true;
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        assert DiscoveryNode.isDataNode(env.settings()) == false;

        if (DiscoveryNode.isMasterNode(env.settings()) == false) {
            processNoMasterNoDataNode(terminal, dataPaths);
        } else {
            processMasterNoDataNode(terminal, dataPaths);
        }
    }

    private void processNoMasterNoDataNode(Terminal terminal, Path[] dataPaths) throws IOException {
        NodeEnvironment.NodePath[] nodePaths = toNodePaths(dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting shard data paths");
        List<Path> shardDataPaths = NodeEnvironment.collectShardDataPaths(nodePaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting index metadata paths");
        List<Path> indexMetaDataPaths = NodeEnvironment.collectIndexMetaDataPaths(nodePaths);

        Set<Path> indexPaths = uniqueParentPaths(shardDataPaths, indexMetaDataPaths);
        if (indexPaths.isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_DATA_TO_CLEAN_UP_FOUND);
            return;
        }

        Set<String> indexUUIDs = indexUUIDsFor(indexPaths);
        outputVerboseInformation(terminal, nodePaths, indexPaths, indexUUIDs);

        terminal.println(noMasterMessage(indexUUIDs.size(), shardDataPaths.size(), indexMetaDataPaths.size()));
        outputHowToSeeVerboseInformation(terminal);

        final Manifest manifest = loadManifest(terminal, dataPaths);

        terminal.println("Node is being re-purposed as no-master and no-data. Clean-up of index data will be performed.");
        confirm(terminal, "Do you want to proceed?");

        if (manifest != null) {
            rewriteManifest(terminal, manifest, dataPaths);
        }

        removePaths(terminal, indexPaths);

        terminal.println("Node successfully repurposed to no-master and no-data.");
    }

    private void processMasterNoDataNode(Terminal terminal, Path[] dataPaths) throws IOException {
        NodeEnvironment.NodePath[] nodePaths = toNodePaths(dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting shard data paths");
        List<Path> shardDataPaths = NodeEnvironment.collectShardDataPaths(nodePaths);
        if (shardDataPaths.isEmpty()) {
            terminal.println(NO_SHARD_DATA_TO_CLEAN_UP_FOUND);
            return;
        }

        Set<Path> indexPaths = uniqueParentPaths(shardDataPaths);
        Set<String> indexUUIDs = indexUUIDsFor(indexPaths);
        outputVerboseInformation(terminal, nodePaths, shardDataPaths, indexUUIDs);

        terminal.println(shardMessage(shardDataPaths.size(), indexUUIDs.size()));
        outputHowToSeeVerboseInformation(terminal);

        terminal.println("Node is being re-purposed as master and no-data. Clean-up of shard data will be performed.");
        confirm(terminal, "Do you want to proceed?");

        removePaths(terminal, shardDataPaths);

        terminal.println("Node successfully repurposed to master and no-data.");
    }

    private void outputVerboseInformation(Terminal terminal, NodeEnvironment.NodePath[] nodePaths,
                                          Collection<Path> pathsToCleanup, Set<String> indexUUIDs) {
        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE)) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Paths to clean up:");
            pathsToCleanup.forEach(p -> terminal.println(Terminal.Verbosity.VERBOSE, "  " + p.toString()));
            terminal.println(Terminal.Verbosity.VERBOSE, "Indices affected:");
            indexUUIDs.forEach(uuid -> terminal.println(Terminal.Verbosity.VERBOSE, "  " + toIndexName(nodePaths, uuid)));
        }
    }

    private void outputHowToSeeVerboseInformation(Terminal terminal) {
        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE) == false) {
            terminal.println("Use -v to see list of paths and indices affected");
        }
    }
    private String toIndexName(NodeEnvironment.NodePath[] nodePaths, String uuid) {
        Path[] indexPaths = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            indexPaths[i] = nodePaths[i].resolve(uuid);
        }
        try {
            IndexMetaData metaData = IndexMetaData.FORMAT.loadLatestState(logger, namedXContentRegistry, indexPaths);
            return metaData.getIndex().getName();
        } catch (Exception e) {
            return "no name for uuid: " + uuid + ": " + e;
        }
    }

    private Set<String> indexUUIDsFor(Set<Path> indexPaths) {
        return indexPaths.stream().map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
    }

    static String noMasterMessage(int indexes, int shards, int indexMetaData) {
        return "Found " + indexes + " indices ("
                + shards + " shards and " + indexMetaData + " index meta data) to clean up";
    }

    static String shardMessage(int shards, int indices) {
        return "Found " + shards + " shards in " + indices + " indices to clean up";
    }

    private void rewriteManifest(Terminal terminal, Manifest manifest, Path[] dataPaths) throws WriteStateException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Re-writing manifest");
        Manifest newManifest = new Manifest(manifest.getCurrentTerm(), manifest.getClusterStateVersion(), manifest.getGlobalGeneration(),
            new HashMap<>());
        Manifest.FORMAT.writeAndCleanup(newManifest, dataPaths);
    }

    private Manifest loadManifest(Terminal terminal, Path[] dataPaths) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading manifest");
        final Manifest manifest = Manifest.FORMAT.loadLatestState(logger, namedXContentRegistry, dataPaths);

        if (manifest == null) {
            terminal.println(Terminal.Verbosity.SILENT, PRE_V7_MESSAGE);
        }
        return manifest;
    }

    private void removePaths(Terminal terminal, Collection<Path> paths) {
        terminal.println(Terminal.Verbosity.VERBOSE, "Removing data");
        paths.forEach(this::removePath);
    }

    private void removePath(Path path) {
        try {
            IOUtils.rm(path);
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to clean up path: " + path + ": " + e.getMessage());
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private Set<Path> uniqueParentPaths(Collection<Path>... paths) {
        // equals on Path is good enough here due to the way these are collected.
        return Arrays.stream(paths).flatMap(Collection::stream).map(Path::getParent).collect(Collectors.toSet());
    }

    //package-private for testing
    OptionParser getParser() {
        return parser;
    }
}
