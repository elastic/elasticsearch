/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.env;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.env.NodeEnvironment.INDICES_FOLDER;

public class NodeRepurposeCommand extends ElasticsearchNodeCommand {

    static final String ABORTED_BY_USER_MSG = ElasticsearchNodeCommand.ABORTED_BY_USER_MSG;
    static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = ElasticsearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG;
    static final String NO_CLEANUP = "Node has node.data=true -> no clean up necessary";
    static final String NO_DATA_TO_CLEAN_UP_FOUND = "No data to clean-up found";
    static final String NO_SHARD_DATA_TO_CLEAN_UP_FOUND = "No shard data to clean-up found";

    public NodeRepurposeCommand() {
        super("Repurpose this node to another master/data role, cleaning up any excess persisted data");
    }

    void testExecute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        execute(terminal, options, env, null);
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        if (DiscoveryNode.canContainData(settings)) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_CLEANUP);
            return false;
        }

        return true;
    }

    @Override
    protected void processDataPaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env) throws IOException {
        assert DiscoveryNode.canContainData(env.settings()) == false;

        if (DiscoveryNode.isMasterNode(env.settings()) == false) {
            processNoMasterNoDataNode(terminal, dataPaths, env);
        } else {
            processMasterNoDataNode(terminal, dataPaths, env);
        }
    }

    private static void processNoMasterNoDataNode(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        NodeEnvironment.DataPath[] nodeDataPaths = toDataPaths(dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting shard data paths");
        List<Path> shardDataPaths = NodeEnvironment.collectShardDataPaths(nodeDataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting index metadata paths");
        List<Path> indexMetadataPaths = NodeEnvironment.collectIndexMetadataPaths(nodeDataPaths);

        Set<Path> indexPaths = uniqueParentPaths(shardDataPaths, indexMetadataPaths);

        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        final Metadata metadata = loadClusterState(terminal, env, persistedClusterStateService).metadata();
        if (indexPaths.isEmpty() && metadata.indices().isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_DATA_TO_CLEAN_UP_FOUND);
            return;
        }

        final Set<String> indexUUIDs = Sets.union(
            indexUUIDsFor(indexPaths),
            metadata.indices().values().stream().map(IndexMetadata::getIndexUUID).collect(Collectors.toSet())
        );

        outputVerboseInformation(terminal, indexPaths, indexUUIDs, metadata);

        terminal.println(noMasterMessage(indexUUIDs.size(), shardDataPaths.size(), indexMetadataPaths.size()));
        outputHowToSeeVerboseInformation(terminal);

        terminal.println("Node is being re-purposed as no-master and no-data. Clean-up of index data will be performed.");
        confirm(terminal, "Do you want to proceed?");

        removePaths(terminal, indexPaths); // clean-up shard dirs
        // clean-up all metadata dirs
        MetadataStateFormat.deleteMetaState(dataPaths);
        IOUtils.rm(Stream.of(dataPaths).map(path -> path.resolve(INDICES_FOLDER)).toArray(Path[]::new));

        terminal.println("Node successfully repurposed to no-master and no-data.");
    }

    private static void processMasterNoDataNode(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        NodeEnvironment.DataPath[] nodeDataPaths = toDataPaths(dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting shard data paths");
        List<Path> shardDataPaths = NodeEnvironment.collectShardDataPaths(nodeDataPaths);
        if (shardDataPaths.isEmpty()) {
            terminal.println(NO_SHARD_DATA_TO_CLEAN_UP_FOUND);
            return;
        }

        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        final Metadata metadata = loadClusterState(terminal, env, persistedClusterStateService).metadata();

        final Set<Path> indexPaths = uniqueParentPaths(shardDataPaths);
        final Set<String> indexUUIDs = indexUUIDsFor(indexPaths);

        outputVerboseInformation(terminal, shardDataPaths, indexUUIDs, metadata);

        terminal.println(shardMessage(shardDataPaths.size(), indexUUIDs.size()));
        outputHowToSeeVerboseInformation(terminal);

        terminal.println("Node is being re-purposed as master and no-data. Clean-up of shard data will be performed.");
        confirm(terminal, "Do you want to proceed?");

        removePaths(terminal, shardDataPaths); // clean-up shard dirs

        terminal.println("Node successfully repurposed to master and no-data.");
    }

    private static ClusterState loadClusterState(Terminal terminal, Environment env, PersistedClusterStateService psf) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        return clusterState(env, psf.loadBestOnDiskState());
    }

    private static void outputVerboseInformation(
        Terminal terminal,
        Collection<Path> pathsToCleanup,
        Set<String> indexUUIDs,
        Metadata metadata
    ) {
        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE)) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Paths to clean up:");
            pathsToCleanup.forEach(p -> terminal.println(Terminal.Verbosity.VERBOSE, "  " + p.toString()));
            terminal.println(Terminal.Verbosity.VERBOSE, "Indices affected:");
            indexUUIDs.forEach(uuid -> terminal.println(Terminal.Verbosity.VERBOSE, "  " + toIndexName(uuid, metadata)));
        }
    }

    private static void outputHowToSeeVerboseInformation(Terminal terminal) {
        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE) == false) {
            terminal.println("Use -v to see list of paths and indices affected");
        }
    }

    private static String toIndexName(String uuid, Metadata metadata) {
        if (metadata != null) {
            for (Map.Entry<String, IndexMetadata> indexMetadata : metadata.indices().entrySet()) {
                if (indexMetadata.getValue().getIndexUUID().equals(uuid)) {
                    return indexMetadata.getValue().getIndex().getName();
                }
            }
        }
        return "no name for uuid: " + uuid;
    }

    private static Set<String> indexUUIDsFor(Set<Path> indexPaths) {
        return indexPaths.stream().map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
    }

    static String noMasterMessage(int indexes, int shards, int indexMetadata) {
        return "Found " + indexes + " indices (" + shards + " shards and " + indexMetadata + " index meta data) to clean up";
    }

    static String shardMessage(int shards, int indices) {
        return "Found " + shards + " shards in " + indices + " indices to clean up";
    }

    private static void removePaths(Terminal terminal, Collection<Path> paths) {
        terminal.println(Terminal.Verbosity.VERBOSE, "Removing data");
        paths.forEach(NodeRepurposeCommand::removePath);
    }

    private static void removePath(Path path) {
        try {
            IOUtils.rm(path);
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to clean up path: " + path + ": " + e.getMessage());
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static Set<Path> uniqueParentPaths(Collection<Path>... paths) {
        // equals on Path is good enough here due to the way these are collected.
        return Arrays.stream(paths).flatMap(Collection::stream).map(Path::getParent).collect(Collectors.toSet());
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }
}
