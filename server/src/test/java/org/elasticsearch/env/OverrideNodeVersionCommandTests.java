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
import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OverrideNodeVersionCommandTests extends ESTestCase {

    private Environment environment;
    private Path nodePath;
    private String nodeId;
    private final OptionSet noOptions = new OptionParser().parse();

    @Before
    public void createNodePaths() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(settings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            nodePath = nodeEnvironment.nodeDataPath();
            nodeId = nodeEnvironment.nodeId();

            try (PersistedClusterStateService.Writer writer = new PersistedClusterStateService(nodePath, nodeId,
                xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L).createWriter()) {
                writer.writeFullStateAndCommit(1L, ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
                    .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build()).build())
                    .build());
            }
        }
    }

    @After
    public void checkClusterStateIntact() throws IOException {
        assertTrue(Metadata.SETTING_READ_ONLY_SETTING.get(new PersistedClusterStateService(nodePath, nodeId,
            xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), () -> 0L)
            .loadOnDiskState().metadata.persistentSettings()));
    }

    public void testFailsOnEmptyPath() {
        final Path emptyPath = createTempDir();
        final MockTerminal mockTerminal = new MockTerminal();
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, emptyPath, noOptions, environment));
        assertThat(elasticsearchException.getMessage(), equalTo(OverrideNodeVersionCommand.NO_METADATA_MESSAGE));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testFailsIfUnnecessary() throws IOException {
        final Version nodeVersion = Version.fromId(between(Version.CURRENT.minimumIndexCompatibilityVersion().id, Version.CURRENT.id));
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePath);
        final MockTerminal mockTerminal = new MockTerminal();
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePath, noOptions, environment));
        assertThat(elasticsearchException.getMessage(), allOf(
            containsString("compatible with current version"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testWarnsIfTooOld() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePath);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput("n\n");
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePath, noOptions, environment));
        assertThat(elasticsearchException.getMessage(), equalTo("aborted by user"));
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePath);
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testWarnsIfTooNew() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePath);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
        final ElasticsearchException elasticsearchException = expectThrows(ElasticsearchException.class, () ->
            new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePath, noOptions, environment));
        assertThat(elasticsearchException.getMessage(), equalTo("aborted by user"));
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString())));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePath);
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testOverwritesIfTooOld() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooOldVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePath);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePath, noOptions, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("too old"),
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePath);
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
    }

    public void testOverwritesIfTooNew() throws Exception {
        final Version nodeVersion = NodeMetadataTests.tooNewVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, nodePath);
        final MockTerminal mockTerminal = new MockTerminal();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processNodePaths(mockTerminal, nodePath, noOptions, environment);
        assertThat(mockTerminal.getOutput(), allOf(
            containsString("data loss"),
            containsString("You should not use this tool"),
            containsString(Version.CURRENT.toString()),
            containsString(nodeVersion.toString()),
            containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePath);
        assertThat(nodeMetadata.nodeVersion(), equalTo(Version.CURRENT));
    }
}
