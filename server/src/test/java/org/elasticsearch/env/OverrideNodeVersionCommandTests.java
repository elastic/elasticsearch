/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.env;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.elasticsearch.Version;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
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
    private Path[] dataPaths;
    private String nodeId;
    private final OptionSet noOptions = new OptionParser().parse();

    @Before
    public void createDataPaths() throws IOException {
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        environment = TestEnvironment.newEnvironment(settings);
        try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            dataPaths = nodeEnvironment.nodeDataPaths();
            nodeId = nodeEnvironment.nodeId();

            try (
                PersistedClusterStateService.Writer writer = new PersistedClusterStateService(
                    dataPaths,
                    nodeId,
                    xContentRegistry(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L,
                    ESTestCase::randomBoolean
                ).createWriter()
            ) {
                writer.writeFullStateAndCommit(
                    1L,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .metadata(
                            Metadata.builder()
                                .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
                                .build()
                        )
                        .build()
                );
            }
        }
    }

    @After
    public void checkClusterStateIntact() throws IOException {
        assertTrue(
            Metadata.SETTING_READ_ONLY_SETTING.get(
                new PersistedClusterStateService(
                    dataPaths,
                    nodeId,
                    xContentRegistry(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L,
                    ESTestCase::randomBoolean
                ).loadBestOnDiskState().metadata.persistentSettings()
            )
        );
    }

    public void testFailsOnEmptyPath() {
        final Path emptyPath = createTempDir();
        final MockTerminal mockTerminal = MockTerminal.create();
        final ElasticsearchException elasticsearchException = expectThrows(
            ElasticsearchException.class,
            () -> new OverrideNodeVersionCommand().processDataPaths(mockTerminal, new Path[] { emptyPath }, noOptions, environment)
        );
        assertThat(elasticsearchException.getMessage(), equalTo(OverrideNodeVersionCommand.NO_METADATA_MESSAGE));
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testFailsIfUnnecessary() throws IOException {
        final BuildVersion nodeVersion = BuildVersion.fromVersionId(
            between(Version.CURRENT.minimumCompatibilityVersion().id, Version.CURRENT.id)
        );
        PersistedClusterStateService.overrideVersion(nodeVersion, dataPaths);
        final MockTerminal mockTerminal = MockTerminal.create();
        final ElasticsearchException elasticsearchException = expectThrows(
            ElasticsearchException.class,
            () -> new OverrideNodeVersionCommand().processDataPaths(mockTerminal, dataPaths, noOptions, environment)
        );
        assertThat(
            elasticsearchException.getMessage(),
            allOf(
                containsString("compatible with current version"),
                containsString(BuildVersion.current().toString()),
                containsString(nodeVersion.toString())
            )
        );
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));
    }

    public void testWarnsIfTooOld() throws Exception {
        final BuildVersion nodeVersion = NodeMetadataTests.tooOldBuildVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, dataPaths);
        final MockTerminal mockTerminal = MockTerminal.create();
        mockTerminal.addTextInput("n");
        final ElasticsearchException elasticsearchException = expectThrows(
            ElasticsearchException.class,
            () -> new OverrideNodeVersionCommand().processDataPaths(mockTerminal, dataPaths, noOptions, environment)
        );
        assertThat(elasticsearchException.getMessage(), equalTo("aborted by user"));
        assertThat(
            mockTerminal.getOutput(),
            allOf(
                containsString("too old"),
                containsString("data loss"),
                containsString("You should not use this tool"),
                containsString(Version.CURRENT.toString()),
                containsString(nodeVersion.toString())
            )
        );
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testWarnsIfTooNew() throws Exception {
        final BuildVersion nodeVersion = NodeMetadataTests.tooNewBuildVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, dataPaths);
        final MockTerminal mockTerminal = MockTerminal.create();
        mockTerminal.addTextInput(randomFrom("yy", "Yy", "n", "yes", "true", "N", "no"));
        final ElasticsearchException elasticsearchException = expectThrows(
            ElasticsearchException.class,
            () -> new OverrideNodeVersionCommand().processDataPaths(mockTerminal, dataPaths, noOptions, environment)
        );
        assertThat(elasticsearchException.getMessage(), equalTo("aborted by user"));
        assertThat(
            mockTerminal.getOutput(),
            allOf(
                containsString("data loss"),
                containsString("You should not use this tool"),
                containsString(Version.CURRENT.toString()),
                containsString(nodeVersion.toString())
            )
        );
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(nodeVersion));
    }

    public void testOverwritesIfTooOld() throws Exception {
        final BuildVersion nodeVersion = NodeMetadataTests.tooOldBuildVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, dataPaths);
        final MockTerminal mockTerminal = MockTerminal.create();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processDataPaths(mockTerminal, dataPaths, noOptions, environment);
        assertThat(
            mockTerminal.getOutput(),
            allOf(
                containsString("too old"),
                containsString("data loss"),
                containsString("You should not use this tool"),
                containsString(Version.CURRENT.toString()),
                containsString(nodeVersion.toString()),
                containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)
            )
        );
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(BuildVersion.current()));
    }

    public void testOverwritesIfTooNew() throws Exception {
        final BuildVersion nodeVersion = NodeMetadataTests.tooNewBuildVersion();
        PersistedClusterStateService.overrideVersion(nodeVersion, dataPaths);
        final MockTerminal mockTerminal = MockTerminal.create();
        mockTerminal.addTextInput(randomFrom("y", "Y"));
        new OverrideNodeVersionCommand().processDataPaths(mockTerminal, dataPaths, noOptions, environment);
        assertThat(
            mockTerminal.getOutput(),
            allOf(
                containsString("data loss"),
                containsString("You should not use this tool"),
                containsString(Version.CURRENT.toString()),
                containsString(nodeVersion.toString()),
                containsString(OverrideNodeVersionCommand.SUCCESS_MESSAGE)
            )
        );
        expectThrows(IllegalStateException.class, () -> mockTerminal.readText(""));

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        assertThat(nodeMetadata.nodeVersion(), equalTo(BuildVersion.current()));
    }
}
