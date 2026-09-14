/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutEntitlements;
import org.elasticsearch.xcontent.ToXContent;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;

@WithoutEntitlements // commands don't run with entitlements enforced
public class DetachClusterCommandTests extends ESTestCase {

    private Settings settings;
    private Path[] dataPaths;

    @Before
    public void createDataPaths() throws Exception {
        final Path dataPath = createTempDir();
        settings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), dataPath.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .build();

        try (var nodeEnvironment = newNodeEnvironment(settings)) {
            dataPaths = nodeEnvironment.nodeDataPaths();
        }
    }

    public void testDoesNotRenderClusterStateWhenNotVerbose() {
        final Metadata metadata = Metadata.builder().putCustom("throwing", new ThrowingCustomMetadataTest()).build();
        final ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        final MockTerminal terminal = MockTerminal.create();

        // Must not throw: at NORMAL verbosity the message is never built, so toString() is never called.
        DetachClusterCommand.printOldAndNewClusterStates(terminal, clusterState, clusterState);
    }

    public void testPrintsClusterStateWhenVerbose() {
        final Metadata metadata = Metadata.builder().putCustom("throwing", new ThrowingCustomMetadataTest()).build();
        final ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        final MockTerminal terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);

        // At VERBOSE, the message is built, so toString() is called and throws.
        expectThrows(AssertionError.class, () -> DetachClusterCommand.printOldAndNewClusterStates(terminal, clusterState, clusterState));
    }

    private void executeCommand(MockTerminal terminal, ClusterState clusterState) throws Exception {
        try (
            PersistedClusterStateService.Writer writer = new PersistedClusterStateService(
                dataPaths,
                randomAlphaOfLength(10),
                xContentRegistry(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L,
                () -> false
            ).createWriter()
        ) {
            writer.writeFullStateAndCommit(1L, clusterState);
        }

        try (DetachClusterCommand command = new DetachClusterCommand()) {
            command.processDataPaths(terminal, dataPaths, command.getParser().parse(), TestEnvironment.newEnvironment(settings));
        }
    }

    private static class ThrowingCustomMetadataTest implements Metadata.ClusterCustom {

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

        @Override
        public Diff<Metadata.ClusterCustom> diff(Metadata.ClusterCustom previousState) {
            return null;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public String toString() {
            throw new AssertionError("ClusterState.toString() was called");
        }
    }
}
