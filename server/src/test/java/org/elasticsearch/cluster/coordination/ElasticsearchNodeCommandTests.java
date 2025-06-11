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
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestClusterCustomMetadata;
import org.elasticsearch.test.TestProjectCustomMetadata;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ElasticsearchNodeCommandTests extends ESTestCase {

    public void testLoadStateWithoutMissingCustomsButPreserved() throws IOException {
        runLoadStateTest(false);
    }

    public void testLoadStateWithMissingCustomsButPreserved() throws IOException {
        runLoadStateTest(true);
    }

    @Override
    public Settings buildEnvSettings(Settings settings) {
        // we control the data path in the tests, so we don't need to set it here
        return settings;
    }

    private void runLoadStateTest(boolean hasMissingCustoms) throws IOException {
        final var dataPath = createTempDir();
        final Settings settings = Settings.builder()
            .putList(Environment.PATH_DATA_SETTING.getKey(), List.of(dataPath.toString()))
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .build();

        try (var nodeEnvironment = newNodeEnvironment(settings)) {
            final var persistedClusterStateServiceWithFullRegistry = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L,
                () -> false
            );

            // 1. Simulating persisting cluster state by a running node
            final long initialTerm = randomNonNegativeLong();
            final Metadata initialMetadata = randomMeta(hasMissingCustoms);
            final ClusterState initialState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(initialMetadata).build();
            try (var writer = persistedClusterStateServiceWithFullRegistry.createWriter()) {
                writer.writeFullStateAndCommit(initialTerm, initialState);
            }

            // 2. Simulating loading the persisted cluster state by the CLI
            final var persistedClusterStateServiceForNodeCommand = ElasticsearchNodeCommand.createPersistedClusterStateService(
                Settings.EMPTY,
                new Path[] { dataPath }
            );
            final Tuple<Long, ClusterState> loadedTermAndClusterState = ElasticsearchNodeCommand.loadTermAndClusterState(
                persistedClusterStateServiceForNodeCommand,
                TestEnvironment.newEnvironment(settings)
            );

            assertThat(loadedTermAndClusterState.v1(), equalTo(initialTerm));
            final var loadedMetadata = loadedTermAndClusterState.v2().metadata();
            assertNotNull(loadedMetadata.getProject().custom(IndexGraveyard.TYPE));
            assertThat(
                loadedMetadata.getProject().custom(IndexGraveyard.TYPE),
                instanceOf(ElasticsearchNodeCommand.UnknownProjectCustom.class)
            );
            if (hasMissingCustoms) {
                assertThat(
                    loadedMetadata.getProject().custom(MissingProjectCustomMetadata.TYPE),
                    instanceOf(ElasticsearchNodeCommand.UnknownProjectCustom.class)
                );
                assertThat(
                    loadedMetadata.custom(MissingClusterCustomMetadata.TYPE),
                    instanceOf(ElasticsearchNodeCommand.UnknownClusterCustom.class)
                );
            } else {
                assertNull(loadedMetadata.getProject().custom(MissingProjectCustomMetadata.TYPE));
                assertNull(loadedMetadata.custom(MissingClusterCustomMetadata.TYPE));
            }

            final long newTerm = initialTerm + 1;
            try (var writer = persistedClusterStateServiceForNodeCommand.createWriter()) {
                writer.writeFullStateAndCommit(newTerm, ClusterState.builder(ClusterState.EMPTY_STATE).metadata(loadedMetadata).build());
            }

            // 3. Simulate node restart after updating on-disk state with the CLI tool
            final var bestOnDiskState = persistedClusterStateServiceWithFullRegistry.loadBestOnDiskState();
            assertThat(bestOnDiskState.currentTerm, equalTo(newTerm));
            final Metadata reloadedMetadata = bestOnDiskState.metadata;
            assertThat(reloadedMetadata.getProject().indexGraveyard(), equalTo(initialMetadata.getProject().indexGraveyard()));
            if (hasMissingCustoms) {
                assertThat(
                    reloadedMetadata.getProject().custom(MissingProjectCustomMetadata.TYPE),
                    equalTo(initialMetadata.getProject().custom(MissingProjectCustomMetadata.TYPE))
                );
            } else {
                assertNull(reloadedMetadata.getProject().custom(MissingProjectCustomMetadata.TYPE));
            }
        }
    }

    private Metadata randomMeta(boolean hasMissingCustoms) {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.generateClusterUuidIfNeeded();
        int numDelIndices = randomIntBetween(0, 5);
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (int i = 0; i < numDelIndices; i++) {
            graveyard.addTombstone(new Index(randomAlphaOfLength(10) + "del-idx-" + i, UUIDs.randomBase64UUID()));
        }
        if (randomBoolean()) {
            int numDataStreams = randomIntBetween(0, 5);
            for (int i = 0; i < numDataStreams; i++) {
                String dataStreamName = "name" + 1;
                IndexMetadata backingIndex = createFirstBackingIndex(dataStreamName).build();
                mdBuilder.put(newInstance(dataStreamName, List.of(backingIndex.getIndex())));
            }
        }
        mdBuilder.indexGraveyard(graveyard.build());
        if (hasMissingCustoms) {
            mdBuilder.putCustom(
                MissingProjectCustomMetadata.TYPE,
                new MissingProjectCustomMetadata("test missing project custom metadata")
            );
            mdBuilder.putCustom(
                MissingClusterCustomMetadata.TYPE,
                new MissingClusterCustomMetadata("test missing cluster custom metadata")
            );
        }
        return mdBuilder.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Stream.of(
                ClusterModule.getNamedXWriteables().stream(),
                IndicesModule.getNamedXContents().stream(),
                Stream.of(
                    new NamedXContentRegistry.Entry(
                        Metadata.ProjectCustom.class,
                        new ParseField(MissingProjectCustomMetadata.TYPE),
                        parser -> MissingProjectCustomMetadata.fromXContent(MissingProjectCustomMetadata::new, parser)
                    ),
                    new NamedXContentRegistry.Entry(
                        Metadata.ClusterCustom.class,
                        new ParseField(MissingClusterCustomMetadata.TYPE),
                        parser -> MissingClusterCustomMetadata.fromXContent(MissingClusterCustomMetadata::new, parser)
                    )
                )
            ).flatMap(Function.identity()).toList()
        );
    }

    private static class MissingProjectCustomMetadata extends TestProjectCustomMetadata {

        static final String TYPE = "missing_project_custom_metadata";

        MissingProjectCustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class MissingClusterCustomMetadata extends TestClusterCustomMetadata {

        static final String TYPE = "missing_cluster_custom_metadata";

        MissingClusterCustomMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }
}
