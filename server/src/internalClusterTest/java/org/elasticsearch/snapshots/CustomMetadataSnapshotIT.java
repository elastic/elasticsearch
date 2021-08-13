/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.TestCustomMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class CustomMetadataSnapshotIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestCustomMetadataPlugin.class);
    }

    public void testRestoreCustomMetadata() throws Exception {
        Path tempDir = randomRepoPath();

        logger.info("--> start node");
        internalCluster().startNode();
        createIndex("test-idx");
        logger.info("--> add custom persistent metadata");
        updateClusterState(currentState -> {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("before_snapshot_s"));
            metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("before_snapshot_ns"));
            metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("before_snapshot_s_gw"));
            metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("before_snapshot_ns_gw"));
            metadataBuilder.putCustom(
                SnapshotableGatewayNoApiMetadata.TYPE,
                new SnapshotableGatewayNoApiMetadata("before_snapshot_s_gw_noapi")
            );
            builder.metadata(metadataBuilder);
            return builder.build();
        });

        createRepository("test-repo", "fs", tempDir);
        createFullSnapshot("test-repo", "test-snap");
        assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> change custom persistent metadata");
        updateClusterState(currentState -> {
            ClusterState.Builder builder = ClusterState.builder(currentState);
            Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
            if (randomBoolean()) {
                metadataBuilder.putCustom(SnapshottableMetadata.TYPE, new SnapshottableMetadata("after_snapshot_s"));
            } else {
                metadataBuilder.removeCustom(SnapshottableMetadata.TYPE);
            }
            metadataBuilder.putCustom(NonSnapshottableMetadata.TYPE, new NonSnapshottableMetadata("after_snapshot_ns"));
            if (randomBoolean()) {
                metadataBuilder.putCustom(SnapshottableGatewayMetadata.TYPE, new SnapshottableGatewayMetadata("after_snapshot_s_gw"));
            } else {
                metadataBuilder.removeCustom(SnapshottableGatewayMetadata.TYPE);
            }
            metadataBuilder.putCustom(NonSnapshottableGatewayMetadata.TYPE, new NonSnapshottableGatewayMetadata("after_snapshot_ns_gw"));
            metadataBuilder.removeCustom(SnapshotableGatewayNoApiMetadata.TYPE);
            builder.metadata(metadataBuilder);
            return builder.build();
        });

        logger.info("--> delete repository");
        assertAcked(clusterAdmin().prepareDeleteRepository("test-repo"));

        createRepository("test-repo-2", "fs", tempDir);

        logger.info("--> restore snapshot");
        clusterAdmin().prepareRestoreSnapshot("test-repo-2", "test-snap")
            .setRestoreGlobalState(true)
            .setIndices("-*")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();

        logger.info("--> make sure old repository wasn't restored");
        assertRequestBuilderThrows(clusterAdmin().prepareGetRepositories("test-repo"), RepositoryMissingException.class);
        assertThat(clusterAdmin().prepareGetRepositories("test-repo-2").get().repositories().size(), equalTo(1));

        logger.info("--> check that custom persistent metadata was restored");
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        Metadata metadata = clusterState.getMetadata();
        assertThat(((SnapshottableMetadata) metadata.custom(SnapshottableMetadata.TYPE)).getData(), equalTo("before_snapshot_s"));
        assertThat(((NonSnapshottableMetadata) metadata.custom(NonSnapshottableMetadata.TYPE)).getData(), equalTo("after_snapshot_ns"));
        assertThat(
            ((SnapshottableGatewayMetadata) metadata.custom(SnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw")
        );
        assertThat(
            ((NonSnapshottableGatewayMetadata) metadata.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("after_snapshot_ns_gw")
        );

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> check that gateway-persistent custom metadata survived full cluster restart");
        clusterState = clusterAdmin().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);
        metadata = clusterState.getMetadata();
        assertThat(metadata.custom(SnapshottableMetadata.TYPE), nullValue());
        assertThat(metadata.custom(NonSnapshottableMetadata.TYPE), nullValue());
        assertThat(
            ((SnapshottableGatewayMetadata) metadata.custom(SnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw")
        );
        assertThat(
            ((NonSnapshottableGatewayMetadata) metadata.custom(NonSnapshottableGatewayMetadata.TYPE)).getData(),
            equalTo("after_snapshot_ns_gw")
        );
        // Shouldn't be returned as part of API response
        assertThat(metadata.custom(SnapshotableGatewayNoApiMetadata.TYPE), nullValue());
        // But should still be in state
        metadata = internalCluster().getInstance(ClusterService.class).state().metadata();
        assertThat(
            ((SnapshotableGatewayNoApiMetadata) metadata.custom(SnapshotableGatewayNoApiMetadata.TYPE)).getData(),
            equalTo("before_snapshot_s_gw_noapi")
        );
    }

    public static class TestCustomMetadataPlugin extends Plugin {

        private final List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>();
        private final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

        public TestCustomMetadataPlugin() {
            registerBuiltinWritables();
        }

        private <T extends Metadata.Custom> void registerMetadataCustom(
            String name,
            Writeable.Reader<T> reader,
            Writeable.Reader<NamedDiff<?>> diffReader,
            CheckedFunction<XContentParser, T, IOException> parser
        ) {
            namedWritables.add(new NamedWriteableRegistry.Entry(Metadata.Custom.class, name, reader));
            namedWritables.add(new NamedWriteableRegistry.Entry(NamedDiff.class, name, diffReader));
            namedXContents.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(name), parser));
        }

        private void registerBuiltinWritables() {
            registerMetadataCustom(
                SnapshottableMetadata.TYPE,
                SnapshottableMetadata::readFrom,
                SnapshottableMetadata::readDiffFrom,
                SnapshottableMetadata::fromXContent
            );
            registerMetadataCustom(
                NonSnapshottableMetadata.TYPE,
                NonSnapshottableMetadata::readFrom,
                NonSnapshottableMetadata::readDiffFrom,
                NonSnapshottableMetadata::fromXContent
            );
            registerMetadataCustom(
                SnapshottableGatewayMetadata.TYPE,
                SnapshottableGatewayMetadata::readFrom,
                SnapshottableGatewayMetadata::readDiffFrom,
                SnapshottableGatewayMetadata::fromXContent
            );
            registerMetadataCustom(
                NonSnapshottableGatewayMetadata.TYPE,
                NonSnapshottableGatewayMetadata::readFrom,
                NonSnapshottableGatewayMetadata::readDiffFrom,
                NonSnapshottableGatewayMetadata::fromXContent
            );
            registerMetadataCustom(
                SnapshotableGatewayNoApiMetadata.TYPE,
                SnapshotableGatewayNoApiMetadata::readFrom,
                NonSnapshottableGatewayMetadata::readDiffFrom,
                SnapshotableGatewayNoApiMetadata::fromXContent
            );
        }

        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return namedWritables;
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return namedXContents;
        }
    }

    private static class SnapshottableMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable";

        SnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshottableMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshottableMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static SnapshottableMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshottableMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_SNAPSHOT;
        }
    }

    private static class NonSnapshottableMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_non_snapshottable";

        NonSnapshottableMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static NonSnapshottableMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(NonSnapshottableMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static NonSnapshottableMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(NonSnapshottableMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_ONLY;
        }
    }

    private static class SnapshottableGatewayMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable_gateway";

        SnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshottableGatewayMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshottableGatewayMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static SnapshottableGatewayMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshottableGatewayMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.API, Metadata.XContentContext.SNAPSHOT, Metadata.XContentContext.GATEWAY);
        }
    }

    private static class NonSnapshottableGatewayMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_non_snapshottable_gateway";

        NonSnapshottableGatewayMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static NonSnapshottableGatewayMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(NonSnapshottableGatewayMetadata::new, in);
        }

        public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
            return readDiffFrom(TYPE, in);
        }

        public static NonSnapshottableGatewayMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(NonSnapshottableGatewayMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return Metadata.API_AND_GATEWAY;
        }

    }

    private static class SnapshotableGatewayNoApiMetadata extends TestCustomMetadata {
        public static final String TYPE = "test_snapshottable_gateway_no_api";

        SnapshotableGatewayNoApiMetadata(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        public static SnapshotableGatewayNoApiMetadata readFrom(StreamInput in) throws IOException {
            return readFrom(SnapshotableGatewayNoApiMetadata::new, in);
        }

        public static SnapshotableGatewayNoApiMetadata fromXContent(XContentParser parser) throws IOException {
            return fromXContent(SnapshotableGatewayNoApiMetadata::new, parser);
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT);
        }
    }
}
