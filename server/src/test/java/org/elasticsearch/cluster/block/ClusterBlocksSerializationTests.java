/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.block;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ClusterBlocksSerializationTests extends AbstractWireSerializingTestCase<
    ClusterBlocksSerializationTests.ClusterBlocksTestWrapper> {

    @Override
    protected Writeable.Reader<ClusterBlocksTestWrapper> instanceReader() {
        return ClusterBlocksTestWrapper::new;
    }

    @Override
    protected ClusterBlocksTestWrapper createTestInstance() {
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        if (randomBoolean()) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        if (randomBoolean()) {
            builder.addIndexBlock(projectId, randomIdentifier(), randomIndexBlock());
        }
        if (randomBoolean()) {
            builder.addIndexBlock(projectId, randomIdentifier(), randomIndexBlock());
        }
        if (randomBoolean()) {
            builder.addIndexBlock(randomUniqueProjectId(), randomIdentifier(), randomIndexBlock());
        }
        return new ClusterBlocksTestWrapper(builder.build());
    }

    @Override
    protected ClusterBlocksTestWrapper mutateInstance(ClusterBlocksTestWrapper instance) throws IOException {
        final ClusterBlocks clusterBlocks = instance.clusterBlocks();
        final var builder = ClusterBlocks.builder(clusterBlocks);
        return switch (between(0, 2)) {
            case 0 -> {
                final Set<ClusterBlock> globalBlocks = clusterBlocks.global();
                if (globalBlocks.isEmpty()) {
                    builder.addGlobalBlock(randomGlobalBlock());
                } else {
                    globalBlocks.forEach(builder::removeGlobalBlock);
                    builder.addGlobalBlock(randomValueOtherThanMany(globalBlocks::contains, this::randomGlobalBlock));
                }
                yield new ClusterBlocksTestWrapper(builder.build());
            }
            case 1 -> {
                if (clusterBlocks.noIndexBlockAllProjects()) {
                    builder.addIndexBlock(randomProjectIdOrDefault(), randomIdentifier(), randomIndexBlock());
                } else {
                    if (randomBoolean()) {
                        final ProjectId projectId = clusterBlocks.projectBlocksMap.keySet().iterator().next();
                        builder.addIndexBlock(projectId, randomIdentifier(), randomIndexBlock());
                        if (randomBoolean()) {
                            builder.addIndexBlock(projectId, randomIdentifier(), randomIndexBlock());
                        }
                    } else {
                        final ProjectId projectId = clusterBlocks.projectBlocksMap.keySet()
                            .stream()
                            .filter(pid -> clusterBlocks.indices(pid).isEmpty() == false)
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("All projects have empty indicesBlock"));
                        final Map<String, Set<ClusterBlock>> indicesBlocks = clusterBlocks.indices(projectId);
                        indicesBlocks.entrySet()
                            .stream()
                            .findAny()
                            .map(entry -> builder.removeIndexBlock(projectId, entry.getKey(), entry.getValue().iterator().next()))
                            .orElseThrow(() -> new AssertionError("indicesBlock is empty"));
                    }
                }
                yield new ClusterBlocksTestWrapper(builder.build());
            }
            case 2 -> {
                builder.addIndexBlock(randomUniqueProjectId(), randomIdentifier(), randomIndexBlock());
                yield new ClusterBlocksTestWrapper(builder.build());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        };
    }

    public void testWriteToBwc() throws IOException {
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        if (randomBoolean()) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        final String indexName = randomIdentifier();
        final ClusterBlock block = randomIndexBlock();
        builder.addIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        if (randomBoolean()) {
            builder.removeIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        }
        final ClusterBlocks clusterBlocks = builder.build();

        final var out = new BytesStreamOutput();
        final TransportVersion bwcVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        out.setTransportVersion(bwcVersion);
        clusterBlocks.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(bwcVersion);
        final ClusterBlocks deserialized = ClusterBlocks.readFrom(in);
        assertThat(new ClusterBlocksTestWrapper(deserialized), equalTo(new ClusterBlocksTestWrapper(clusterBlocks)));
    }

    public void testWriteToBwcFailure() {
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        if (randomBoolean()) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        final String indexName = randomIdentifier();
        final ClusterBlock block = randomIndexBlock();
        builder.addIndexBlock(randomUniqueProjectId(), indexName, block);
        if (randomBoolean()) {
            builder.addIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        }
        final ClusterBlocks clusterBlocks = builder.build();

        final var out = new BytesStreamOutput();
        final TransportVersion bwcVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        out.setTransportVersion(bwcVersion);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> clusterBlocks.writeTo(out));
        assertThat(e.getMessage(), containsString("Cannot write multi-project blocks to a stream with version"));
    }

    public void testDiff() throws IOException {
        final ClusterBlocks base = ClusterBlocks.builder()
            .addIndexBlock(Metadata.DEFAULT_PROJECT_ID, randomIdentifier(), randomIndexBlock())
            .build();

        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        if (randomBoolean()) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        final String indexName = randomIdentifier();
        final ClusterBlock block = randomIndexBlock();
        builder.addIndexBlock(randomUniqueProjectId(), indexName, block);
        if (randomBoolean()) {
            builder.addIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        }
        final ClusterBlocks current = builder.build();

        final var diff = current.diff(base);
        final var out = new BytesStreamOutput();
        diff.writeTo(out);
        final ClusterBlocks reconstructed = ClusterBlocks.readDiffFrom(out.bytes().streamInput()).apply(base);
        assertThat(new ClusterBlocksTestWrapper(reconstructed), equalTo(new ClusterBlocksTestWrapper(current)));
    }

    public void testDiffBwc() throws IOException {
        final ClusterBlocks base = ClusterBlocks.builder()
            .addIndexBlock(Metadata.DEFAULT_PROJECT_ID, randomIdentifier(), randomIndexBlock())
            .build();

        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        if (randomBoolean()) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        final String indexName = randomIdentifier();
        final ClusterBlock block = randomIndexBlock();
        builder.addIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        if (randomBoolean()) {
            builder.removeIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        }
        final ClusterBlocks current = builder.build();

        final var diff = current.diff(base);
        final var out = new BytesStreamOutput();
        final TransportVersion bwcVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        out.setTransportVersion(bwcVersion);
        diff.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(bwcVersion);
        final ClusterBlocks reconstructed = ClusterBlocks.readDiffFrom(in).apply(base);
        assertThat(new ClusterBlocksTestWrapper(reconstructed), equalTo(new ClusterBlocksTestWrapper(current)));
    }

    public void testDiffBwcFailure() {
        final ClusterBlocks base = ClusterBlocks.builder()
            .addIndexBlock(Metadata.DEFAULT_PROJECT_ID, randomIdentifier(), randomIndexBlock())
            .build();

        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        if (randomBoolean()) {
            builder.addGlobalBlock(randomGlobalBlock());
        }
        final String indexName = randomIdentifier();
        final ClusterBlock block = randomIndexBlock();
        builder.addIndexBlock(randomUniqueProjectId(), indexName, block);
        if (randomBoolean()) {
            builder.addIndexBlock(Metadata.DEFAULT_PROJECT_ID, indexName, block);
        }
        final ClusterBlocks current = builder.build();

        final var diff = current.diff(base);
        final var out = new BytesStreamOutput();
        final TransportVersion bwcVersion = TransportVersionUtils.getPreviousVersion(TransportVersions.MULTI_PROJECT);
        out.setTransportVersion(bwcVersion);
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> diff.writeTo(out));
        assertThat(e.getMessage(), containsString("Cannot write multi-project blocks diff to a stream with version"));
    }

    private ClusterBlock randomGlobalBlock() {
        return randomFrom(
            GatewayService.STATE_NOT_RECOVERED_BLOCK,
            NoMasterBlockService.NO_MASTER_BLOCK_ALL,
            Metadata.CLUSTER_READ_ONLY_BLOCK,
            Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK
        );
    }

    private ClusterBlock randomIndexBlock() {
        return randomFrom(
            IndexMetadata.INDEX_METADATA_BLOCK,
            IndexMetadata.INDEX_READ_BLOCK,
            IndexMetadata.INDEX_WRITE_BLOCK,
            IndexMetadata.INDEX_REFRESH_BLOCK,
            IndexMetadata.INDEX_READ_ONLY_BLOCK,
            IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
        );
    }

    static class ClusterBlocksTestWrapper implements Writeable {

        private final ClusterBlocks clusterBlocks;

        ClusterBlocksTestWrapper(ClusterBlocks clusterBlocks) {
            this.clusterBlocks = clusterBlocks;
        }

        ClusterBlocksTestWrapper(StreamInput in) throws IOException {
            this.clusterBlocks = ClusterBlocks.readFrom(in);
        }

        ClusterBlocks clusterBlocks() {
            return clusterBlocks;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ClusterBlocksTestWrapper that = (ClusterBlocksTestWrapper) o;
            return clusterBlocks.global().equals(that.clusterBlocks.global())
                && indicesBlocksAllProjects().equals(that.indicesBlocksAllProjects());
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterBlocks.global(), indicesBlocksAllProjects());
        }

        @Override
        public String toString() {
            return "ClusterBlocksWrapper{" + "clusterBlocks=" + clusterBlocks + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterBlocks.writeTo(out);
        }

        private Map<ProjectId, Map<String, Set<ClusterBlock>>> indicesBlocksAllProjects() {
            return clusterBlocks.projectBlocksMap.keySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(Function.identity(), clusterBlocks::indices));
        }
    }
}
