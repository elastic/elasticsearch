/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DanglingIndicesStateTests extends ESTestCase {

    private static final Settings indexSettings = Settings.builder()
        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .build();

    public void testDanglingIndicesAreDiscovered() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());

            Metadata metadata = Metadata.builder().build();
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService, metadata);

            assertTrue(danglingState.getDanglingIndices().isEmpty());

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            Map<Index, IndexMetadata> newDanglingIndices = danglingState.getDanglingIndices();
            assertTrue(newDanglingIndices.containsKey(dangledIndex.getIndex()));
        }
    }

    public void testInvalidIndexFolder() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            Metadata metadata = Metadata.builder().build();

            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService, metadata);

            final String uuid = "test1UUID";
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, uuid);
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);
            Path path = env.resolveIndexFolder(uuid);
            if (Files.exists(path)) {
                Files.move(path, path.resolveSibling("invalidUUID"), StandardCopyOption.ATOMIC_MOVE);
            }

            final IllegalStateException e = expectThrows(IllegalStateException.class, danglingState::getDanglingIndices);
            assertThat(e.getMessage(), equalTo("[invalidUUID] invalid index folder name, rename to [test1UUID]"));
        }
    }

    public void testDanglingIndicesNotReportedWhenTombstonePresent() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(dangledIndex.getIndex()).build();
            final Metadata metadata = Metadata.builder().indexGraveyard(graveyard).build();

            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService, metadata);

            final Map<Index, IndexMetadata> newDanglingIndices = danglingState.getDanglingIndices();
            assertThat(newDanglingIndices, is(emptyMap()));
        }
    }

    public void testDanglingIndicesReportedWhenIndexNameIsAlreadyUsed() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());

            final Settings.Builder danglingSettings = Settings.builder()
                .put(indexSettings)
                .put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test_index").settings(danglingSettings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            // Build another index with the same name but a different UUID
            final Settings.Builder existingSettings = Settings.builder()
                .put(indexSettings)
                .put(IndexMetadata.SETTING_INDEX_UUID, "test2UUID");
            IndexMetadata existingIndex = IndexMetadata.builder("test_index").settings(existingSettings).build();
            metaStateService.writeIndex("test_write", existingIndex);

            final ImmutableOpenMap<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
                .fPut(dangledIndex.getIndex().getName(), existingIndex)
                .build();
            final Metadata metadata = Metadata.builder().indices(indices).build();

            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService, metadata);

            // All dangling indices should be found
            final Map<Index, IndexMetadata> newDanglingIndices = danglingState.getDanglingIndices();
            assertThat(newDanglingIndices, is(aMapWithSize(1)));
        }
    }

    public void testDanglingIndicesStripAliases() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1")
                .settings(settings)
                .putAlias(AliasMetadata.newAliasMetadataBuilder("test_aliasd").build())
                .build();
            metaStateService.writeIndex("test_write", dangledIndex);
            assertThat(dangledIndex.getAliases().size(), equalTo(1));

            final Metadata metadata = Metadata.builder().build();
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService, metadata);

            Map<Index, IndexMetadata> newDanglingIndices = danglingState.getDanglingIndices();
            assertThat(newDanglingIndices.size(), equalTo(1));
            Map.Entry<Index, IndexMetadata> entry = newDanglingIndices.entrySet().iterator().next();
            assertThat(entry.getKey().getName(), equalTo("test1"));
            assertThat(entry.getValue().getAliases().size(), equalTo(0));
        }
    }

    private DanglingIndicesState createDanglingIndicesState(MetaStateService metaStateService, Metadata metadata) {
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        return new DanglingIndicesState(metaStateService, clusterService);
    }
}
