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
package org.elasticsearch.gateway;

import org.elasticsearch.Version;
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
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService);
            Metadata metadata = Metadata.builder().build();

            assertTrue(danglingState.findDanglingIndices(metadata).isEmpty());

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            Map<Index, IndexMetadata> newDanglingIndices = danglingState.findDanglingIndices(metadata);
            assertTrue(newDanglingIndices.containsKey(dangledIndex.getIndex()));
        }
    }

    public void testInvalidIndexFolder() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService);

            Metadata metadata = Metadata.builder().build();
            final String uuid = "test1UUID";
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, uuid);
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);
            for (Path path : env.resolveIndexFolder(uuid)) {
                if (Files.exists(path)) {
                    Files.move(path, path.resolveSibling("invalidUUID"), StandardCopyOption.ATOMIC_MOVE);
                }
            }

            final IllegalStateException e = expectThrows(IllegalStateException.class, () -> danglingState.findDanglingIndices(metadata));
            assertThat(e.getMessage(), equalTo("[invalidUUID] invalid index folder name, rename to [test1UUID]"));
        }
    }

    public void testDanglingIndicesNotReportedWhenTombstonePresent() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService);

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(dangledIndex.getIndex()).build();
            final Metadata metadata = Metadata.builder().indexGraveyard(graveyard).build();

            final Map<Index, IndexMetadata> newDanglingIndices = danglingState.findDanglingIndices(metadata);
            assertThat(newDanglingIndices, is(emptyMap()));
        }
    }

    public void testDanglingIndicesReportedWhenIndexNameIsAlreadyUsed() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService);

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

            // All dangling indices should be found
            final Map<Index, IndexMetadata> newDanglingIndices = danglingState.findDanglingIndices(metadata);
            assertThat(newDanglingIndices, is(aMapWithSize(1)));
        }
    }

    public void testDanglingIndicesStripAliases() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(metaStateService);

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1")
                .settings(settings)
                .putAlias(AliasMetadata.newAliasMetadataBuilder("test_aliasd").build())
                .build();
            metaStateService.writeIndex("test_write", dangledIndex);
            assertThat(dangledIndex.getAliases().size(), equalTo(1));

            final Metadata metadata = Metadata.builder().build();
            Map<Index, IndexMetadata> newDanglingIndices = danglingState.findDanglingIndices(metadata);
            assertThat(newDanglingIndices.size(), equalTo(1));
            Map.Entry<Index, IndexMetadata> entry = newDanglingIndices.entrySet().iterator().next();
            assertThat(entry.getKey().getName(), equalTo("test1"));
            assertThat(entry.getValue().getAliases().size(), equalTo(0));
        }
    }

    private DanglingIndicesState createDanglingIndicesState(MetaStateService metaStateService) {
        final Settings allocateSettings = Settings.builder().build();

        final ClusterService clusterServiceMock = mock(ClusterService.class);
        when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

        return new DanglingIndicesState(metaStateService, clusterServiceMock);
    }
}
