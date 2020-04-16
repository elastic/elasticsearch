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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static org.elasticsearch.gateway.DanglingIndicesState.AUTO_IMPORT_DANGLING_INDICES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DanglingIndicesStateTests extends ESTestCase {

    private static Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    // The setting AUTO_IMPORT_DANGLING_INDICES_SETTING is deprecated, so we must disable
    // warning checks or all the tests will fail.
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testCleanupWhenEmpty() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            assertTrue(danglingState.getDanglingIndices().isEmpty());
            Metadata metadata = Metadata.builder().build();
            danglingState.cleanupAllocatedDangledIndices(metadata);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }

    public void testDanglingIndicesDiscovery() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
            Metadata metadata = Metadata.builder().build();
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);
            Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
            assertTrue(newDanglingIndices.containsKey(dangledIndex.getIndex()));
            metadata = Metadata.builder().put(dangledIndex, false).build();
            newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
            assertFalse(newDanglingIndices.containsKey(dangledIndex.getIndex()));
        }
    }

    public void testInvalidIndexFolder() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

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
            try {
                danglingState.findNewDanglingIndices(metadata);
                fail("no exception thrown for invalid folder name");
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), equalTo("[invalidUUID] invalid index folder name, rename to [test1UUID]"));
            }
        }
    }

    public void testDanglingProcessing() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            Metadata metadata = Metadata.builder().build();

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            // check that several runs when not in the metadata still keep the dangled index around
            int numberOfChecks = randomIntBetween(1, 10);
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
                assertThat(newDanglingIndices.size(), equalTo(1));
                assertThat(newDanglingIndices.keySet(), Matchers.hasItems(dangledIndex.getIndex()));
                assertTrue(danglingState.getDanglingIndices().isEmpty());
            }

            for (int i = 0; i < numberOfChecks; i++) {
                danglingState.findNewAndAddDanglingIndices(metadata);

                assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
                assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems(dangledIndex.getIndex()));
            }

            // simulate allocation to the metadata
            metadata = Metadata.builder(metadata).put(dangledIndex, true).build();

            // check that several runs when in the metadata, but not cleaned yet, still keeps dangled
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
                assertTrue(newDanglingIndices.isEmpty());

                assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
                assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems(dangledIndex.getIndex()));
            }

            danglingState.cleanupAllocatedDangledIndices(metadata);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }

    public void testDanglingIndicesNotImportedWhenTombstonePresent() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            final IndexGraveyard graveyard = IndexGraveyard.builder().addTombstone(dangledIndex.getIndex()).build();
            final Metadata metadata = Metadata.builder().indexGraveyard(graveyard).build();
            assertThat(danglingState.findNewDanglingIndices(metadata).size(), equalTo(0));
        }
    }

    public void testDanglingIndicesStripAliases() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            DanglingIndicesState danglingState = createDanglingIndicesState(env, metaStateService);

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1")
                .settings(settings)
                .putAlias(AliasMetadata.newAliasMetadataBuilder("test_aliasd").build())
                .build();
            metaStateService.writeIndex("test_write", dangledIndex);
            assertThat(dangledIndex.getAliases().size(), equalTo(1));

            final Metadata metadata = Metadata.builder().build();
            Map<Index, IndexMetadata> newDanglingIndices = danglingState.findNewDanglingIndices(metadata);
            assertThat(newDanglingIndices.size(), equalTo(1));
            Map.Entry<Index, IndexMetadata> entry = newDanglingIndices.entrySet().iterator().next();
            assertThat(entry.getKey().getName(), equalTo("test1"));
            assertThat(entry.getValue().getAliases().size(), equalTo(0));
        }
    }

    public void testDanglingIndicesAreNotAllocatedWhenDisabled() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            LocalAllocateDangledIndices localAllocateDangledIndices = mock(LocalAllocateDangledIndices.class);

            final Settings allocateSettings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), false).build();

            final ClusterService clusterServiceMock = mock(ClusterService.class);
            when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

            final DanglingIndicesState danglingIndicesState = new DanglingIndicesState(
                env,
                metaStateService,
                localAllocateDangledIndices,
                clusterServiceMock
            );

            assertFalse("Expected dangling imports to be disabled", danglingIndicesState.isAutoImportDanglingIndicesEnabled());
        }
    }

    public void testDanglingIndicesAreAllocatedWhenEnabled() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(env, xContentRegistry());
            LocalAllocateDangledIndices localAllocateDangledIndices = mock(LocalAllocateDangledIndices.class);
            final Settings allocateSettings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true).build();

            final ClusterService clusterServiceMock = mock(ClusterService.class);
            when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

            DanglingIndicesState danglingIndicesState = new DanglingIndicesState(
                env,
                metaStateService,
                localAllocateDangledIndices, clusterServiceMock
            );

            assertTrue("Expected dangling imports to be enabled", danglingIndicesState.isAutoImportDanglingIndicesEnabled());

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test1UUID");
            IndexMetadata dangledIndex = IndexMetadata.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", dangledIndex);

            danglingIndicesState.findNewAndAddDanglingIndices(Metadata.builder().build());

            danglingIndicesState.allocateDanglingIndices();

            verify(localAllocateDangledIndices).allocateDangled(any(), any());
        }
    }

    private DanglingIndicesState createDanglingIndicesState(NodeEnvironment env, MetaStateService metaStateService) {
        final Settings allocateSettings = Settings.builder().put(AUTO_IMPORT_DANGLING_INDICES_SETTING.getKey(), true).build();

        final ClusterService clusterServiceMock = mock(ClusterService.class);
        when(clusterServiceMock.getSettings()).thenReturn(allocateSettings);

        return new DanglingIndicesState(env, metaStateService, null, clusterServiceMock);
    }
}
