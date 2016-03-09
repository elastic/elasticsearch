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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.PersistedIndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DanglingIndicesStateTests extends ESTestCase {
    private static Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    public void testCleanupWhenEmpty() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null);

            assertTrue(danglingState.getDanglingIndices().isEmpty());
            MetaData metaData = MetaData.builder().build();
            danglingState.cleanupAllocatedDangledIndices(metaData);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }
    public void testDanglingIndicesDiscovery() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null);

            assertTrue(danglingState.getDanglingIndices().isEmpty());
            MetaData metaData = MetaData.builder().build();
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_INDEX_UUID, "test1UUID");
            IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", new PersistedIndexMetaData(dangledIndex, Strings.randomBase64UUID()));
            Map<Index, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData).v1();
            assertTrue(newDanglingIndices.containsKey(dangledIndex.getIndex()));
            metaData = MetaData.builder().put(dangledIndex, false).build();
            newDanglingIndices = danglingState.findNewDanglingIndices(metaData).v1();
            assertFalse(newDanglingIndices.containsKey(dangledIndex.getIndex()));
        }
    }

    public void testInvalidIndexFolder() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null);

            MetaData metaData = MetaData.builder().build();
            final String uuid = "test1UUID";
            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_INDEX_UUID, uuid);
            IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(settings).build();
            metaStateService.writeIndex("test_write", new PersistedIndexMetaData(dangledIndex, Strings.randomBase64UUID()));
            for (Path path : env.resolveIndexFolder(uuid)) {
                if (Files.exists(path)) {
                    Files.move(path, path.resolveSibling("invalidUUID"), StandardCopyOption.ATOMIC_MOVE);
                }
            }
            try {
                danglingState.findNewDanglingIndices(metaData);
                fail("no exception thrown for invalid folder name");
            } catch (IllegalStateException e) {
                assertThat(e.getMessage(), equalTo("[invalidUUID] invalid index folder name, rename to [test1UUID]"));
            }
        }
    }

    public void testDanglingProcessing() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null);

            MetaData metaData = MetaData.builder().clusterUUID(Strings.randomBase64UUID()).build();

            final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_INDEX_UUID, "test1UUID");
            IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(settings).build();
            // a different cluster UUID, so the dangling index imports
            PersistedIndexMetaData persistedDangledIndex = new PersistedIndexMetaData(dangledIndex, Strings.randomBase64UUID());
            metaStateService.writeIndex("test_write", persistedDangledIndex);

            // check that several runs when not in the metadata still keep the dangled index around
            int numberOfChecks = randomIntBetween(1, 10);
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData).v1();
                assertThat(newDanglingIndices.size(), equalTo(1));
                assertThat(newDanglingIndices.keySet(), Matchers.hasItems(dangledIndex.getIndex()));
                assertTrue(danglingState.getDanglingIndices().isEmpty());
            }

            for (int i = 0; i < numberOfChecks; i++) {
                danglingState.findNewAndAddDanglingIndices(metaData);

                assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
                assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems(dangledIndex.getIndex()));
            }

            // simulate allocation to the metadata
            metaData = MetaData.builder(metaData).put(dangledIndex, true).build();

            // check that several runs when in the metadata, but not cleaned yet, still keeps dangled
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData).v1();
                assertTrue(newDanglingIndices.isEmpty());

                assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
                assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems(dangledIndex.getIndex()));
            }

            danglingState.cleanupAllocatedDangledIndices(metaData);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }

    public void testDanglingProcessingNoImport() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null);

            final String clusterUUID = Strings.randomBase64UUID();
            final String indexName = "test1";
            MetaData metaData = MetaData.builder().clusterUUID(clusterUUID).build();

            IndexMetaData dangledIndex = IndexMetaData.builder(indexName).settings(indexSettings).build();
            metaStateService.writeIndex("test_write", new PersistedIndexMetaData(dangledIndex, clusterUUID));

            // check that for several runs, no dangling indices are imported because of the same cluster UUID,
            // and that deleted indices are not empty (so they can be cleaned up)
            int numberOfChecks = randomIntBetween(1, 10);
            for (int i = 0; i < numberOfChecks; i++) {
                Tuple<Map<Index, IndexMetaData>, Map<Index, IndexMetaData>> tuple = danglingState.findNewDanglingIndices(metaData);
                Map<Index, IndexMetaData> newDanglingIndices = tuple.v1();
                Map<Index, IndexMetaData> deletedIndices = tuple.v2();
                assertThat(newDanglingIndices.size(), equalTo(0));
                assertTrue(danglingState.getDanglingIndices().isEmpty());
                assertThat(deletedIndices.size(), equalTo(1));
                assertThat(deletedIndices.keySet().stream().map(index -> index.getName()).collect(Collectors.toList()),
                           Matchers.hasItems(indexName));
            }

            for (int i = 0; i < numberOfChecks; i++) {
                danglingState.findNewAndAddDanglingIndices(metaData);
                assertThat(danglingState.getDanglingIndices().size(), equalTo(0));
                assertThat(danglingState.getDeletedIndices().size(), equalTo(1));
                assertThat(danglingState.getDeletedIndices().keySet().stream().map(index -> index.getName()).collect(Collectors.toList()),
                           Matchers.hasItems(indexName));
            }

            // check that deleted indices exist, then get deleted, then don't exist in the data structure nor on disk
            assertThat(danglingState.getDeletedIndices().size(), equalTo(1));
            danglingState.cleanDeletedIndices();
            assertThat(danglingState.getDeletedIndices().size(), equalTo(0));
            for (Path path : env.indexPaths(dangledIndex.getIndex())) {
                assertFalse(Files.exists(path));
            }

            // simulate allocation to the metadata
            metaData = MetaData.builder(metaData).put(dangledIndex, true).build();

            // check that several runs when in the metadata, there is no dangling because of the same cluster UUID
            for (int i = 0; i < numberOfChecks; i++) {
                Map<Index, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData).v1();
                assertTrue(newDanglingIndices.isEmpty());
                assertThat(danglingState.getDanglingIndices().size(), equalTo(0));
            }

            danglingState.cleanupAllocatedDangledIndices(metaData);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }

}
