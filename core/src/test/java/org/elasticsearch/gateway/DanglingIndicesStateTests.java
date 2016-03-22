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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.IndexStateMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the dangling indices import and dangling indices delete functionality.
 */
public class DanglingIndicesStateTests extends ESTestCase {

    private static ThreadPool THREAD_POOL;
    private static Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    private IndicesService indicesService;
    private NodeEnvironment env;

    @BeforeClass
    public static void createThreadPool() {
        THREAD_POOL = new ThreadPool("DynamicMappingDisabledTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        env = newNodeEnvironment();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        indicesService = new IndicesService(Settings.EMPTY, null, env, clusterSettings, null, null, null, null, null,
                                            THREAD_POOL, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, null);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        env.close();
        env = null;
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testCleanupWhenEmpty() throws Exception {
        MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
        DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null, indicesService);

        assertTrue(danglingState.getDanglingIndices().isEmpty());
        MetaData metaData = MetaData.builder().build();
        danglingState.cleanupAllocatedDangledIndices(metaData);
        assertTrue(danglingState.getDanglingIndices().isEmpty());
    }

    public void testDanglingIndicesDiscovery() throws Exception {
        MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
        DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null, indicesService);

        assertTrue(danglingState.getDanglingIndices().isEmpty());
        MetaData metaData = MetaData.builder().build();
        final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_INDEX_UUID, "test1UUID");
        IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(settings).build();
        metaStateService.writeIndex("test_write", new IndexStateMetaData(dangledIndex, Strings.randomBase64UUID()));
        Map<Index, IndexMetaData> newDanglingIndices = danglingState.findDanglingAndDeletedIndices(metaData).v1();
        assertTrue(newDanglingIndices.containsKey(dangledIndex.getIndex()));
        metaData = MetaData.builder().put(dangledIndex, false).build();
        newDanglingIndices = danglingState.findDanglingAndDeletedIndices(metaData).v1();
        assertFalse(newDanglingIndices.containsKey(dangledIndex.getIndex()));
    }

    public void testInvalidIndexFolder() throws Exception {
        MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
        DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null, indicesService);

        MetaData metaData = MetaData.builder().build();
        final String uuid = "test1UUID";
        final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_INDEX_UUID, uuid);
        IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(settings).build();
        metaStateService.writeIndex("test_write", new IndexStateMetaData(dangledIndex, Strings.randomBase64UUID()));
        for (Path path : env.resolveIndexFolder(uuid)) {
            if (Files.exists(path)) {
                Files.move(path, path.resolveSibling("invalidUUID"), StandardCopyOption.ATOMIC_MOVE);
            }
        }
        try {
            danglingState.findDanglingAndDeletedIndices(metaData);
            fail("no exception thrown for invalid folder name");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("[invalidUUID] invalid index folder name, rename to [test1UUID]"));
        }
    }

    public void testDanglingProcessing() throws Exception {
        MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
        DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null, indicesService);

        MetaData metaData = MetaData.builder().clusterUUID(Strings.randomBase64UUID()).build();

        final Settings.Builder settings = Settings.builder().put(indexSettings).put(IndexMetaData.SETTING_INDEX_UUID, "test1UUID");
        IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(settings).build();
        // a different cluster UUID, so the dangling index imports
        IndexStateMetaData persistedDangledIndex = new IndexStateMetaData(dangledIndex, Strings.randomBase64UUID());
        metaStateService.writeIndex("test_write", persistedDangledIndex);

        // check that several runs when not in the metadata still keep the dangled index around
        int numberOfChecks = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfChecks; i++) {
            Map<Index, IndexMetaData> newDanglingIndices = danglingState.findDanglingAndDeletedIndices(metaData).v1();
            assertThat(newDanglingIndices.size(), equalTo(1));
            assertThat(newDanglingIndices.keySet(), Matchers.hasItems(dangledIndex.getIndex()));
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }

        for (int i = 0; i < numberOfChecks; i++) {
            danglingState.findOnDiskAndAddDanglingIndices(metaData);

            assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
            assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems(dangledIndex.getIndex()));
        }

        // simulate allocation to the metadata
        metaData = MetaData.builder(metaData).put(dangledIndex, true).build();

        // check that several runs when in the metadata, but not cleaned yet, still keeps dangled
        for (int i = 0; i < numberOfChecks; i++) {
            Map<Index, IndexMetaData> newDanglingIndices = danglingState.findDanglingAndDeletedIndices(metaData).v1();
            assertTrue(newDanglingIndices.isEmpty());

            assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
            assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems(dangledIndex.getIndex()));
        }

        danglingState.cleanupAllocatedDangledIndices(metaData);
        assertTrue(danglingState.getDanglingIndices().isEmpty());
    }

    public void testDanglingProcessingNoImport() throws Exception {
        MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
        DanglingIndicesState danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null, indicesService);

        final String clusterUUID = Strings.randomBase64UUID();
        final String indexName = "test1";
        IndexMetaData dangledIndex = IndexMetaData.builder(indexName).settings(indexSettings).build();
        MetaData metaData = MetaData.builder().clusterUUID(clusterUUID).build();
        metaStateService.writeIndex("test_write", new IndexStateMetaData(dangledIndex, clusterUUID));

        // check that for several runs, no dangling indices are imported because of the same cluster UUID,
        // and that deleted indices are not empty (so they can be cleaned up)
        int numberOfChecks = randomIntBetween(1, 10);
        for (int i = 0; i < numberOfChecks; i++) {
            Tuple<Map<Index, IndexMetaData>, Map<Index, IndexMetaData>> tuple = danglingState.findDanglingAndDeletedIndices(metaData);
            Map<Index, IndexMetaData> newDanglingIndices = tuple.v1();
            Map<Index, IndexMetaData> deletedIndices = tuple.v2();
            assertThat(newDanglingIndices.size(), equalTo(0));
            assertTrue(danglingState.getDanglingIndices().isEmpty());
            assertThat(deletedIndices.size(), equalTo(1));
            assertThat(deletedIndices.keySet().stream().map(index -> index.getName()).collect(Collectors.toList()),
                Matchers.hasItems(indexName));
        }

        for (int i = 0; i < numberOfChecks; i++) {
            Map<Index, IndexMetaData> deletedIndices = danglingState.findOnDiskAndAddDanglingIndices(metaData);
            assertThat(danglingState.getDanglingIndices().size(), equalTo(0));
            assertThat(deletedIndices.size(), equalTo(1));
            assertThat(deletedIndices.keySet().stream().map(index -> index.getName()).collect(Collectors.toList()),
                Matchers.hasItems(indexName));
        }

        // check that deleted indices exist, then get deleted, then don't exist in the data structure nor on disk
        danglingState = new DanglingIndicesState(Settings.EMPTY, env, metaStateService, null, indicesService);
        dangledIndex = IndexMetaData.builder(indexName).settings(indexSettings).build();
        metaData = MetaData.builder().clusterUUID(clusterUUID).build();
        ClusterState clusterState = new ClusterState.Builder(new ClusterName("testCluster")).metaData(metaData).build();
        metaStateService.writeIndex("test_write", new IndexStateMetaData(dangledIndex, clusterUUID));
        Map<Index, IndexMetaData> deletedIndices = danglingState.findOnDiskAndAddDanglingIndices(metaData);
        assertThat(deletedIndices.size(), equalTo(1));
        danglingState.cleanDeletedIndices(deletedIndices, clusterState);
        for (Path path : env.indexPaths(dangledIndex.getIndex())) {
            assertFalse(Files.exists(path));
        }

        // simulate allocation to the metadata
        metaData = MetaData.builder(metaData).put(dangledIndex, true).build();

        // check that several runs when in the metadata, there is no dangling because of the same cluster UUID
        for (int i = 0; i < numberOfChecks; i++) {
            Map<Index, IndexMetaData> newDanglingIndices = danglingState.findDanglingAndDeletedIndices(metaData).v1();
            assertTrue(newDanglingIndices.isEmpty());
            assertThat(danglingState.getDanglingIndices().size(), equalTo(0));
        }

        danglingState.cleanupAllocatedDangledIndices(metaData);
        assertTrue(danglingState.getDanglingIndices().isEmpty());
    }

}
