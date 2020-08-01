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
package org.elasticsearch.index.shard;

import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class MultipleDataPathShardAllocationTests extends ESSingleNodeTestCase {
    // Sneakiness to install mock file stores so we can pretend how much free space we have on each path.data:
    private static MockFileStore aFileStore = new MockFileStore("mocka");
    private static MockFileStore bFileStore = new MockFileStore("mockb");
    private static String aPathPart;
    private static String bPathPart;

    @BeforeClass
    public static void installMockUsableSpaceFS() throws Exception {
        FileSystem current = PathUtils.getDefaultFileSystem();
        aPathPart = current.getSeparator() + 'a' + current.getSeparator();
        bPathPart = current.getSeparator() + 'b' + current.getSeparator();
        FileSystemProvider mock = new MockUsableSpaceFileSystemProvider(current);
        PathUtilsForTesting.installMock(mock.getFileSystem(null));
    }

    @AfterClass
    public static void removeMockUsableSpaceFS() throws Exception {
        PathUtilsForTesting.teardown();
        aFileStore = null;
        bFileStore = null;
    }

    /** Mock file system that fakes usable space for each FileStore */
    static class MockUsableSpaceFileSystemProvider extends FilterFileSystemProvider {

        MockUsableSpaceFileSystemProvider(FileSystem inner) {
            super("mockusablespace://", inner);
            final List<FileStore> fileStores = new ArrayList<>();
            fileStores.add(aFileStore);
            fileStores.add(bFileStore);
        }

        @Override
        public FileStore getFileStore(Path path) throws IOException {
            if (path.toString().contains(aPathPart) || (path.toString() + path.getFileSystem().getSeparator()).contains(aPathPart)) {
                return aFileStore;
            } else {
                return bFileStore;
            }
        }
    }

    static class MockFileStore extends FileStore {

        public long usableSpace;

        private final String desc;

        MockFileStore(String desc) {
            this.desc = desc;
        }

        @Override
        public String type() {
            return "mock";
        }

        @Override
        public String name() {
            return desc;
        }

        @Override
        public String toString() {
            return desc;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public long getTotalSpace() throws IOException {
            return usableSpace*3;
        }

        @Override
        public long getUsableSpace() throws IOException {
            return usableSpace;
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            return usableSpace*2;
        }

        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
            return false;
        }

        @Override
        public boolean supportsFileAttributeView(String name) {
            return false;
        }

        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
            return null;
        }

        @Override
        public Object getAttribute(String attribute) throws IOException {
            return null;
        }
    }

    @Override
    protected Settings nodeSettings() {
        final String[] dataPaths = new String[2];
        final Path tempDir = createTempDir("tempDataDir");
        dataPaths[0] = tempDir.resolve("a").toString();
        dataPaths[1] = tempDir.resolve("b").toString();
        return Settings.builder()
            .put(super.nodeSettings())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPaths).build();
    }

    public void testSelectMostFreeSpacePath() {
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class);
        NodeEnvironment.NodePath[] nodePaths = nodeEnv.nodePaths();
        assertEquals("mocka", nodePaths[0].fileStore.name());
        assertEquals("mockb", nodePaths[1].fileStore.name());

        aFileStore.usableSpace = 20000;
        bFileStore.usableSpace = 10000;
        // create index with one shard, shard should go to path a
        assertAcked(client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get());
        ensureGreen();
        assertShardDataPath("index", aPathPart, null);
        Index index = resolveIndex("index");
        assertEquals(nodePaths[0].getNumShards(), 1);
        assertEquals(nodePaths[0].getNumShards(index), 1);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);


        // delete index, check num of shards
        assertAcked(client().admin().indices().prepareDelete("index").get());
        assertEquals(nodePaths[0].getNumShards(), 0);
        assertEquals(nodePaths[0].getNumShards(index), 0);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);

        // reverse a/b path space
        aFileStore.usableSpace = 10000;
        bFileStore.usableSpace = 20000;
        // create index with one shard, shard should go to path b
        assertAcked(client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        assertShardDataPath("index", bPathPart, null);
        index = resolveIndex("index");
        assertEquals(nodePaths[0].getNumShards(), 0);
        assertEquals(nodePaths[0].getNumShards(index), 0);
        assertEquals(nodePaths[1].getNumShards(), 1);
        assertEquals(nodePaths[1].getNumShards(index), 1);

        // delete index, check num of shards
        assertAcked(client().admin().indices().prepareDelete("index").get());
        assertEquals(nodePaths[0].getNumShards(), 0);
        assertEquals(nodePaths[0].getNumShards(index), 0);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);
    }

    public void testSelectCurrentNodeLeastShardsPath() {
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class);
        NodeEnvironment.NodePath[] nodePaths = nodeEnv.nodePaths();
        assertEquals("mocka", nodePaths[0].fileStore.name());
        assertEquals("mockb", nodePaths[1].fileStore.name());

        aFileStore.usableSpace = 20000;
        bFileStore.usableSpace = 10000;
        // create index with one shard, shard should go to path a
        assertAcked(client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get());
        ensureGreen();
        assertShardDataPath("index", aPathPart, null);
        Index index = resolveIndex("index");
        assertEquals(nodePaths[0].getNumShards(), 1);
        assertEquals(nodePaths[0].getNumShards(index), 1);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);

        // create index1 with one shard, shard should go to path b even path a has more free space
        // since path b has least shards of this node regardless of different indices
        assertAcked(client().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get());
        ensureGreen();
        assertShardDataPath("index1", bPathPart, null);
        Index index1 = resolveIndex("index1");
        assertEquals(nodePaths[0].getNumShards(), 1);
        assertEquals(nodePaths[0].getNumShards(index), 1);
        assertEquals(nodePaths[1].getNumShards(), 1);
        assertEquals(nodePaths[1].getNumShards(index1), 1);

        // delete index, check num of shards
        assertAcked(client().admin().indices().prepareDelete("index", "index1").get());
        assertEquals(nodePaths[0].getNumShards(), 0);
        assertEquals(nodePaths[0].getNumShards(index), 0);
        assertEquals(nodePaths[0].getNumShards(index1), 0);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);
        assertEquals(nodePaths[1].getNumShards(index1), 0);
    }

    public void testSelectCurrentIndexLeastShardsPath() {
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class);
        NodeEnvironment.NodePath[] nodePaths = nodeEnv.nodePaths();
        assertEquals("mocka", nodePaths[0].fileStore.name());
        assertEquals("mockb", nodePaths[1].fileStore.name());

        aFileStore.usableSpace = 20000;
        bFileStore.usableSpace = 10000;
        // create index with one shard, shard should go to path a
        assertAcked(client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get());
        ensureGreen();
        assertShardDataPath("index", aPathPart, null);
        Index index = resolveIndex("index");
        assertEquals(nodePaths[0].getNumShards(), 1);
        assertEquals(nodePaths[0].getNumShards(index), 1);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);

        // reverse a/b path space
        aFileStore.usableSpace = 10000;
        bFileStore.usableSpace = 20000;

        // now path b has most free space and least shards of this node regardless of different indices,
        // now create index1 with two shards, the first shard should go to path b,
        // now path a and b has the same num of total shards, but path b still has more free space,
        // then second shard should go to path a, since path a has the least shards of current index1.
        assertAcked(client().admin().indices().prepareCreate("index1")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0)));
        ensureGreen();
        assertShardDataPath("index1", bPathPart, aPathPart);
        Index index1 = resolveIndex("index1");
        assertEquals(nodePaths[0].getNumShards(), 2);
        assertEquals(nodePaths[0].getNumShards(index), 1);
        assertEquals(nodePaths[0].getNumShards(index1), 1);
        assertEquals(nodePaths[1].getNumShards(), 1);
        assertEquals(nodePaths[1].getNumShards(index1), 1);

        // delete index, check num of shards
        assertAcked(client().admin().indices().prepareDelete("index", "index1").get());
        assertEquals(nodePaths[0].getNumShards(), 0);
        assertEquals(nodePaths[0].getNumShards(index), 0);
        assertEquals(nodePaths[0].getNumShards(index1), 0);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);
        assertEquals(nodePaths[1].getNumShards(index1), 0);
    }

    public void testCustomIndexDataPathWouldBeIgnored() {
        NodeEnvironment nodeEnv = getInstanceFromNode(NodeEnvironment.class);
        NodeEnvironment.NodePath[] nodePaths = nodeEnv.nodePaths();
        assertEquals("mocka", nodePaths[0].fileStore.name());
        assertEquals("mockb", nodePaths[1].fileStore.name());

        aFileStore.usableSpace = 10000;
        bFileStore.usableSpace = 20000;
        // create index with custom index data path, shard state should go to path a
        String customDataPath = randomAlphaOfLength(10);
        assertAcked(client().admin().indices().prepareCreate("index")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_DATA_PATH, customDataPath)
                .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)).get());
        ensureGreen();
        IndicesStatsResponse response = indicesStats("index");
        assertThat(response.getShards()[0].getDataPath(), containsString(customDataPath));
        assertThat(response.getShards()[0].getStatePath() + '\\', containsString(aPathPart));
        Index index = resolveIndex("index");
        assertEquals(nodePaths[0].getNumShards(), 0);
        assertEquals(nodePaths[0].getNumShards(index), 0);
        assertEquals(nodePaths[1].getNumShards(), 0);
        assertEquals(nodePaths[1].getNumShards(index), 0);
    }

    private void assertShardDataPath(String index, String firstShardPath, String secondShardPath) {
        IndicesStatsResponse response = indicesStats(index);
        if (firstShardPath != null) {
            // get data path only returned parent path
            assertThat(response.getShards()[0].getDataPath() + '\\', containsString(firstShardPath));
        }
        if (secondShardPath != null) {
            // get data path only returned parent path
            assertThat(response.getShards()[1].getDataPath() + '\\', containsString(secondShardPath));
        }
    }

    private IndicesStatsResponse indicesStats(String index) {
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.clear();
        request.indices(index);
        return client().admin().indices().stats(request).actionGet();
    }
}
