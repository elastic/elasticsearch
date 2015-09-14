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
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment.NodePath;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Separate test class from ShardPathTests because we need static (BeforeClass) setup to install mock filesystems... */
public class NewPathForShardTests extends ESTestCase {

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
    
        public MockUsableSpaceFileSystemProvider(FileSystem inner) {
            super("mockusablespace://", inner);
            final List<FileStore> fileStores = new ArrayList<>();
            fileStores.add(aFileStore);
            fileStores.add(bFileStore);
        }

        @Override
        public FileStore getFileStore(Path path) throws IOException {
            if (path.toString().contains(aPathPart)) {
                return aFileStore;
            } else {
                return bFileStore;
            }
        }
    }

    static class MockFileStore extends FileStore {

        public long usableSpace;

        private final String desc;

        public MockFileStore(String desc) {
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

    public void testSelectNewPathForShard() throws Exception {
        Path path = PathUtils.get(createTempDir().toString());

        // Use 2 data paths:
        String[] paths = new String[] {path.resolve("a").toString(),
                                       path.resolve("b").toString()};

        Settings settings = Settings.builder()
            .put("path.home", path)
            .putArray("path.data", paths).build();
        NodeEnvironment nodeEnv = new NodeEnvironment(settings, new Environment(settings));

        // Make sure all our mocking above actually worked:
        NodePath[] nodePaths = nodeEnv.nodePaths();
        assertEquals(2, nodePaths.length);

        assertEquals("mocka", nodePaths[0].fileStore.name());
        assertEquals("mockb", nodePaths[1].fileStore.name());

        // Path a has lots of free space, but b has little, so new shard should go to a:
        aFileStore.usableSpace = 100000;
        bFileStore.usableSpace = 1000;

        ShardId shardId = new ShardId("index", 0);
        ShardPath result = ShardPath.selectNewPathForShard(nodeEnv, shardId, Settings.EMPTY, 100, Collections.<Path,Integer>emptyMap());
        assertTrue(result.getDataPath().toString().contains(aPathPart));

        // Test the reverse: b has lots of free space, but a has little, so new shard should go to b:
        aFileStore.usableSpace = 1000;
        bFileStore.usableSpace = 100000;

        shardId = new ShardId("index", 0);
        result = ShardPath.selectNewPathForShard(nodeEnv, shardId, Settings.EMPTY, 100, Collections.<Path,Integer>emptyMap());
        assertTrue(result.getDataPath().toString().contains(bPathPart));

        // Now a and be have equal usable space; we allocate two shards to the node, and each should go to different paths:
        aFileStore.usableSpace = 100000;
        bFileStore.usableSpace = 100000;

        Map<Path,Integer> dataPathToShardCount = new HashMap<>();
        ShardPath result1 = ShardPath.selectNewPathForShard(nodeEnv, shardId, Settings.EMPTY, 100, dataPathToShardCount);
        dataPathToShardCount.put(NodeEnvironment.shardStatePathToDataPath(result1.getDataPath()), 1);
        ShardPath result2 = ShardPath.selectNewPathForShard(nodeEnv, shardId, Settings.EMPTY, 100, dataPathToShardCount);

        // #11122: this was the original failure: on a node with 2 disks that have nearly equal
        // free space, we would always allocate all N incoming shards to the one path that
        // had the most free space, never using the other drive unless new shards arrive
        // after the first shards started using storage:
        assertNotEquals(result1.getDataPath(), result2.getDataPath());
        
        nodeEnv.close();
    }
}
