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

import com.carrotsearch.randomizedtesting.annotations.Repeat;

import org.apache.lucene.mockfile.FilterFileSystem;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class NewPathForShardTest extends ESTestCase {

    // Sneakiness to install a mock filesystem to pretend how much free space we have on each path.data:

    private static MockFileStore aFileStore = new MockFileStore("mocka");
    private static MockFileStore bFileStore = new MockFileStore("mockb");
    private static FileSystem origFileSystem;

    @BeforeClass
    public static void installMockUsableSpaceFS() throws Exception {
        // Necessary so when Environment.clinit runs, to gather all FileStores, it sees ours:
        Field field = PathUtils.class.getDeclaredField("DEFAULT");
        field.setAccessible(true);
        // nocommit can't double Filter maybe?
        // origFileSystem = (FileSystem) field.get(null);
        origFileSystem = FileSystems.getDefault();
        FileSystem mock = new MockUsableSpaceFileSystemProvider().getFileSystem(getBaseTempDirForTestClass().toUri());
        field.set(null, mock);
        assertEquals(mock, PathUtils.getDefaultFileSystem());
    }

    @AfterClass
    public static void removeMockUsableSpaceFS() throws Exception {
        Field field = PathUtils.class.getDeclaredField("DEFAULT");
        field.setAccessible(true);
        field.set(null, origFileSystem);
        origFileSystem = null;
        aFileStore = null;
        bFileStore = null;
    }

    /** Mock file system that fakes usable space for each FileStore */
    static class MockUsableSpaceFileSystemProvider extends FilterFileSystemProvider {
    
        public MockUsableSpaceFileSystemProvider() {
            super("mockusablespace://", FileSystems.getDefault());
            final List<FileStore> fileStores = new ArrayList<>();
            fileStores.add(aFileStore);
            fileStores.add(bFileStore);
            fileSystem = new FilterFileSystem(this, origFileSystem) {
                    @Override
                    public Iterable<FileStore> getFileStores() {
                        return fileStores;
                    }
                };
        }

        @Override
        public FileStore getFileStore(Path path) throws IOException {
            if (path.toString().contains("/a/")) {
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
            return "mock";
        }

        @Override
        public String toString() {
            return desc;
        }

        // TODO: we can enable mocking of these when we need them later:

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
        Path path = createTempDir();

        String[] paths = new String[2];
        paths[0] = path.resolve("a").toString();
        paths[1] = path.resolve("b").toString();

        Settings settings = Settings.builder()
            .put("path.home", path)
            .putArray("path.data", paths).build();
        NodeEnvironment nodeEnv = new NodeEnvironment(settings, new Environment(settings));
    }
}
