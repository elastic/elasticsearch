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
package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.RateLimitedFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SleepingLockWrapper;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FsDirectoryServiceTests extends ESTestCase {

    public void testHasSleepWrapperOnSharedFS() throws IOException {
        Settings build = randomBoolean() ?
            Settings.builder().put(IndexMetaData.SETTING_SHARED_FILESYSTEM, true).build() :
            Settings.builder().put(IndexMetaData.SETTING_SHADOW_REPLICAS, true).build();;
        IndexSettings settings = IndexSettingsModule.newIndexSettings("foo", build);
        IndexStoreConfig config = new IndexStoreConfig(build);
        IndexStore store = new IndexStore(settings, config);
        Path tempDir = createTempDir().resolve(settings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(settings.getIndex(), 0));
        FsDirectoryService fsDirectoryService = new FsDirectoryService(settings, store, path);
        Directory directory = fsDirectoryService.newDirectory();
        assertTrue(directory instanceof RateLimitedFSDirectory);
        RateLimitedFSDirectory rateLimitingDirectory = (RateLimitedFSDirectory) directory;
        Directory delegate = rateLimitingDirectory.getDelegate();
        assertTrue(delegate.getClass().toString(), delegate instanceof SleepingLockWrapper);
    }

    public void testHasNoSleepWrapperOnNormalFS() throws IOException {
        Settings build = Settings.builder().put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "simplefs").build();
        IndexSettings settings = IndexSettingsModule.newIndexSettings("foo", build);
        IndexStoreConfig config = new IndexStoreConfig(build);
        IndexStore store = new IndexStore(settings, config);
        Path tempDir = createTempDir().resolve(settings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(settings.getIndex(), 0));
        FsDirectoryService fsDirectoryService = new FsDirectoryService(settings, store, path);
        Directory directory = fsDirectoryService.newDirectory();
        assertTrue(directory instanceof RateLimitedFSDirectory);
        RateLimitedFSDirectory rateLimitingDirectory = (RateLimitedFSDirectory) directory;
        Directory delegate = rateLimitingDirectory.getDelegate();
        assertFalse(delegate instanceof SleepingLockWrapper);
        assertTrue(delegate instanceof SimpleFSDirectory);
    }
}
