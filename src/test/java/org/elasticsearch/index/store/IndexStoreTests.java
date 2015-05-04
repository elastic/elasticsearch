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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

/**
 */
public class IndexStoreTests extends ElasticsearchTestCase {

    public void testStoreDirectory() throws IOException {
        final Path tempDir = createTempDir();
        final IndexStoreModule.Type[] values = IndexStoreModule.Type.values();
        final IndexStoreModule.Type type = RandomPicks.randomFrom(random(), values);
        Settings settings = ImmutableSettings.settingsBuilder().put(IndexStoreModule.STORE_TYPE, type.name()).build();
        FsDirectoryService service = new FsDirectoryService(settings, null, new ShardPath(tempDir, tempDir, "foo", new ShardId("foo", 0)));
        try (final Directory directory = service.newFSDirectory(tempDir, NoLockFactory.INSTANCE)) {
            switch (type) {
                case NIOFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof NIOFSDirectory);
                    break;
                case MMAPFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof MMapDirectory);
                    break;
                case SIMPLEFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof SimpleFSDirectory);
                    break;
                case FS:
                case DEFAULT:
                   if (Constants.WINDOWS) {
                        if (Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
                            assertTrue(type + " " + directory.toString(), directory instanceof MMapDirectory);
                        } else {
                            assertTrue(type + " " + directory.toString(), directory instanceof SimpleFSDirectory);
                        }
                    }  else if (Constants.JRE_IS_64BIT && MMapDirectory.UNMAP_SUPPORTED) {
                        assertTrue(type + " " + directory.toString(), directory instanceof FileSwitchDirectory);
                    } else  {
                        assertTrue(type + " " + directory.toString(), directory instanceof NIOFSDirectory);
                    }
                    break;
            }
        }
    }

    public void testStoreDirectoryDefault() throws IOException {
        final Path tempDir = createTempDir();
        Settings settings = ImmutableSettings.EMPTY;
        FsDirectoryService service = new FsDirectoryService(settings, null, new ShardPath(tempDir, tempDir, "foo", new ShardId("foo", 0)));
        try (final Directory directory = service.newFSDirectory(tempDir, NoLockFactory.INSTANCE)) {
            if (Constants.WINDOWS) {
                assertTrue(directory.toString(), directory instanceof MMapDirectory || directory instanceof SimpleFSDirectory);
            } else {
                assertTrue(directory.toString(), directory instanceof FileSwitchDirectory);
            }
        }
    }


}
