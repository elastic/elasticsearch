/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.indices.IndicesService.WRITE_DANGLING_INDICES_INFO_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BulkAfterWriteFsyncFailureIT extends ESSingleNodeTestCase {
    private static FSyncFailureFileSystemProvider fsyncFailureFileSystemProvider;

    @BeforeClass
    public static void installDisruptFSyncFS() {
        FileSystem current = PathUtils.getDefaultFileSystem();
        fsyncFailureFileSystemProvider = new FSyncFailureFileSystemProvider(current);
        PathUtilsForTesting.installMock(fsyncFailureFileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeDisruptFSyncFS() {
        PathUtilsForTesting.teardown();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(WRITE_DANGLING_INDICES_INFO_SETTING.getKey(), false).build();
    }

    public void testFsyncFailureDoesNotAdvanceLocalCheckpoints() {
        String indexName = randomIdentifier();
        client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(indexSettings(1, 0).put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1))
            .setMapping("key", "type=keyword", "val", "type=long")
            .get();
        ensureGreen(indexName);

        var localCheckpointBeforeBulk = getLocalCheckpointForShard(indexName, 0);
        fsyncFailureFileSystemProvider.failFSyncOnce(true);
        var bulkResponse = client().prepareBulk().add(prepareIndex(indexName).setId("1").setSource("key", "foo", "val", 10)).get();
        assertTrue(bulkResponse.hasFailures());
        var localCheckpointAfterFailedBulk = getLocalCheckpointForShard(indexName, 0);
        // fsync for the translog failed, hence the checkpoint doesn't advance
        assertThat(localCheckpointBeforeBulk, equalTo(localCheckpointAfterFailedBulk));

        // Since background refreshes are disabled, the shard is considered green until the next operation is appended into the translog
        ensureGreen(indexName);

        // If the after write fsync fails, it'll fail the TranslogWriter but not the Engine, we'll need to try to append a new operation
        // into the translog so the exception bubbles up and fails the engine. On the other hand, the TranslogReplicationAction will retry
        // this action on AlreadyClosedExceptions, that's why the operation ends up succeeding even after the engine failed.
        var bulkResponse2 = client().prepareBulk().add(prepareIndex(indexName).setId("2").setSource("key", "bar", "val", 20)).get();
        assertFalse(bulkResponse2.hasFailures());

        var localCheckpointAfterSuccessfulBulk = getLocalCheckpointForShard(indexName, 0);
        assertThat(localCheckpointAfterSuccessfulBulk, is(greaterThan(localCheckpointAfterFailedBulk)));
    }

    long getLocalCheckpointForShard(String index, int shardId) {
        var indicesService = getInstanceFromNode(IndicesService.class);
        var indexShard = indicesService.indexServiceSafe(resolveIndex(index)).getShard(shardId);
        return indexShard.getLocalCheckpoint();
    }

    public static class FSyncFailureFileSystemProvider extends FilterFileSystemProvider {
        private final AtomicBoolean failFSyncs = new AtomicBoolean();

        public FSyncFailureFileSystemProvider(FileSystem delegate) {
            super("fsyncfailure://", delegate);
        }

        public void failFSyncOnce(boolean shouldFail) {
            failFSyncs.set(shouldFail);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {

                @Override
                public void force(boolean metaData) throws IOException {
                    if (failFSyncs.compareAndSet(true, false)) {
                        throw new IOException("simulated");
                    }
                    super.force(metaData);
                }
            };
        }
    }
}
