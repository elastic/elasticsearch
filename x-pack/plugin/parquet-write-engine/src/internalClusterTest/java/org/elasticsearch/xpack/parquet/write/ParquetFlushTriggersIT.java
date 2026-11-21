/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The two triggers of the flush that no one asks for: the age of the oldest local file, and a shard that has no more writes. The shard
 * checks the first after each write, and the memory controller of the node checks the second, so this class gives the node a short
 * time after which a shard counts as idle. Another class holds the tests that need the local files to stay local.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushTriggersIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetFlushTriggersIT extends ESIntegTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String INDEX = "parquet-triggers";
    private static final String REPOSITORY = "lake";

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
            // a shard that has had no write for a second is idle, and the node looks every second
            .put("indices.memory.shard_inactive_time", "1s")
            .put("indices.memory.interval", "1s")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected void beforeIndexDeletion() {
        // the base class searches every shard, and this engine cannot be searched
    }

    private void createIndex(Settings.Builder extra) throws IOException {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.durability", randomFrom("local", "staged"))
                        .put(extra.build())
                )
                .setMapping("count", "type=long")
        );
        ensureGreen(INDEX);
    }

    private BulkResponse bulk(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = from; i < to; i++) {
            bulk.add(new IndexRequest(INDEX).source("count", i));
        }
        return bulk.get();
    }

    private IndexShard shard() {
        return internalCluster().getDataNodeInstance(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(0);
    }

    private long localFiles() throws IOException {
        final Directory directory = shard().store().directory();
        long files = 0;
        for (String name : directory.listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_")) {
                files++;
            }
        }
        return files;
    }

    private ShardTable table() throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        final String uuid = resolveIndex(INDEX).getUUID();
        final FsBlobStore store = new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true);
        return ShardTable.open(
            store,
            new TableLocation(root + "/" + INDEX + "/" + uuid, BlobPath.EMPTY.add(INDEX).add(uuid)),
            0,
            INDEX,
            uuid,
            shard().getHistoryUUID(),
            null
        );
    }

    /** The high-water mark that the table lists, or -1 while the table does not exist because nothing was uploaded yet. */
    private long uploadedThrough() {
        try {
            return table().uploadedThroughSeqNo();
        } catch (RuntimeException | IOException e) {
            return -1;
        }
    }

    /** The age is looked at after a write, so the write that comes after the age has passed uploads the files that are older. */
    public void testAFileThatIsOlderThanTheThresholdIsUploadedByTheNextWrite() throws Exception {
        // an idle shard would upload by itself, so the idle time of this index is long
        createIndex(Settings.builder().put("index.parquet.flush_threshold_age", "1s").put("index.parquet.flush_threshold_size", "1gb"));
        assertFalse(bulk(0, 100).hasFailures());
        assertThat(localFiles(), greaterThan(0L));

        safeSleep(1_500);
        assertFalse(bulk(100, 200).hasFailures());

        assertBusy(() -> assertThat(uploadedThrough(), equalTo(199L)));
    }

    /** A shard that has had no write for the idle time of the node uploads what it has, with no flush request and no further write. */
    public void testAnIdleShardUploadsItsLocalFiles() throws Exception {
        createIndex(Settings.builder().put("index.parquet.flush_threshold_age", "1h").put("index.parquet.flush_threshold_size", "1gb"));
        assertFalse(bulk(0, 100).hasFailures());
        assertThat(localFiles(), greaterThan(0L));

        assertBusy(() -> assertThat(uploadedThrough(), equalTo(99L)), 30, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertThat(localFiles(), equalTo(0L)));
    }
}
