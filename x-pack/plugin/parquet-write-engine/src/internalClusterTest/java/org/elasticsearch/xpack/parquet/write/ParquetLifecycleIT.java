/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Spike S2: a flush merges the local files and uploads them to a real {@code fs} repository as the files of an Iceberg table, and
 * a restart finds what is uploaded and what is not.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetLifecycleIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetLifecycleIT extends ESIntegTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final String INDEX = "parquet-lifecycle";
    private static final String REPOSITORY = "lake";
    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    private static Path repoRoot;

    @BeforeClass
    public static void createRepoRoot() {
        repoRoot = createTempDir();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // the engine has no row path, and the test cluster turns this kill switch off for some seeds
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .put(Environment.PATH_REPO_SETTING.getKey(), repoRoot)
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

    private void registerRepository() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
    }

    private void createIndex(int shards, Settings.Builder extra) throws IOException {
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "strict")
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host.name")
            .field("type", "keyword")
            .endObject()
            .startObject("count")
            .field("type", "long")
            .endObject()
            .startObject("bytes")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", shards)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.durability", randomFrom("local", "staged"))
                        .put(extra.build())
                )
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private BulkResponse bulk(int from, int to) {
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
        for (int i = from; i < to; i++) {
            bulk.add(
                new IndexRequest(INDEX).source(
                    "@timestamp",
                    BASE_TIMESTAMP + i * 1000L,
                    "host.name",
                    "host-" + (i % 7),
                    "count",
                    i,
                    "bytes",
                    i % 1000
                )
            );
        }
        return bulk.get();
    }

    private IndexShard shard(int shard) {
        return internalCluster().getDataNodeInstance(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(shard);
    }

    private long localFiles(int shard) throws IOException {
        final Directory directory = shard(shard).store().directory();
        long files = 0;
        for (String name : directory.listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_")) {
                files++;
            }
        }
        return files;
    }

    /** The table of the index, as a reader outside the engine opens it: from the repository directory. */
    private ShardTable table(int shard) throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        final String tablePath = INDEX + "/" + resolveIndex(INDEX).getUUID();
        final FsBlobStore store = new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true);
        return ShardTable.open(
            store,
            new TableLocation(root + "/" + tablePath, BlobPath.EMPTY.add(INDEX).add(resolveIndex(INDEX).getUUID())),
            shard,
            INDEX,
            resolveIndex(INDEX).getUUID(),
            shard(shard).getHistoryUUID(),
            null
        );
    }

    /** The values of {@code count} in the files that the table lists for the shards, each once. */
    private TreeSet<Long> uploadedCounts(int shards) throws IOException {
        final TreeSet<Long> counts = new TreeSet<>();
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        for (int shard = 0; shard < shards; shard++) {
            long expectedFrom = 0;
            for (ShardTable.UploadedFile file : table(shard).uploadedFiles()) {
                assertThat("the files of shard " + shard + " are contiguous", file.fromSeqNo(), equalTo(expectedFrom));
                expectedFrom = file.toSeqNo() + 1;
                final Path path = org.elasticsearch.core.PathUtils.get(java.net.URI.create(file.location()));
                final TestParquetBytes.Contents contents = TestParquetBytes.read(java.nio.file.Files.readAllBytes(path));
                assertThat(contents.rows().size(), equalTo((int) file.rows()));
                for (var row : contents.rows()) {
                    assertTrue("count appears once", counts.add(row.getLong("count", 0)));
                }
            }
        }
        return counts;
    }

    private void assertUploaded(int shards, int rows) throws IOException {
        final TreeSet<Long> counts = uploadedCounts(shards);
        assertThat(counts.size(), equalTo(rows));
        assertThat(counts.first(), equalTo(0L));
        assertThat(counts.last(), equalTo(rows - 1L));
    }

    public void testClosingAndOpeningAnIndexKeepsEveryRow() throws Exception {
        registerRepository();
        createIndex(2, Settings.builder().put("index.parquet.compaction.enabled", false));
        assertFalse(bulk(0, 1_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertFalse(bulk(1_000, 1_500).hasFailures()); // only on the local disk

        assertAcked(indicesAdmin().prepareClose(INDEX));
        assertAcked(indicesAdmin().prepareOpen(INDEX));
        ensureGreen(INDEX);

        // closing the index verifies and flushes its shards, so the rows of the local files are uploaded now or still safe on the disk
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(1_500L));
        assertFalse(bulk(1_500, 2_000).hasFailures());
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertUploaded(2, 2_000);
        assertThat(localFiles(0) + localFiles(1), equalTo(0L));
    }

    public void testAnIndexThatIsClosedAndOpenedManyTimesKeepsItsSequence() throws Exception {
        registerRepository();
        createIndex(1, Settings.builder().put("index.parquet.compaction.enabled", false));
        int rows = 0;
        for (int round = 0; round < 3; round++) {
            assertFalse(bulk(rows, rows + 300).hasFailures());
            rows += 300;
            assertAcked(indicesAdmin().prepareClose(INDEX));
            assertAcked(indicesAdmin().prepareOpen(INDEX));
            ensureGreen(INDEX);
        }
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertUploaded(1, rows);
        assertThat(shard(0).seqNoStats().getMaxSeqNo(), equalTo(rows - 1L));
    }
}
