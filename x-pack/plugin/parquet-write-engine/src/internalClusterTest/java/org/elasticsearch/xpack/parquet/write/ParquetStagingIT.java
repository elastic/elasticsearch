/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.action.bulk.BulkItemResponse;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * With {@code index.parquet.durability: staged} a bulk is acknowledged only after a copy of its file is in the repository below
 * {@code staging/}, and a flush deletes the copies of the rows that it has put in the table. A bulk that cannot be staged fails and leaves
 * nothing behind.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushIT.IcebergWorkerPoolFilter.class }
)
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetStagingIT extends ESIntegTestCase {

    private static final String INDEX = "parquet-staging";
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

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    private void registerRepository() {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repoRoot.resolve(REPOSITORY).toString()))
        );
    }

    /**
     * Makes the staging directory of the shard read-only, so that the upload of a staged file fails with an {@code IOException} that
     * the repository gives for any write error. It exists after the first staged bulk.
     */
    private void failStaging(boolean fail) throws IOException {
        final Path directory = tableDirectory().resolve("staging").resolve("shard-0").resolve(shard().getHistoryUUID());
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString(fail ? "r-xr-xr-x" : "rwxr-xr-x"));
    }

    private void createIndex() throws IOException {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(
                    Settings.builder()
                        .put("index.storage_format", "parquet")
                        .put("index.mode", "columnar")
                        .put("index.number_of_shards", 1)
                        .put("index.parquet.repository", REPOSITORY)
                        .put("index.parquet.durability", "staged")
                        .put("index.parquet.flush_threshold_age", "6h")
                        .put("index.parquet.compaction.enabled", false)
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

    private static void assertBulkOk(BulkResponse response) {
        if (response.hasFailures()) {
            final BulkItemResponse failed = java.util.Arrays.stream(response.getItems())
                .filter(BulkItemResponse::isFailed)
                .findFirst()
                .get();
            fail(org.elasticsearch.ExceptionsHelper.stackTrace(failed.getFailure().getCause()));
        }
    }

    private IndexShard shard() {
        return internalCluster().getDataNodeInstance(IndicesService.class).indexServiceSafe(resolveIndex(INDEX)).getShard(0);
    }

    private long localFiles() throws IOException {
        long files = 0;
        for (String name : shard().store().directory().listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_")) {
                files++;
            }
        }
        return files;
    }

    private Path tableDirectory() {
        return repoRoot.resolve(REPOSITORY).resolve(INDEX).resolve(resolveIndex(INDEX).getUUID());
    }

    /** The staged files of the shard in its current history, as a reader outside the engine sees them. */
    private List<Path> stagedFiles() throws IOException {
        final Path directory = tableDirectory().resolve("staging").resolve("shard-0").resolve(shard().getHistoryUUID());
        if (Files.exists(directory) == false) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(f -> f.getFileName().toString().endsWith(".parquet")).sorted().toList();
        }
    }

    private TreeSet<Long> countsOf(List<Path> files) throws IOException {
        final TreeSet<Long> counts = new TreeSet<>();
        for (Path file : files) {
            for (var row : TestParquetBytes.read(Files.readAllBytes(file)).rows()) {
                assertTrue("count appears once", counts.add(row.getLong("count", 0)));
            }
        }
        return counts;
    }

    /** The table of the index, as a reader outside the engine opens it: from the repository directory. */
    private ShardTable table() throws IOException {
        final String root = repoRoot.resolve(REPOSITORY).toAbsolutePath().normalize().toUri().toString().replaceAll("/+$", "");
        final String uuid = resolveIndex(INDEX).getUUID();
        return ShardTable.open(
            new FsBlobStore(4096, repoRoot.resolve(REPOSITORY), true),
            new TableLocation(root + "/" + INDEX + "/" + uuid, BlobPath.EMPTY.add(INDEX).add(uuid)),
            0,
            INDEX,
            uuid,
            shard().getHistoryUUID(),
            null
        );
    }

    /** The values of {@code count} in the data files that the table lists for the shard. */
    private TreeSet<Long> uploadedCounts() throws IOException {
        final List<Path> files = new ArrayList<>();
        for (ShardTable.UploadedFile file : table().uploadedFiles()) {
            files.add(org.elasticsearch.core.PathUtils.get(java.net.URI.create(file.location())));
        }
        return countsOf(files);
    }

    public void testABulkIsStagedBeforeItIsAcknowledgedAndAFlushDeletesTheCopies() throws Exception {
        registerRepository();
        createIndex();

        assertBulkOk(bulk(0, 1_000));
        assertBulkOk(bulk(1_000, 2_000));

        // every acknowledged row is in a staged file, and the staged files are the local files
        final List<Path> staged = stagedFiles();
        assertThat(staged, hasSize(2));
        assertThat(staged.size(), equalTo((int) localFiles()));
        final TreeSet<Long> counts = countsOf(staged);
        assertThat(counts.size(), equalTo(2_000));
        assertThat(counts.first(), equalTo(0L));
        assertThat(counts.last(), equalTo(1_999L));
        assertThat("nothing is in the table yet", table().uploadedFiles(), hasSize(0));

        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        assertThat("the flush deleted the staged copies", stagedFiles(), hasSize(0));
        assertThat(uploadedCounts().size(), equalTo(2_000));
        assertThat(localFiles(), equalTo(0L));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(2_000L));
    }

    public void testABulkThatCannotBeStagedFailsAndLeavesNothingBehind() throws Exception {
        assumeTrue(
            "needs POSIX permissions",
            org.elasticsearch.core.PathUtils.getDefaultFileSystem().supportedFileAttributeViews().contains("posix")
        );
        assumeFalse("root ignores permissions", "root".equals(System.getProperty("user.name")));
        registerRepository();
        createIndex();
        assertBulkOk(bulk(0, 100));

        failStaging(true);
        final BulkResponse failed;
        try {
            failed = bulk(100, 200);
        } finally {
            failStaging(false);
        }

        assertTrue(failed.hasFailures());
        for (BulkItemResponse item : failed.getItems()) {
            assertTrue(item.isFailed());
            assertThat(item.getFailure().getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            // the client sees what failed, not only that something did
            assertThat(item.getFailure().getMessage(), containsString("AccessDeniedException"));
            assertThat(org.elasticsearch.ExceptionsHelper.stackTrace(item.getFailure().getCause()), containsString("could not stage"));
        }
        assertThat("the local file of the failed bulk is gone", localFiles(), equalTo(1L));
        assertThat(stagedFiles(), hasSize(1));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(100L));

        assertBulkOk(bulk(200, 300));
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));

        // the rows of the failed bulk are not in the table, and the rows of the other bulks are, once
        final TreeSet<Long> counts = uploadedCounts();
        assertThat(counts.size(), equalTo(200));
        assertFalse(counts.contains(150L));
        assertTrue(counts.contains(99L));
        assertTrue(counts.contains(250L));
        assertThat(stagedFiles(), hasSize(0));
    }

    /**
     * A throttled request or a reset connection fails the upload of a staged copy for a moment. The engine tries it three times, with
     * waits of 250 ms and 1 s, so a failure that ends within a quarter of a second must not fail the bulk.
     */
    public void testAStagedUploadThatFailsForAMomentIsTriedAgainAndTheBulkIsAcknowledged() throws Exception {
        assumePosixPermissions();
        registerRepository();
        createIndex();
        assertBulkOk(bulk(0, 100));

        failStaging(true);
        final BulkResponse[] response = new BulkResponse[1];
        startInParallel(2, task -> {
            if (task == 0) {
                response[0] = bulk(100, 200);
            } else {
                safeSleep(100);
                try {
                    failStaging(false);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
        });

        assertBulkOk(response[0]);
        assertThat(stagedFiles(), hasSize(2));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(200L));
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        assertThat(uploadedCounts().size(), equalTo(200));
        assertThat(stagedFiles(), hasSize(0));
    }

    /**
     * A flush takes a local file only after its range is processed, and with staged durability that is after the staged copy is
     * uploaded. Otherwise a flush could put the rows of a bulk in the table while the upload is being tried again, and when every
     * try fails the client is told that the bulk failed and sends it again, which indexes the rows twice. The flushes here run all the
     * time while a bulk is failing, which takes over a second because of the waits between the tries.
     */
    public void testAFlushNeverTakesTheFileOfABulkThatFailsToStage() throws Exception {
        assumePosixPermissions();
        registerRepository();
        createIndex();
        assertBulkOk(bulk(0, 100));

        failStaging(true);
        final BulkResponse[] response = new BulkResponse[1];
        final java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();
        final java.util.concurrent.atomic.AtomicInteger flushes = new java.util.concurrent.atomic.AtomicInteger();
        try {
            startInParallel(2, task -> {
                if (task == 0) {
                    try {
                        response[0] = bulk(100, 200);
                    } finally {
                        done.set(true);
                    }
                } else {
                    while (done.get() == false) {
                        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
                        flushes.incrementAndGet();
                    }
                }
            });
        } finally {
            failStaging(false);
        }

        assertTrue(response[0].hasFailures());
        for (BulkItemResponse item : response[0].getItems()) {
            assertThat(item.getFailure().getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
        assertThat("flushes ran while the bulk was being tried", flushes.get(), greaterThan(1));
        assertThat(indicesAdmin().prepareFlush(INDEX).get().getFailedShards(), equalTo(0));
        final TreeSet<Long> counts = uploadedCounts();
        assertThat("only the rows of the bulk that was acknowledged are in the table", counts.size(), equalTo(100));
        assertTrue(counts.contains(99L));
        assertFalse("a row of the failed bulk is in the table", counts.contains(150L));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(100L));
    }

    private static void assumePosixPermissions() {
        assumeTrue(
            "needs POSIX permissions",
            org.elasticsearch.core.PathUtils.getDefaultFileSystem().supportedFileAttributeViews().contains("posix")
        );
        assumeFalse("root ignores permissions", "root".equals(System.getProperty("user.name")));
    }

    public void testAMissingRepositoryFailsTheBulkBeforeAnythingIsWritten() throws Exception {
        // the repository of the index is not registered yet
        createIndex();

        final BulkResponse failed = bulk(0, 100);

        assertTrue(failed.hasFailures());
        for (BulkItemResponse item : failed.getItems()) {
            assertThat(item.getFailure().getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
        assertThat(localFiles(), equalTo(0L));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(0L));

        registerRepository();
        assertBulkOk(bulk(100, 200));
        assertThat(stagedFiles(), hasSize(1));
        assertThat(indicesAdmin().prepareStats(INDEX).get().getTotal().getDocs().getCount(), equalTo(100L));
    }

    public void testConcurrentBulksToOneShardAreAllStaged() throws Exception {
        registerRepository();
        createIndex();
        final int threads = between(3, 6);
        final List<Thread> workers = new ArrayList<>();
        final List<BulkResponse> responses = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int t = 0; t < threads; t++) {
            final int from = t * 500;
            final Thread worker = new Thread(() -> responses.add(bulk(from, from + 500)));
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        assertThat(responses, hasSize(threads));
        for (BulkResponse response : responses) {
            assertBulkOk(response);
        }
        final List<Path> staged = stagedFiles();
        assertThat(staged.size(), greaterThan(0));
        assertThat(countsOf(staged).size(), equalTo(threads * 500));
        assertThat((long) staged.size(), equalTo(localFiles()));
        assertThat(LocalParquetFiles.TEMP_PREFIX, containsString("pqtmp"));
    }
}
