/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.compaction;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.TestLocalFiles;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.flush.ParquetFlusher;
import org.elasticsearch.xpack.parquet.write.flush.ShardCommitData;
import org.elasticsearch.xpack.parquet.write.table.CatalogEntry;
import org.elasticsearch.xpack.parquet.write.table.CatalogRegistration;
import org.elasticsearch.xpack.parquet.write.table.RepositoryCatalog;
import org.elasticsearch.xpack.parquet.write.table.RepositoryFileIO;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.elasticsearch.xpack.parquet.write.table.TestTables;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetCompactorTests.IcebergWorkerPoolFilter.class }
)
public class ParquetCompactorTests extends ESTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final TableLocation LOCATION = new TableLocation("file:///warehouse/logs", BlobPath.EMPTY.add("logs"));
    private static final long HOUR = 3_600_000L;

    private FsBlobStore store;
    private final List<Shard> shards = new ArrayList<>();

    /** The shard of the tests: its store directory, its commit, and its handle on the shared table. */
    private final class Shard {
        final int id;
        final Directory directory = new ByteBuffersDirectory();
        final Map<String, String> commit = new HashMap<>(Map.of("history_uuid", "abc", "local_checkpoint", "-1", "max_seq_no", "-1"));

        Shard(int id) {
            this.id = id;
            shards.add(this);
        }

        ShardTable table() {
            return TestTables.open(store, LOCATION, id, registration());
        }

        /** What a restart does: the temporary files go, and the table is opened again. */
        ParquetCompactor compactor(ParquetCompactor.Settings settings, ParquetCompactor.Hooks hooks) throws IOException {
            LocalParquetFiles.deleteTemporaryFiles(directory);
            return new ParquetCompactor(directory, table(), () -> settings, hooks);
        }

        /** Writes the rows as one local file and uploads it as one file of the table. */
        void flush(long from, long to) throws IOException {
            TestLocalFiles.write(directory, from, to, false);
            final ShardCommitData commits = new ShardCommitData() {
                @Override
                public Map<String, String> userData() {
                    return new HashMap<>(commit);
                }

                @Override
                public void commit(Map<String, String> userData) {
                    commit.clear();
                    commit.putAll(userData);
                }
            };
            new ParquetFlusher(
                directory,
                commits,
                table(),
                () -> new ParquetFlusher.Settings(1L << 30, 1 << 16, CompressionCodecName.SNAPPY, false),
                () -> Long.MAX_VALUE,
                ParquetFlusher.Hooks.NONE
            ).flush();
        }

        /** Flushes {@code files} files of {@code rowsEach} rows each, one after the other. */
        void flushMany(int files, int rowsEach) throws IOException {
            for (int i = 0; i < files; i++) {
                flush((long) i * rowsEach, (long) (i + 1) * rowsEach - 1);
            }
        }

        Set<String> blobs() throws IOException {
            return container().listBlobs(RepositoryFileIO.PURPOSE).keySet();
        }

        /** The one blob of the current history for the range, whatever its upload attempt. */
        String blobOf(long from, long to) throws IOException {
            final String prefix = String.format(java.util.Locale.ROOT, "%019d-%019d-", from, to);
            final List<String> found = blobs().stream().filter(name -> name.startsWith(prefix)).toList();
            assertThat("blobs of the range [" + from + ", " + to + "]", found, hasSize(1));
            return found.get(0);
        }

        BlobContainer container() {
            return store.blobContainer(BlobPath.EMPTY.add("logs").add("data").add("shard-" + id).add(TestTables.HISTORY));
        }

        List<String> compactedFiles() throws IOException {
            final List<String> names = new ArrayList<>();
            for (String name : directory.listAll()) {
                if (name.startsWith(LocalParquetFiles.COMPACTED_PREFIX)) {
                    names.add(name);
                }
            }
            return names;
        }

        /** Every row 0 to rows-1 is in the active files exactly once, and the rows of the files in their order are 0 to rows-1. */
        void assertActiveFilesHoldRowsInOrder(long rows) throws IOException {
            final List<Long> inOrder = new ArrayList<>();
            for (ShardTable.UploadedFile file : table().uploadedFiles()) {
                try (var in = TestTables.read(store, LOCATION, file.location())) {
                    final TestParquetBytes.Contents contents = TestParquetBytes.read(in.readAllBytes());
                    assertThat(contents.rows().size(), equalTo((int) file.rows()));
                    for (var row : contents.rows()) {
                        inOrder.add(row.getLong("count", 0));
                    }
                }
            }
            assertThat(inOrder.size(), equalTo((int) rows));
            for (int i = 0; i < inOrder.size(); i++) {
                assertThat("the row order of the shard is kept", inOrder.get(i), equalTo((long) i));
            }
        }
    }

    private static final String BASE_URI = "file:///warehouse";
    private static final List<String> NAMESPACE = List.of("default");

    private RepositoryCatalog catalog() {
        return new RepositoryCatalog(store, BlobPath.EMPTY, BASE_URI);
    }

    private CatalogRegistration registration() {
        return new CatalogRegistration(catalog(), NAMESPACE, "logs", "logs", "index-uuid", "cluster-uuid");
    }

    private int newestMetadataVersion() throws IOException {
        int newest = 0;
        for (String name : store.blobContainer(BlobPath.EMPTY.add("logs").add("metadata")).listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            final var matcher = java.util.regex.Pattern.compile("v(\\d+)\\.metadata\\.json").matcher(name);
            if (matcher.matches()) {
                newest = Math.max(newest, Integer.parseInt(matcher.group(1)));
            }
        }
        return newest;
    }

    private static ParquetCompactor.Settings settings(long targetBytes, long delayMillis) {
        return new ParquetCompactor.Settings(targetBytes, 2, 1L << 30, 1 << 16, CompressionCodecName.SNAPPY, delayMillis);
    }

    private static ParquetCompactor.Hooks crashAt(String crashStep) {
        return step -> {
            if (step.equals(crashStep)) {
                throw new IOException("crash after [" + step + "]");
            }
        };
    }

    @Before
    public void createStore() throws Exception {
        store = new FsBlobStore(4096, createTempDir(), false);
    }

    @After
    public void closeStore() throws Exception {
        store.close();
        for (Shard shard : shards) {
            shard.directory.close();
        }
    }

    private static ShardTable.ShardFile file(long from, long to, long bytes) {
        return new ShardTable.ShardFile(
            new ShardTable.UploadedFile(
                "file:///warehouse/logs/data/shard-0/" + TestTables.HISTORY + "/" + ShardTable.dataFileName(from, to, "attempt"),
                from,
                to,
                to - from + 1,
                bytes
            ),
            null
        );
    }

    private static List<List<long[]>> ranges(List<List<ShardTable.ShardFile>> runs) {
        final List<List<long[]>> result = new ArrayList<>();
        for (List<ShardTable.ShardFile> run : runs) {
            final List<long[]> ranges = new ArrayList<>();
            for (ShardTable.ShardFile file : run) {
                ranges.add(new long[] { file.file().fromSeqNo(), file.file().toSeqNo() });
            }
            result.add(ranges);
        }
        return result;
    }

    // ---- the policy ----

    public void testTheRunsAreAdjacentSmallFilesAndALargeFileEndsARun() {
        // target 100: below 50 is small
        final List<ShardTable.ShardFile> files = List.of(
            file(0, 9, 10),
            file(10, 19, 10),
            file(20, 29, 10),
            file(30, 99, 80), // large: ends the run
            file(100, 109, 10), // alone: fewer than min_files
            file(110, 119, 80), // large
            file(120, 129, 10),
            file(130, 139, 10)
        );

        final List<List<long[]>> runs = ranges(ParquetCompactor.chooseRuns(files, 100, 1L << 30, 2));

        assertThat(runs, hasSize(2));
        assertThat(runs.get(0).get(0), equalTo(new long[] { 0, 9 }));
        assertThat(runs.get(0), hasSize(3));
        assertThat(runs.get(1).get(0), equalTo(new long[] { 120, 129 }));
        assertThat(runs.get(1), hasSize(2));
    }

    public void testTheMaximumSizeSplitsARunAndASmallRestIsLeftAlone() {
        final List<ShardTable.ShardFile> files = List.of(
            file(0, 9, 10),
            file(10, 19, 10),
            file(20, 29, 10),
            file(30, 39, 10),
            file(40, 49, 10)
        );

        // room for two files: runs of two, and the fifth file is alone
        final List<List<long[]>> runs = ranges(ParquetCompactor.chooseRuns(files, 1000, 25, 2));

        assertThat(runs, hasSize(2));
        assertThat(runs.get(0), hasSize(2));
        assertThat(runs.get(1), hasSize(2));
        assertThat(runs.get(1).get(1), equalTo(new long[] { 30, 39 }));
    }

    // ---- the compaction ----

    public void testSmallFilesAreMergedIntoOneThatKeepsTheRowsInOrder() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(4, 100);
        assertThat(shard.table().uploadedFiles(), hasSize(4));

        final ParquetCompactor.Result result = shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE).compact();

        assertThat(result.runs(), equalTo(1));
        assertThat(result.replacedFiles(), equalTo(4));
        assertThat(result.rows(), equalTo(400L));
        final List<ShardTable.UploadedFile> files = shard.table().uploadedFiles();
        assertThat(files, hasSize(1));
        assertThat(files.get(0).fromSeqNo(), equalTo(0L));
        assertThat(files.get(0).toSeqNo(), equalTo(399L));
        assertThat(files.get(0).rows(), equalTo(400L));
        shard.assertActiveFilesHoldRowsInOrder(400);
        assertThat(shard.compactedFiles(), empty());
        assertThat(shard.table().uploadedThroughSeqNo(), equalTo(399L));
    }

    public void testTheCatalogEntryNamesTheNewestMetadataAfterEachFlushAndAfterACompaction() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(4, 50);
        final CatalogEntry afterFlushes = catalog().read(NAMESPACE, "logs").orElseThrow();
        assertThat("a flush updates the entry", afterFlushes.metadataVersion(), equalTo(newestMetadataVersion()));

        shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE).compact();

        // the rewrite of the compaction (and the expiry of snapshots after it) are commits of the table too
        final CatalogEntry afterCompaction = catalog().read(NAMESPACE, "logs").orElseThrow();
        assertThat(afterCompaction.metadataVersion(), greaterThan(afterFlushes.metadataVersion()));
        assertThat(afterCompaction.metadataVersion(), equalTo(newestMetadataVersion()));
        assertThat(afterCompaction.tableUuid(), equalTo(afterFlushes.tableUuid()));
        assertThat(catalog().load(NAMESPACE, "logs").orElseThrow().metadataVersion(), equalTo(newestMetadataVersion()));
    }

    public void testFilesThatAreLargeEnoughAreLeftAlone() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(3, 100);

        final ParquetCompactor.Result result = shard.compactor(settings(1, HOUR), ParquetCompactor.Hooks.NONE).compact();

        assertThat(result.runs(), equalTo(0));
        assertThat(shard.table().uploadedFiles(), hasSize(3));
    }

    public void testAGapInTheRunsKeepsTheOrderOfTheRows() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(5, 100);
        final List<ShardTable.UploadedFile> before = shard.table().uploadedFiles();
        // the third file is large for this target and the others are not
        final long target = before.get(2).bytes() * 2 - 1;
        final ParquetCompactor.Settings settings = new ParquetCompactor.Settings(
            target,
            2,
            1L << 30,
            1 << 16,
            CompressionCodecName.SNAPPY,
            HOUR
        );
        // all five files have about the same size, so make the run small instead: only the first two fit
        final ParquetCompactor.Settings two = new ParquetCompactor.Settings(
            Long.MAX_VALUE / 4,
            2,
            before.get(0).bytes() + before.get(1).bytes() + 1,
            1 << 16,
            CompressionCodecName.SNAPPY,
            HOUR
        );
        assertNotNull(settings);

        final ParquetCompactor.Result result = shard.compactor(two, ParquetCompactor.Hooks.NONE).compact();

        assertThat(result.runs(), equalTo(2));
        final List<ShardTable.UploadedFile> files = shard.table().uploadedFiles();
        assertThat(files, hasSize(3));
        shard.assertActiveFilesHoldRowsInOrder(500);
    }

    // ---- crashes ----

    public void testACrashAtAnyStepLosesNoRowAndTheNextCompactionFinishes() throws Exception {
        for (String step : List.of("merged", "uploaded", "rewritten", "cleaned")) {
            final Shard shard = new Shard(shards.size());
            shard.flushMany(4, 50);

            expectThrows(IOException.class, () -> shard.compactor(settings(1L << 30, HOUR), crashAt(step)).compact());
            // whatever the step was, the table holds every row exactly once
            shard.assertActiveFilesHoldRowsInOrder(200);

            // the restart: the same compaction runs again (or has nothing to do, after the switch)
            shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE).compact();

            final List<ShardTable.UploadedFile> files = shard.table().uploadedFiles();
            assertThat("after a crash at [" + step + "]", files, hasSize(1));
            shard.assertActiveFilesHoldRowsInOrder(200);
            assertThat(shard.compactedFiles(), empty());
        }
    }

    public void testTheOutputOfACompactionThatCrashedBeforeItsCommitIsDeletedWhenAnotherRunReplacesIt() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(4, 50);
        expectThrows(IOException.class, () -> shard.compactor(settings(1L << 30, HOUR), crashAt("uploaded")).compact());
        final String orphan = shard.blobOf(0, 199);
        assertThat("the output was uploaded and no snapshot lists it", shard.blobs(), org.hamcrest.Matchers.hasItem(orphan));

        // another file arrives, so the next run covers a different range and the orphan is not overwritten
        shard.flush(200, 249);
        final ParquetCompactor.Result result = shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE).compact();

        assertThat(result.runs(), equalTo(1));
        assertThat(result.orphansDeleted(), equalTo(1));
        assertThat(shard.blobs(), not(org.hamcrest.Matchers.hasItem(orphan)));
        shard.assertActiveFilesHoldRowsInOrder(250);
    }

    public void testAnUploadAboveTheHighWaterMarkIsNotAnOrphan() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(2, 50);
        // the upload of a flush that has not appended its file yet: it is in the repository, and no snapshot lists it
        final ShardTable table = shard.table();
        table.upload(100, 149, "attempt-x", new java.io.ByteArrayInputStream(new byte[] { 1, 2, 3 }), 3);

        final ParquetCompactor.Result result = shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE).compact();

        assertThat(result.orphansDeleted(), equalTo(0));
        assertThat(shard.blobs(), org.hamcrest.Matchers.hasItem(ShardTable.dataFileName(100, 149, "attempt-x")));
    }

    // ---- readers and deletion ----

    public void testReplacedFilesStayReadableForTheDelayAndAreDeletedAfterIt() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(4, 50);
        final List<String> originals = new ArrayList<>();
        for (ShardTable.UploadedFile file : shard.table().uploadedFiles()) {
            originals.add(file.location().substring(file.location().lastIndexOf('/') + 1));
        }

        shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE).compact();

        // a reader that holds the snapshot from before the compaction can still read every file of it
        for (String original : originals) {
            assertThat(shard.blobs(), org.hamcrest.Matchers.hasItem(original));
            try (var in = shard.container().readBlob(RepositoryFileIO.PURPOSE, original)) {
                assertThat(TestParquetBytes.read(in.readAllBytes()).rows().size(), equalTo(50));
            }
        }

        // a later flush and compaction with no delay: the snapshots before the compaction expire, and so do their files
        shard.flush(200, 249);
        shard.compactor(settings(1L << 30, 0), ParquetCompactor.Hooks.NONE).compact();

        for (String original : originals) {
            assertThat("replaced and expired", shard.blobs(), not(org.hamcrest.Matchers.hasItem(original)));
        }
        shard.assertActiveFilesHoldRowsInOrder(250);
    }

    // ---- concurrency ----

    public void testAFlushThatAppendsDuringACompactionDoesNotBreakItsCommit() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(4, 50);
        final AtomicReference<IOException> failure = new AtomicReference<>();

        shard.compactor(settings(1L << 30, HOUR), step -> {
            if (step.equals("uploaded")) {
                // the rewrite has not committed yet: a flush appends its file in front of it
                try {
                    shard.flush(200, 249);
                } catch (IOException e) {
                    failure.set(e);
                }
            }
        }).compact();

        assertNull(failure.get());
        final List<ShardTable.UploadedFile> files = shard.table().uploadedFiles();
        assertThat(files, hasSize(2));
        assertThat(files.get(0).toSeqNo(), equalTo(199L));
        assertThat(files.get(1).fromSeqNo(), equalTo(200L));
        shard.assertActiveFilesHoldRowsInOrder(250);
    }

    /**
     * Iceberg validates a rewrite that it retries after another commit against the history since the snapshot that the rewrite was
     * planned from. Once snapshots have expired the history has a gap, and a rewrite that does not name its snapshot fails with "cannot
     * determine history". This is what a flush that appends during a compaction does to a table with a delay of zero.
     */
    public void testARewriteThatIsRetriedAfterSnapshotsExpiredStillCommits() throws Exception {
        final Shard shard = new Shard(0);
        shard.flushMany(4, 50);
        shard.compactor(settings(1L << 30, 0), ParquetCompactor.Hooks.NONE).compact();
        shard.flush(200, 249);
        shard.compactor(settings(1L << 30, 0), ParquetCompactor.Hooks.NONE).compact(); // expires the snapshots before it
        shard.flush(250, 299);
        shard.flush(300, 349);
        final AtomicReference<IOException> failure = new AtomicReference<>();

        shard.compactor(settings(1L << 30, 0), step -> {
            if (step.equals("uploaded")) {
                try {
                    shard.flush(350, 399);
                } catch (IOException e) {
                    failure.set(e);
                }
            }
        }).compact();

        assertNull(failure.get());
        final List<ShardTable.UploadedFile> files = shard.table().uploadedFiles();
        assertThat(files, hasSize(2));
        assertThat("the compaction was not dropped", files.get(0).toSeqNo(), equalTo(349L));
        shard.assertActiveFilesHoldRowsInOrder(400);
    }

    public void testShardsCompactTheirFilesOfOneTableAtTheSameTime() throws Exception {
        final int shardCount = between(2, 4);
        final List<Shard> list = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            final Shard shard = new Shard(i);
            shard.flushMany(5, 40);
            list.add(shard);
        }
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        final List<Exception> failures = java.util.Collections.synchronizedList(new ArrayList<>());
        for (Shard shard : list) {
            final ParquetCompactor compactor = shard.compactor(settings(1L << 30, HOUR), ParquetCompactor.Hooks.NONE);
            final Thread thread = new Thread(() -> {
                try {
                    start.await();
                    compactor.compact();
                } catch (Exception e) {
                    failures.add(e);
                }
            });
            threads.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(failures, empty());
        for (Shard shard : list) {
            assertThat("shard " + shard.id, shard.table().uploadedFiles(), hasSize(1));
            shard.assertActiveFilesHoldRowsInOrder(200);
        }
        assertThat(new TreeSet<>(List.of(1)), contains(1));
    }
}
