/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.flush;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.TestLocalFiles;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
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
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlusherTests.IcebergWorkerPoolFilter.class }
)
public class ParquetFlusherTests extends ESTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final TableLocation LOCATION = new TableLocation("file:///warehouse/logs", BlobPath.EMPTY.add("logs"));

    private FsBlobStore store;
    private Directory directory;
    private InMemoryCommitData commits;
    private ParquetFlusher.Settings settings;
    /** The threads of the merges of all the tests, and how many of them a merge uses (1 is the thread of the flush alone). */
    private ExecutorService mergeExecutor;
    private int mergeParallelism;
    /** The thread of the uploads, or null for a flusher that uploads on its own thread after each merge. */
    private ExecutorService uploadExecutor;
    /** What the engine reports as complete: every bulk with a range up to this has finished writing its local file. */
    private long completeThrough;

    /** The one commit of the shard: the keys of the server stay as they are. */
    private static final class InMemoryCommitData implements ShardCommitData {
        private Map<String, String> data = new HashMap<>(Map.of("history_uuid", "abc", "local_checkpoint", "-1", "max_seq_no", "-1"));

        @Override
        public Map<String, String> userData() {
            return new HashMap<>(data);
        }

        @Override
        public void commit(Map<String, String> userData) {
            data = new HashMap<>(userData);
        }
    }

    @Before
    public void createFixtures() throws Exception {
        store = new FsBlobStore(4096, createTempDir(), false);
        directory = new ByteBuffersDirectory();
        commits = new InMemoryCommitData();
        settings = new ParquetFlusher.Settings(1L << 30, 1 << 16, CompressionCodecName.SNAPPY, false);
        completeThrough = Long.MAX_VALUE;
        mergeExecutor = Executors.newFixedThreadPool(3);
        mergeParallelism = between(1, 4);
        uploadExecutor = randomBoolean() ? Executors.newFixedThreadPool(2) : null;
    }

    @After
    public void closeFixtures() throws Exception {
        mergeExecutor.shutdown();
        assertTrue(mergeExecutor.awaitTermination(30, TimeUnit.SECONDS));
        if (uploadExecutor != null) {
            uploadExecutor.shutdown();
            assertTrue(uploadExecutor.awaitTermination(30, TimeUnit.SECONDS));
        }
        store.close();
        directory.close();
    }

    /** The settings of the test, with the parallelism of the merges that this run of the test uses. */
    private ParquetFlusher.Settings flushSettings() {
        return new ParquetFlusher.Settings(
            settings.maxFileBytes(),
            settings.rowGroupBytes(),
            settings.codec(),
            settings.staged(),
            mergeParallelism
        );
    }

    /** What a restart does: the temporary files of unfinished writes go, and the table is opened again. */
    private ParquetFlusher newFlusher(ParquetFlusher.Hooks hooks) throws IOException {
        LocalParquetFiles.deleteTemporaryFiles(directory);
        return new ParquetFlusher(
            directory,
            commits,
            TestTables.open(store, LOCATION, 0),
            this::flushSettings,
            () -> completeThrough,
            hooks,
            ParquetFlusher.RunListener.NONE,
            mergeExecutor,
            uploadExecutor,
            () -> () -> {}
        );
    }

    /** A flusher with a listener, which is told after each committed run. */
    private ParquetFlusher newFlusher(ParquetFlusher.Hooks hooks, ParquetFlusher.RunListener listener) throws IOException {
        LocalParquetFiles.deleteTemporaryFiles(directory);
        return new ParquetFlusher(
            directory,
            commits,
            TestTables.open(store, LOCATION, 0),
            this::flushSettings,
            () -> completeThrough,
            hooks,
            listener,
            mergeExecutor,
            uploadExecutor,
            () -> () -> {}
        );
    }

    private static ParquetFlusher.Hooks crashAt(String crashStep) {
        return step -> {
            if (step.equals(crashStep)) {
                throw new IOException("crash after [" + step + "]");
            }
        };
    }

    private List<String> localFileNames() throws IOException {
        final List<String> names = new ArrayList<>();
        for (String name : directory.listAll()) {
            if (name.startsWith("pqlocal_") || name.startsWith("pqmerged_") || name.startsWith(LocalParquetFiles.TEMP_PREFIX)) {
                names.add(name);
            }
        }
        return names;
    }

    /** Every row 0 to rows-1 is in the files of the table exactly once, in files whose ranges do not overlap. */
    private void assertTableHasRowsExactlyOnce(long rows) throws IOException {
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        final TreeSet<Long> seen = new TreeSet<>();
        long expectedFrom = 0;
        for (ShardTable.UploadedFile file : table.uploadedFiles()) {
            assertThat("ranges of the files are contiguous", file.fromSeqNo(), equalTo(expectedFrom));
            expectedFrom = file.toSeqNo() + 1;
            try (var in = TestTables.read(store, LOCATION, file.location())) {
                final TestParquetBytes.Contents contents = TestParquetBytes.read(in.readAllBytes());
                assertThat(contents.rows().size(), equalTo((int) file.rows()));
                for (var row : contents.rows()) {
                    assertTrue("row " + row.getLong("count", 0) + " appears once", seen.add(row.getLong("count", 0)));
                }
            }
        }
        assertThat(seen.size(), equalTo((int) rows));
        assertThat(seen.first(), equalTo(0L));
        assertThat(seen.last(), equalTo(rows - 1));
    }

    private void assertFlushed(long rows) throws IOException {
        assertTableHasRowsExactlyOnce(rows);
        final Map<String, String> data = commits.userData();
        assertThat(data.get(ShardCommitData.PENDING_UPLOAD), nullValue());
        assertThat(ShardCommitData.uploadedThroughSeqNo(data), equalTo(rows - 1));
        assertThat(ShardCommitData.uploadedRows(data), equalTo(rows));
        assertThat(localFileNames(), hasSize(0));
        // the keys of the server are untouched
        assertThat(data.get("history_uuid"), equalTo("abc"));
        assertThat(data.get("max_seq_no"), equalTo("-1"));
    }

    public void testFlushMergesAllLocalFilesIntoOneFileOfTheTable() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        TestLocalFiles.write(directory, 100, 249, false);
        TestLocalFiles.write(directory, 250, 299, false);

        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(result.files(), equalTo(1));
        assertThat(result.rows(), equalTo(300L));
        assertFlushed(300);
        assertThat(TestTables.open(store, LOCATION, 0).uploadedFiles(), hasSize(1));
    }

    public void testAFlushWithNothingToDoDoesNothing() throws Exception {
        final ParquetFlusher flusher = newFlusher(ParquetFlusher.Hooks.NONE);

        final ParquetFlusher.Result result = flusher.flush();

        assertThat(result.files(), equalTo(0));
        assertThat(commits.userData().get(ShardCommitData.UPLOADED_THROUGH_SEQ_NO), nullValue());
        assertThat(TestTables.open(store, LOCATION, 0).uploadedFiles(), hasSize(0));
    }

    /**
     * Bulks write their files in parallel, so the file of a higher range can exist while a lower one is still being written. A flush
     * that took the higher file would record its range as uploaded, and then delete the lower file as a leftover when it appears.
     */
    public void testAFileAboveTheCompleteBoundStaysForTheNextFlushSoALateLowerFileIsNeverLost() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        TestLocalFiles.write(directory, 200, 299, false); // the bulk of the range 100 to 199 is still writing
        completeThrough = 99;

        final ParquetFlusher.Result first = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(first.files(), equalTo(1));
        assertThat(first.rows(), equalTo(100L));
        assertThat(ShardCommitData.uploadedThroughSeqNo(commits.userData()), equalTo(99L));
        assertThat("the file above the bound is untouched", localFileNames(), hasSize(1));

        TestLocalFiles.write(directory, 100, 199, false); // the late bulk finishes
        completeThrough = 299;

        final ParquetFlusher.Result second = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(second.rows(), equalTo(200L));
        assertFlushed(300);
    }

    public void testAFlushWithOnlyFilesAboveTheCompleteBoundDoesNothing() throws Exception {
        TestLocalFiles.write(directory, 100, 199, false);
        completeThrough = 50;

        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(result.files(), equalTo(0));
        assertThat(commits.userData().get(ShardCommitData.UPLOADED_THROUGH_SEQ_NO), nullValue());
        assertThat(localFileNames(), hasSize(1));
        assertThat(TestTables.open(store, LOCATION, 0).uploadedFiles(), hasSize(0));
    }

    /**
     * A flush of several runs takes minutes. The engine counts the local bytes for its backpressure, and it must learn about each run when
     * the run is committed, and not when the whole flush is over, or it keeps rejecting bulks for the space of files that are gone.
     */
    public void testTheListenerIsToldAfterEachRunWithTheStateOfThatRun() throws Exception {
        TestLocalFiles.write(directory, 0, 499, false);
        TestLocalFiles.write(directory, 500, 999, false);
        TestLocalFiles.write(directory, 1000, 1499, false);
        settings = new ParquetFlusher.Settings(1, 1 << 16, CompressionCodecName.SNAPPY, false); // a run for each file
        // with a background upload the next run is already merged when the listener is told, and the test counts the merged files too
        uploadExecutor = null;

        final List<String> seen = new ArrayList<>();
        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE, () -> {
            final Map<String, String> data = commits.userData();
            seen.add(
                "through="
                    + ShardCommitData.uploadedThroughSeqNo(data)
                    + " rows="
                    + ShardCommitData.uploadedRows(data)
                    + " pending="
                    + data.get(ShardCommitData.PENDING_UPLOAD)
                    + " local="
                    + localFileNames().size()
            );
        }).flush();

        assertThat(result.files(), equalTo(3));
        assertThat(
            seen,
            contains(
                "through=499 rows=500 pending=null local=2",
                "through=999 rows=1000 pending=null local=1",
                "through=1499 rows=1500 pending=null local=0"
            )
        );
        assertFlushed(1500);
    }

    // ---- the upload of a run goes on in the background while the next run is merged ----

    private void useBackgroundUploads() {
        if (uploadExecutor != null) {
            uploadExecutor.shutdown();
        }
        uploadExecutor = Executors.newFixedThreadPool(2);
    }

    /** Runs {@code action} when the {@code count}th time that the flush reaches {@code step}, on the thread that reaches it. */
    private static ParquetFlusher.Hooks onStep(String step, int count, ParquetFlusher.Hooks action) {
        final AtomicInteger reached = new AtomicInteger();
        return reachedStep -> {
            if (reachedStep.equals(step) && reached.incrementAndGet() == count) {
                action.reached(reachedStep);
            }
        };
    }

    private static ParquetFlusher.Hooks both(ParquetFlusher.Hooks first, ParquetFlusher.Hooks second) {
        return step -> {
            first.reached(step);
            second.reached(step);
        };
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (latch.await(30, TimeUnit.SECONDS) == false) {
                throw new IOException("the test did not release the latch");
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * The second run is merged while the first one uploads, and its marker is written only after the first one is in the table: there is
     * one marker, so a crash can always finish the run that the marker names.
     */
    public void testTheNextRunIsMergedWhileTheRunBeforeItUploadsAndItsMarkerWaitsForIt() throws Exception {
        useBackgroundUploads();
        TestLocalFiles.write(directory, 0, 499, false);
        TestLocalFiles.write(directory, 500, 999, false);
        settings = new ParquetFlusher.Settings(1, 1 << 16, CompressionCodecName.SNAPPY, false); // a run for each file
        final CountDownLatch secondMerged = new CountDownLatch(1);
        final ParquetFlusher.Hooks hooks = both(onStep("merged", 2, step -> secondMerged.countDown()), onStep("uploaded", 1, step -> {
            // the upload of the first run does not end before the second run is merged
            await(secondMerged);
        }));

        final ParquetFlusher.Result result = newFlusher(
            both(
                hooks,
                onStep(
                    "pending",
                    2,
                    step -> assertThat(
                        "the first run is in the table when the marker of the second is written",
                        ShardCommitData.uploadedThroughSeqNo(commits.userData()),
                        equalTo(499L)
                    )
                )
            )
        ).flush();

        assertThat(result.files(), equalTo(2));
        assertFlushed(1000);
    }

    /** A flush that does not wait returns while its last upload runs, and the files of that run stay until it has finished. */
    public void testAFlushThatDoesNotWaitReturnsWhileTheLastUploadRuns() throws Exception {
        useBackgroundUploads();
        final LocalParquetFiles.LocalFile file = TestLocalFiles.write(directory, 0, 499, false);
        final CountDownLatch release = new CountDownLatch(1);
        final ParquetFlusher flusher = newFlusher(onStep("uploaded", 1, step -> await(release)));

        final ParquetFlusher.Result result = flusher.flush(false);

        assertThat(result.files(), equalTo(1));
        assertThat(flusher.uploadingLocalBytes(), equalTo(file.length()));
        assertThat(ShardCommitData.uploadedThroughSeqNo(commits.userData()), equalTo(-1L));
        assertThat("the marker is in the commit data", commits.userData().get(ShardCommitData.PENDING_UPLOAD), not(nullValue()));
        assertThat("the local file stays", localFileNames(), hasSize(2)); // the file and the merged file
        release.countDown();
        flusher.flush(true); // nothing new: waits for the upload
        assertThat(flusher.uploadingLocalBytes(), equalTo(0L));
        assertFlushed(500);
    }

    /** The next flush merges its own files while the upload of the earlier one runs, and it never takes the files of that upload. */
    public void testTheNextFlushMergesWhileTheUploadOfTheEarlierOneRunsAndDoesNotTakeItsFiles() throws Exception {
        useBackgroundUploads();
        TestLocalFiles.write(directory, 0, 499, false);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch secondMerged = new CountDownLatch(1);
        final ParquetFlusher flusher = newFlusher(
            both(onStep("uploaded", 1, step -> await(release)), onStep("merged", 2, step -> secondMerged.countDown()))
        );
        flusher.flush(false);
        TestLocalFiles.write(directory, 500, 999, false);

        final AtomicReference<ParquetFlusher.Result> second = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                second.set(flusher.flush(false));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        thread.start();
        try {
            assertTrue("the second flush merged while the first upload runs", secondMerged.await(30, TimeUnit.SECONDS));
            release.countDown();
            thread.join(30_000);
            assertFalse(thread.isAlive());
        } finally {
            release.countDown();
        }
        assertThat("only its own file", second.get().rows(), equalTo(500L));
        flusher.flush(true);
        assertFlushed(1000);
    }

    /** A flush that waits also waits for the upload that an earlier flush left running. */
    public void testAFlushThatWaitsWaitsForTheUploadOfAnEarlierFlush() throws Exception {
        useBackgroundUploads();
        TestLocalFiles.write(directory, 0, 499, false);
        final CountDownLatch release = new CountDownLatch(1);
        final ParquetFlusher flusher = newFlusher(onStep("uploaded", 1, step -> await(release)));
        flusher.flush(false);

        final AtomicBoolean finished = new AtomicBoolean();
        final Thread thread = new Thread(() -> {
            try {
                flusher.flush(true);
                finished.set(true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        thread.start();
        try {
            thread.join(300);
            assertTrue("it waits for the upload", thread.isAlive());
            assertFalse(finished.get());
        } finally {
            release.countDown();
        }
        thread.join(30_000);
        assertTrue(finished.get());
        assertFlushed(500);
    }

    /** An upload that fails in the background is logged, and the next flush finishes the run from its marker. */
    public void testAnUploadThatFailsInTheBackgroundIsFinishedByTheNextFlush() throws Exception {
        useBackgroundUploads();
        TestLocalFiles.write(directory, 0, 499, false);
        final ParquetFlusher flusher = newFlusher(onStep("uploaded", 1, step -> { throw new IOException("the repository is down"); }));

        flusher.flush(false); // does not fail: the upload is not its business any more
        assertBusy(() -> assertThat(flusher.uploadingLocalBytes(), equalTo(0L)));
        assertThat("the run is not in the table", ShardCommitData.uploadedThroughSeqNo(commits.userData()), equalTo(-1L));

        flusher.flush(true);

        assertFlushed(500);
    }

    public void testAListenerThatFailsDoesNotFailAFlushThatIsCommitted() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        TestLocalFiles.write(directory, 100, 199, false);
        settings = new ParquetFlusher.Settings(1, 1 << 16, CompressionCodecName.SNAPPY, false);

        final int[] calls = new int[1];
        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE, () -> {
            calls[0]++;
            if (randomBoolean()) {
                throw new IOException("no");
            }
            throw new IllegalStateException("no");
        }).flush();

        assertThat(result.files(), equalTo(2));
        assertThat(calls[0], equalTo(2));
        assertFlushed(200);
    }

    /** A crash after the table listed the file but before the commit data moved: the recovery finishes the run. */
    public void testTheListenerIsToldWhenARecoveryFinishesARunThatACrashCutOff() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        expectThrows(IOException.class, () -> newFlusher(crashAt("appended")).flush());

        final int[] calls = new int[1];
        newFlusher(ParquetFlusher.Hooks.NONE, () -> calls[0]++).flush();

        assertThat("the recovery deletes the local files of the run, and the engine counts again", calls[0], equalTo(1));
        assertFlushed(100);
    }

    /** A crash after the commit data moved but before the local files were deleted: the next flush deletes them as leftovers. */
    public void testTheListenerIsToldWhenAFlushDeletesTheLeftoverFilesOfARunThatWasCommitted() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        expectThrows(IOException.class, () -> newFlusher(crashAt("committed")).flush());
        assertThat(
            "the local file is still there, with the merged file of the run",
            localFileNames().stream().filter(name -> name.startsWith("pqlocal_")).toList(),
            hasSize(1)
        );

        final int[] calls = new int[1];
        newFlusher(ParquetFlusher.Hooks.NONE, () -> calls[0]++).flush();

        assertThat(calls[0], equalTo(1));
        assertFlushed(100);
    }

    public void testTheListenerIsNotToldByAFlushThatHasNothingToDo() throws Exception {
        final int[] calls = new int[1];
        newFlusher(ParquetFlusher.Hooks.NONE, () -> calls[0]++).flush();
        assertThat(calls[0], equalTo(0));
    }

    public void testTheMaximumFileSizeSplitsTheFlushIntoSeveralFiles() throws Exception {
        final var a = TestLocalFiles.write(directory, 0, 499, false);
        final var b = TestLocalFiles.write(directory, 500, 999, false);
        TestLocalFiles.write(directory, 1000, 1499, false);
        // room for two of the three local files
        settings = new ParquetFlusher.Settings(a.length() + b.length() + 1, 1 << 16, CompressionCodecName.SNAPPY, false);

        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(result.files(), equalTo(2));
        assertFlushed(1500);
        final List<ShardTable.UploadedFile> files = TestTables.open(store, LOCATION, 0).uploadedFiles();
        assertThat(files, hasSize(2));
        assertThat(files.get(0).toSeqNo(), equalTo(999L));
    }

    public void testTheRunsHaveAboutTheSameSizeAndNeverLeaveATinyFile() {
        // ten files of 100 bytes and one of 5, with a maximum of 1,000: the usual case of a threshold that is close to the maximum
        final List<LocalParquetFiles.LocalFile> files = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            files.add(new LocalParquetFiles.LocalFile(LocalParquetFiles.name(i * 10L, i * 10L + 9), i * 10L, i * 10L + 9, 100));
        }
        files.add(new LocalParquetFiles.LocalFile(LocalParquetFiles.name(100, 109), 100, 109, 5));

        final List<List<LocalParquetFiles.LocalFile>> runs = ParquetFlusher.runs(files, 1_000);

        assertThat(runs, hasSize(2));
        long smallest = Long.MAX_VALUE;
        long largest = 0;
        for (List<LocalParquetFiles.LocalFile> run : runs) {
            final long bytes = run.stream().mapToLong(LocalParquetFiles.LocalFile::length).sum();
            assertTrue("a run is at most the maximum", bytes <= 1_000);
            smallest = Math.min(smallest, bytes);
            largest = Math.max(largest, bytes);
        }
        assertTrue("runs of " + smallest + " and " + largest + " bytes are not balanced", largest - smallest <= 200);
        // every file once, in order
        assertThat(runs.stream().mapToInt(List::size).sum(), equalTo(11));
        assertThat(runs.get(0).get(0).fromSeqNo(), equalTo(0L));
        assertThat(runs.get(1).get(runs.get(1).size() - 1).toSeqNo(), equalTo(109L));
    }

    public void testFilesThatFitInOneRunStayInOneRunAndALargeFileIsARunAlone() {
        final List<LocalParquetFiles.LocalFile> small = List.of(
            new LocalParquetFiles.LocalFile(LocalParquetFiles.name(0, 9), 0, 9, 100),
            new LocalParquetFiles.LocalFile(LocalParquetFiles.name(10, 19), 10, 19, 100)
        );
        assertThat(ParquetFlusher.runs(small, 1_000), hasSize(1));
        assertThat(ParquetFlusher.runs(List.of(), 1_000), hasSize(0));

        final List<LocalParquetFiles.LocalFile> large = List.of(
            new LocalParquetFiles.LocalFile(LocalParquetFiles.name(0, 9), 0, 9, 5_000),
            new LocalParquetFiles.LocalFile(LocalParquetFiles.name(10, 19), 10, 19, 100)
        );
        final List<List<LocalParquetFiles.LocalFile>> runs = ParquetFlusher.runs(large, 1_000);
        assertThat(runs, hasSize(2));
        assertThat(runs.get(0), hasSize(1));
    }

    public void testSeveralFlushesAppendFilesAndKeepTheCounts() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();
        TestLocalFiles.write(directory, 100, 249, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertFlushed(250);
        assertThat(TestTables.open(store, LOCATION, 0).uploadedFiles(), hasSize(2));
    }

    public void testAColumnAddedByAMappingUpdateGetsTheNextIdInLaterFiles() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();
        TestLocalFiles.write(directory, 100, 199, true);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertFlushed(200);
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        assertThat(table.schema().columns(), hasSize(4));
        assertThat(table.schema().columns().get(3).name(), equalTo("extra"));
        assertThat(table.schema().columns().get(3).fieldId(), equalTo(4));
    }

    // ---- a crash after each step, then a restart, then another bulk and a flush ----

    private void crashAfterThenRestart(String step) throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        TestLocalFiles.write(directory, 100, 249, false);

        final IOException crash = expectThrows(IOException.class, () -> newFlusher(crashAt(step)).flush());
        assertThat(crash.getMessage(), containsString(step));

        // a bulk that arrives while the node is down is impossible, but one that arrives after the restart is not
        TestLocalFiles.write(directory, 250, 299, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertFlushed(300);
    }

    public void testCrashAfterTheMerge() throws Exception {
        crashAfterThenRestart("merged");
    }

    public void testCrashAfterThePendingMarker() throws Exception {
        crashAfterThenRestart("pending");
    }

    public void testCrashAfterTheUpload() throws Exception {
        crashAfterThenRestart("uploaded");
    }

    public void testCrashAfterTheTableListedTheFile() throws Exception {
        crashAfterThenRestart("appended");
    }

    public void testCrashAfterTheCommitData() throws Exception {
        crashAfterThenRestart("committed");
    }

    public void testCrashAfterTheCleanup() throws Exception {
        crashAfterThenRestart("cleaned");
    }

    public void testAPendingFileThatIsLostBeforeItsUploadIsMergedAgain() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        TestLocalFiles.write(directory, 100, 249, false);
        expectThrows(IOException.class, () -> newFlusher(crashAt("pending")).flush());
        // the merged file is lost, for example with the disk of a crashed node that kept only the synced files
        directory.deleteFile(LocalParquetFiles.mergedName(0, 249));

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertFlushed(250);
    }

    public void testRecoveryDoesNotCountTheRowsTwice() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        // crash after the commit data: the rows are counted. The restart must not count them again.
        expectThrows(IOException.class, () -> newFlusher(crashAt("committed")).flush());
        assertThat(ShardCommitData.uploadedRows(commits.userData()), equalTo(100L));

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(ShardCommitData.uploadedRows(commits.userData()), equalTo(100L));
        assertFlushed(100);
    }

    public void testEveryCodecProducesAReadableFile() throws Exception {
        final CompressionCodecName codec = randomFrom(
            CompressionCodecName.UNCOMPRESSED,
            CompressionCodecName.SNAPPY,
            CompressionCodecName.ZSTD
        );
        settings = new ParquetFlusher.Settings(1L << 30, 1 << 16, codec, false);
        TestLocalFiles.write(directory, 0, 999, false);

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertFlushed(1000);
    }

    // ---- a shard that comes back with a new store: a new history, sequence numbers from 0, the same table ----

    /** The store of a shard with another history: its own directory and commit data, the same table. */
    private final class OtherHistory {
        final Directory directory = new ByteBuffersDirectory();
        final InMemoryCommitData commits = new InMemoryCommitData();

        ParquetFlusher flusher(ParquetFlusher.Hooks hooks) throws IOException {
            LocalParquetFiles.deleteTemporaryFiles(directory);
            return new ParquetFlusher(
                directory,
                commits,
                table(),
                ParquetFlusherTests.this::flushSettings,
                () -> Long.MAX_VALUE,
                hooks,
                ParquetFlusher.RunListener.NONE,
                mergeExecutor,
                uploadExecutor,
                () -> () -> {}
            );
        }

        ShardTable table() {
            return TestTables.open(store, LOCATION, 0, "history-b");
        }
    }

    private byte[] bytesOf(String location) throws IOException {
        try (var in = TestTables.read(store, LOCATION, location)) {
            return in.readAllBytes();
        }
    }

    public void testANewHistoryDoesNotReplaceTheFilesOfTheOldOne() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();
        final String oldLocation = TestTables.open(store, LOCATION, 0).uploadedFiles().get(0).location();
        final byte[] oldBytes = bytesOf(oldLocation);

        final OtherHistory other = new OtherHistory();
        TestLocalFiles.write(other.directory, 0, 99, false);
        other.flusher(ParquetFlusher.Hooks.NONE).flush();

        final List<ShardTable.UploadedFile> newFiles = other.table().uploadedFiles();
        assertThat(newFiles, hasSize(1));
        assertThat(newFiles.get(0).location(), not(equalTo(oldLocation)));
        assertThat("the file of the old history is untouched", bytesOf(oldLocation), equalTo(oldBytes));
        assertThat("the old history still lists its file", TestTables.open(store, LOCATION, 0).uploadedFiles(), hasSize(1));
        assertThat(ShardCommitData.uploadedRows(other.commits.userData()), equalTo(100L));
    }

    public void testACrashOfANewHistoryDoesNotTakeAFileOfTheOldHistoryForItsOwnUpload() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();
        final String oldLocation = TestTables.open(store, LOCATION, 0).uploadedFiles().get(0).location();
        final byte[] oldBytes = bytesOf(oldLocation);

        // the new history crashes at each step before its own table commit, with the same range as the old file
        final String step = randomFrom("merged", "pending", "uploaded");
        final OtherHistory other = new OtherHistory();
        TestLocalFiles.write(other.directory, 0, 99, false);
        expectThrows(IOException.class, () -> other.flusher(crashAt(step)).flush());
        other.flusher(ParquetFlusher.Hooks.NONE).flush();

        final List<ShardTable.UploadedFile> files = other.table().uploadedFiles();
        assertThat("after a crash at [" + step + "] the rows are listed once", files, hasSize(1));
        assertThat(files.get(0).rows(), equalTo(100L));
        assertThat(bytesOf(oldLocation), equalTo(oldBytes));
        final TestParquetBytes.Contents contents = TestParquetBytes.read(bytesOf(files.get(0).location()));
        assertThat(contents.rows().size(), equalTo(100));
        assertThat("the local files of the new history are gone", other.directory.listAll().length, equalTo(0));
        assertThat(ShardCommitData.uploadedRows(other.commits.userData()), equalTo(100L));
    }

    public void testARetryAfterAnUploadThatWasNotListedUsesANewKeyAndTheCleanupDeletesTheFirst() throws Exception {
        TestLocalFiles.write(directory, 0, 99, false);
        expectThrows(IOException.class, () -> newFlusher(crashAt("uploaded")).flush());
        final String marker = commits.userData().get(ShardCommitData.PENDING_UPLOAD);
        final String firstAttempt = marker.substring(marker.lastIndexOf('|') + 1);
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        final String firstKey = table.dataLocation(0, 99, firstAttempt);
        assertThat("the first try uploaded its blob", bytesOf(firstKey).length > 0, equalTo(true));
        assertThat("and the table does not list it", table.isListed(firstKey), equalTo(false));

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        final List<ShardTable.UploadedFile> files = table.uploadedFiles();
        assertThat(files, hasSize(1));
        assertThat("the retry wrote another key", files.get(0).location(), not(equalTo(firstKey)));
        assertFlushed(100);
        assertThat("the blob of the first try is an orphan, and only that", table.deleteOrphanFiles(), equalTo(1));
        assertThat(table.uploadedFiles(), hasSize(1));
        assertFlushed(100);
    }

    public void testTheCleanupOfOneHistoryNeverDeletesAnUnlistedFileOfAnother() throws Exception {
        // history A uploaded up to 999
        TestLocalFiles.write(directory, 0, 999, false);
        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        // history B has uploaded a file that it did not list yet, below the high-water mark of A
        final OtherHistory other = new OtherHistory();
        TestLocalFiles.write(other.directory, 0, 49, false);
        expectThrows(IOException.class, () -> other.flusher(crashAt("uploaded")).flush());
        final String pending = other.commits.userData().get(ShardCommitData.PENDING_UPLOAD);
        final String attempt = pending.substring(pending.lastIndexOf('|') + 1);
        final String unlisted = other.table().dataLocation(0, 49, attempt);

        // the compactor of A cleans up: B's blob is below the mark of A, and must stay. B's own mark is -1, so B would not delete it
        // either.
        assertThat(TestTables.open(store, LOCATION, 0).deleteOrphanFiles(), equalTo(0));
        assertThat(bytesOf(unlisted).length > 0, equalTo(true));
        assertThat(other.table().deleteOrphanFiles(), equalTo(0));

        other.flusher(ParquetFlusher.Hooks.NONE).flush();
        assertThat(other.table().uploadedFiles(), hasSize(1));
    }

    // ---- staged durability: the flush deletes the staged copies of the rows that it has put in the table ----

    private void stage(ShardTable table, LocalParquetFiles.LocalFile local) throws IOException {
        try (var in = directory.openInput(local.name(), org.apache.lucene.store.IOContext.DEFAULT)) {
            table.stageFile(
                local.fromSeqNo(),
                local.toSeqNo(),
                ShardTable.newAttempt(),
                new java.io.BufferedInputStream(new org.elasticsearch.common.lucene.store.InputStreamIndexInput(in, local.length())),
                local.length()
            );
        }
    }

    private void useStagedDurability() {
        settings = new ParquetFlusher.Settings(settings.maxFileBytes(), settings.rowGroupBytes(), settings.codec(), true);
    }

    public void testAFlushDeletesTheStagedCopiesOfTheRowsThatItUploaded() throws Exception {
        useStagedDurability();
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        stage(table, TestLocalFiles.write(directory, 0, 99, false));
        stage(table, TestLocalFiles.write(directory, 100, 249, false));
        assertThat(table.listStaged(), hasSize(2));

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertFlushed(250);
        assertThat(table.listStaged(), hasSize(0));
    }

    public void testAFlushLeavesTheStagedCopiesOfBulksThatCameAfterItsFiles() throws Exception {
        useStagedDurability();
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        stage(table, TestLocalFiles.write(directory, 0, 99, false));
        newFlusher(ParquetFlusher.Hooks.NONE).flush();
        // a bulk whose copy is in staging and whose local file the next flush has not seen
        stage(table, TestLocalFiles.write(directory, 100, 149, false));

        // a flush that has nothing new still looks only at the range below the high-water mark
        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(result.files(), equalTo(1));
        assertThat(table.listStaged(), hasSize(0));
    }

    public void testACrashBeforeTheStagedCopiesAreDeletedLeavesThemForTheNextFlush() throws Exception {
        useStagedDurability();
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        stage(table, TestLocalFiles.write(directory, 0, 99, false));
        expectThrows(IOException.class, () -> newFlusher(crashAt("cleaned")).flush());
        // the crash is after the cleanup of the local files: the staged copy of the rows that the table lists stays
        assertThat(table.listStaged(), hasSize(1));
        assertThat(table.uploadedFiles(), hasSize(1));

        // a flush with no local file at all still deletes it
        final ParquetFlusher.Result result = newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(result.files(), equalTo(0));
        assertThat(table.listStaged(), hasSize(0));
        assertFlushed(100);
    }

    public void testALeftoverStagedCopyBelowTheMarkIsDeletedButOneAboveItIsNot() throws Exception {
        useStagedDurability();
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        stage(table, TestLocalFiles.write(directory, 0, 99, false));
        newFlusher(ParquetFlusher.Hooks.NONE).flush();
        // a staging upload that finished after the flush: below the mark, redundant
        final byte[] bytes = new byte[] { 1 };
        table.stageFile(0, 99, "late", new java.io.ByteArrayInputStream(bytes), bytes.length);
        // the copy of a bulk above the mark whose local file is not in this directory: it must stay (it is adopted, or it is not ours)
        table.stageFile(100, 149, "above", new java.io.ByteArrayInputStream(bytes), bytes.length);

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        final List<ShardTable.StagedFile> left = table.listStaged();
        assertThat(left, hasSize(1));
        assertThat(left.get(0).fromSeqNo(), equalTo(100L));
    }

    public void testWithLocalDurabilityTheFlushDoesNotTouchStaging() throws Exception {
        final ShardTable table = TestTables.open(store, LOCATION, 0);
        final byte[] bytes = new byte[] { 1 };
        table.stageFile(0, 99, "a", new java.io.ByteArrayInputStream(bytes), bytes.length);
        TestLocalFiles.write(directory, 0, 99, false);

        newFlusher(ParquetFlusher.Hooks.NONE).flush();

        assertThat(table.listStaged(), hasSize(1));
    }
}
