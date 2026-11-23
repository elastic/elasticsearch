/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.staging;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.TestLocalFiles;
import org.elasticsearch.xpack.parquet.write.file.TestParquetBytes;
import org.elasticsearch.xpack.parquet.write.flush.ParquetFlusher;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.elasticsearch.xpack.parquet.write.table.TestTables;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * The staged files of a lost store are the only copy of the rows that no flush had put in the table. A shard that comes back with an
 * empty store (a new history) puts them in the table, under the old history, exactly once, whatever the crashes in between.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        StagedFileAdopterTests.IcebergWorkerPoolFilter.class }
)
public class StagedFileAdopterTests extends ESTestCase {

    /** Iceberg keeps a static pool of worker threads for the whole JVM, which is not a leak of a test. */
    public static final class IcebergWorkerPoolFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().startsWith("iceberg-worker-pool");
        }
    }

    private static final TableLocation LOCATION = new TableLocation("file:///warehouse/logs", BlobPath.EMPTY.add("logs"));
    private static final String OLD = "history-old";
    private static final String CURRENT = "history-new";

    private FsBlobStore store;
    /** The directory of the current history, where the merged file of a run is written. */
    private Directory directory;
    /** A scratch directory, where the bytes of a staged file are written before they are staged. */
    private Directory scratch;
    private ParquetFlusher.Settings settings;

    @Before
    public void createFixtures() throws Exception {
        store = new FsBlobStore(4096, createTempDir(), false);
        directory = new ByteBuffersDirectory();
        scratch = new ByteBuffersDirectory();
        settings = new ParquetFlusher.Settings(1L << 30, 1 << 16, CompressionCodecName.SNAPPY, true);
    }

    @After
    public void closeFixtures() throws Exception {
        store.close();
        directory.close();
        scratch.close();
    }

    private ShardTable table(String history) {
        return TestTables.open(store, LOCATION, 0, history);
    }

    private StagedFileAdopter adopter(StagedFileAdopter.Hooks hooks) {
        return new StagedFileAdopter(directory, table(CURRENT), () -> settings, () -> false, hooks);
    }

    private static StagedFileAdopter.Hooks crashAt(String crashStep) {
        return step -> {
            if (step.equals(crashStep)) {
                throw new IOException("crash after [" + step + "]");
            }
        };
    }

    /** Stages the rows from to to, as the engine does: the bytes of the local file, below the staging directory of the history. */
    private void stage(ShardTable history, long from, long to) throws IOException {
        stageRowsAs(history, from, to, from, to);
    }

    /** Stages the rows of one range under the name of another, to make a file whose name does not tell its rows. */
    private void stageRowsAs(ShardTable history, long rowsFrom, long rowsTo, long nameFrom, long nameTo) throws IOException {
        final LocalParquetFiles.LocalFile local = TestLocalFiles.write(scratch, rowsFrom, rowsTo, false);
        try (IndexInput in = scratch.openInput(local.name(), IOContext.DEFAULT)) {
            history.stageFile(
                nameFrom,
                nameTo,
                ShardTable.newAttempt(),
                new BufferedInputStream(new InputStreamIndexInput(in, local.length())),
                local.length()
            );
        }
        scratch.deleteFile(local.name());
    }

    /** The values of {@code count} in the files that the table lists for the history, each once. */
    private TreeSet<Long> listedRows(String history) throws IOException {
        final TreeSet<Long> seen = new TreeSet<>();
        for (ShardTable.UploadedFile file : table(history).uploadedFiles()) {
            try (var in = TestTables.read(store, LOCATION, file.location())) {
                for (var row : TestParquetBytes.read(in.readAllBytes()).rows()) {
                    assertTrue("row " + row.getLong("count", 0) + " appears once", seen.add(row.getLong("count", 0)));
                }
            }
        }
        return seen;
    }

    private void assertRows(String history, long from, long to) throws IOException {
        final TreeSet<Long> expected = new TreeSet<>();
        for (long row = from; row <= to; row++) {
            expected.add(row);
        }
        assertThat(listedRows(history), equalTo(expected));
    }

    public void testTheStagedFilesOfAnOldHistoryAreAdoptedIntoItsOwnDirectory() throws Exception {
        final ShardTable old = table(OLD);
        stage(old, 0, 99);
        stage(old, 100, 249);

        final StagedFileAdopter.Result result = adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(result.histories(), equalTo(1));
        assertThat(result.files(), equalTo(2));
        assertThat(result.rows(), equalTo(250L));
        final List<ShardTable.UploadedFile> files = table(OLD).uploadedFiles();
        assertThat("both staged files are one run", files, hasSize(1));
        assertThat(files.get(0).location(), containsString("/data/shard-0/" + OLD + "/"));
        assertThat(files.get(0).rows(), equalTo(250L));
        assertRows(OLD, 0, 249);
        assertThat(old.listStaged(), hasSize(0));
        assertThat("the new history has no file", table(CURRENT).uploadedFiles(), hasSize(0));
        assertThat("no local file stays", directory.listAll().length, equalTo(0));
    }

    public void testACrashAtAnyStepLeavesTheRowsInTheTableExactlyOnceAfterARerun() throws Exception {
        final String step = randomFrom("merged", "uploaded", "appended", "cleaned");
        final ShardTable old = table(OLD);
        stage(old, 0, 99);
        stage(old, 100, 249);

        expectThrows(IOException.class, () -> adopter(crashAt(step)).adopt());
        adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertRows(OLD, 0, 249);
        assertThat("after a crash at [" + step + "]", old.listStaged(), hasSize(0));
        assertThat(table(OLD).uploadedFiles(), hasSize(1));
        assertThat(directory.listAll().length, equalTo(0));
    }

    public void testAStagedFileAtOrBelowTheMarkIsDeletedAndNotAdoptedAgain() throws Exception {
        final ShardTable old = table(OLD);
        stage(old, 0, 99);
        adopter(StagedFileAdopter.Hooks.NONE).adopt();
        assertRows(OLD, 0, 99);
        // a staging upload that finished after the adoption, and the copy of a later bulk
        stage(old, 0, 99);
        stage(old, 100, 149);

        final StagedFileAdopter.Result result = adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(result.deleted(), equalTo(1));
        assertThat(result.rows(), equalTo(50L));
        assertRows(OLD, 0, 149);
        assertThat(old.listStaged(), hasSize(0));
    }

    public void testTheStagedFilesOfTheCurrentHistoryAreNotTouched() throws Exception {
        final ShardTable current = table(CURRENT);
        stage(current, 0, 99);

        final StagedFileAdopter.Result result = adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(result.histories(), equalTo(0));
        assertThat(current.listStaged(), hasSize(1));
        assertThat(current.uploadedFiles(), hasSize(0));
    }

    /**
     * A file system keeps the empty directory of a history whose staged files were deleted, and the test file system adds directories of
     * its own (a run with one seed found such a directory and counted it as a history). Neither is a history.
     */
    public void testADirectoryWithoutAStagedFileIsNotAHistory() throws Exception {
        stage(table(OLD), 0, 49);
        // the directory of a history that has no staged file: listing the staged files of a table makes its directory
        assertThat(table("history-with-no-file").listStaged(), hasSize(0));
        assertThat(table(CURRENT).listStagedHistories(), contains(OLD));

        final StagedFileAdopter.Result result = adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(result.histories(), equalTo(1));
        assertThat(result.rows(), equalTo(50L));
    }

    public void testSeveralOldHistoriesAreAdoptedEachIntoItsOwnDirectory() throws Exception {
        stage(table("history-a"), 0, 49);
        stage(table("history-b"), 0, 79);
        stage(table(CURRENT), 0, 9);

        final StagedFileAdopter.Result result = adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(result.histories(), equalTo(2));
        assertThat(result.rows(), equalTo(130L));
        assertRows("history-a", 0, 49);
        assertRows("history-b", 0, 79);
        assertThat(table(CURRENT).listStaged(), hasSize(1));
    }

    public void testAGapInTheRangesFromABulkThatWasNotStagedIsAdopted() throws Exception {
        final ShardTable old = table(OLD);
        stage(old, 0, 49);
        stage(old, 60, 99); // the bulk of 50 to 59 failed to stage and was discarded

        adopter(StagedFileAdopter.Hooks.NONE).adopt();

        final TreeSet<Long> rows = listedRows(OLD);
        assertThat(rows.size(), equalTo(90));
        assertThat(rows.contains(55L), equalTo(false));
        assertThat(table(OLD).uploadedThroughSeqNo(), equalTo(99L));
    }

    public void testASmallMaximumFileSizeSplitsTheAdoptionIntoSeveralFiles() throws Exception {
        final ShardTable old = table(OLD);
        stage(old, 0, 499);
        stage(old, 500, 999);
        stage(old, 1000, 1499);
        // room for one staged file in a run
        settings = new ParquetFlusher.Settings(1, 1 << 16, CompressionCodecName.SNAPPY, true);

        adopter(StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(table(OLD).uploadedFiles(), hasSize(3));
        assertRows(OLD, 0, 1499);
    }

    public void testARunWhoseRowsDoNotMatchTheNamesOfItsFilesPublishesNothing() throws Exception {
        final ShardTable old = table(OLD);
        stageRowsAs(old, 0, 99, 0, 49); // the name says 50 rows, the file has 100

        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> adopter(StagedFileAdopter.Hooks.NONE).adopt());

        assertThat(e.getMessage(), containsString("wrote [100] rows"));
        assertThat(table(OLD).uploadedFiles(), hasSize(0));
        assertThat("the staged file stays", old.listStaged(), hasSize(1));
        assertThat("no local file stays", directory.listAll().length, equalTo(0));
    }

    public void testAnAdoptionThatIsToldToStopAdoptsNothing() throws Exception {
        stage(table(OLD), 0, 99);

        new StagedFileAdopter(directory, table(CURRENT), () -> settings, () -> true, StagedFileAdopter.Hooks.NONE).adopt();

        assertThat(table(OLD).uploadedFiles(), hasSize(0));
        assertThat(table(OLD).listStaged(), hasSize(1));
        assertThat(table(OLD).listStaged().get(0).name(), not(equalTo("")));
    }
}
