/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.flush;

import org.apache.iceberg.DataFile;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles.LocalFile;
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * A flush: the local Parquet files that bulks wrote become large files in the repository, and the table lists them.
 *
 * <p>The goal is that a crash at any point loses no acknowledged row, never lists a row twice, and never puts different bytes under one
 * key. A flush of a run of local files goes through these steps, and a restart finishes whichever step was cut off:
 *
 * <ol>
 *   <li>merge the files of the run into one local file with the ids and the codec of the table;</li>
 *   <li>write {@code pending_upload} in the commit data of the shard (the name of the merged file, its range and the upload attempt);</li>
 *   <li>upload the merged file under its final key, below which no reader looks yet. The key has the history of the shard and the
 *       attempt, so that it is new: it never replaces a file of an earlier history or of an earlier try;</li>
 *   <li>append it to the table, which is the moment its rows become visible and count as uploaded;</li>
 *   <li>write the high-water mark and the row count in the commit data and remove {@code pending_upload};</li>
 *   <li>delete the local files of the run and the merged file, and (with staged durability) the staged copies of the run.</li>
 * </ol>
 *
 * <p>The table, not the commit data, is the source of truth for what is uploaded, so recovery asks the table whether the pending file
 * is listed, by its exact key. A pending file that is not listed is uploaded again under a new attempt. The local input files are
 * deleted only in the last step, so a pending file that is lost before its upload costs only a merge again.
 *
 * <p>A flush must not run concurrently with another flush of the same shard; the engine's flush lock takes care of that.
 */
public final class ParquetFlusher {

    private static final Logger logger = LogManager.getLogger(ParquetFlusher.class);

    /** Called after each step with its name, so that a test can stop the flush there like a crash. */
    public interface Hooks {
        Hooks NONE = step -> {};

        void reached(String step) throws IOException;
    }

    /**
     * Told after each run of a flush is committed: its rows are in the table, the high-water mark and the row count of the commit data
     * have moved, and its local files are deleted. The engine counts the local rows and bytes for its backpressure and its statistics, and
     * it must count again at this moment and not only when the whole flush is over, because a flush of several runs takes minutes and
     * would otherwise go on holding the space of the runs that are already uploaded.
     */
    @FunctionalInterface
    public interface RunListener {
        RunListener NONE = () -> {};

        void runCommitted() throws IOException;
    }

    /** The settings that a flush reads when it starts, so that a dynamic update applies to the next flush. */
    public record Settings(long maxFileBytes, long rowGroupBytes, CompressionCodecName codec, boolean staged, int mergeParallelism) {
        /** Settings for a merge on the thread of the flush. */
        public Settings(long maxFileBytes, long rowGroupBytes, CompressionCodecName codec, boolean staged) {
            this(maxFileBytes, rowGroupBytes, codec, staged, 1);
        }
    }

    /** What a flush did. */
    public record Result(int files, long rows, long bytes) {}

    private record Pending(String name, long from, long to, long rows, String attempt) {
        String encode() {
            return name + "|" + from + "|" + to + "|" + rows + "|" + attempt;
        }

        static Pending decode(String value) {
            final String[] parts = value.split("\\|");
            if (parts.length != 5) {
                throw new IllegalStateException("cannot read [" + ShardCommitData.PENDING_UPLOAD + "] [" + value + "]");
            }
            return new Pending(parts[0], Long.parseLong(parts[1]), Long.parseLong(parts[2]), Long.parseLong(parts[3]), parts[4]);
        }

        Pending withAttempt(String newAttempt) {
            return new Pending(name, from, to, rows, newAttempt);
        }
    }

    private final Directory directory;
    private final ShardCommitData commits;
    private final ShardTable table;
    private final Supplier<Settings> settings;
    private final LongSupplier completeThroughSeqNo;
    private final Hooks hooks;
    private final RunListener runListener;
    /** Runs the tasks of the groups of a merge, see {@link ParquetMerger}; null merges on the thread of the flush. */
    private final Executor mergeExecutor;
    /** Runs the upload of a run while the next run is merged; null uploads on the thread of the flush, after each merge. */
    private final Executor uploadExecutor;
    /** Held from the start of an upload until its end, so that the engine does not close under it. */
    private final Supplier<Releasable> uploadGuard;
    /** Serializes the changes of the schema of the table (by a merge) and the append of a file (by an upload), which run on two threads. */
    private final Object tableLock = new Object();
    /** The upload that runs in the background, and that a later flush (or {@link #flush(boolean)} with true) waits for. */
    private volatile InFlight inFlight;

    /**
     * An upload of a run that goes on after the flush that started it has returned. The marker of the run is in the commit data until the
     * upload has finished, so a crash or a failure is recovered like any other: the next flush finishes the run.
     */
    private record InFlight(Pending pending, long localBytes, FutureTask<Void> task) {}

    /**
     * @param completeThroughSeqNo the highest sequence number {@code n} such that every bulk with a range up to {@code n} has finished
     *                             writing its local file (or will never write one). Bulks of a shard write in parallel, so a file of a
     *                             higher range can exist while a lower one is still being written. A flush records the highest range it
     *                             uploaded and deletes every local file at or below it as a leftover, so it must not take a file above
     *                             this bound: the lower file would then be deleted when it appears.
     */
    public ParquetFlusher(
        Directory directory,
        ShardCommitData commits,
        ShardTable table,
        Supplier<Settings> settings,
        LongSupplier completeThroughSeqNo,
        Hooks hooks
    ) {
        this(directory, commits, table, settings, completeThroughSeqNo, hooks, RunListener.NONE);
    }

    /** @param runListener told after each committed run, see {@link RunListener} */
    public ParquetFlusher(
        Directory directory,
        ShardCommitData commits,
        ShardTable table,
        Supplier<Settings> settings,
        LongSupplier completeThroughSeqNo,
        Hooks hooks,
        RunListener runListener
    ) {
        this(directory, commits, table, settings, completeThroughSeqNo, hooks, runListener, null);
    }

    /**
     * @param mergeExecutor runs the tasks of the groups of a merge when {@link Settings#mergeParallelism} is more than 1. It must not be
     *                      the executor that the flush runs on, because the flush waits for the tasks.
     */
    public ParquetFlusher(
        Directory directory,
        ShardCommitData commits,
        ShardTable table,
        Supplier<Settings> settings,
        LongSupplier completeThroughSeqNo,
        Hooks hooks,
        RunListener runListener,
        Executor mergeExecutor
    ) {
        this(directory, commits, table, settings, completeThroughSeqNo, hooks, runListener, mergeExecutor, null, () -> () -> {});
    }

    /**
     * @param uploadExecutor runs the upload of a run so that the next run is merged meanwhile, and the last upload of a flush that does not
     *                       wait is still running when the flush returns. It must not be an executor that the flush or the merge runs on.
     * @param uploadGuard    called when an upload starts; the result is closed when it ends
     */
    public ParquetFlusher(
        Directory directory,
        ShardCommitData commits,
        ShardTable table,
        Supplier<Settings> settings,
        LongSupplier completeThroughSeqNo,
        Hooks hooks,
        RunListener runListener,
        Executor mergeExecutor,
        Executor uploadExecutor,
        Supplier<Releasable> uploadGuard
    ) {
        this.uploadExecutor = uploadExecutor;
        this.uploadGuard = uploadGuard;
        this.mergeExecutor = mergeExecutor;
        this.directory = directory;
        this.commits = commits;
        this.table = table;
        this.settings = settings;
        this.completeThroughSeqNo = completeThroughSeqNo;
        this.hooks = hooks;
        this.runListener = runListener;
    }

    /** {@link #flush(boolean)} that returns when everything that it took is in the table. */
    public Result flush() throws IOException {
        return flush(true);
    }

    /**
     * Uploads the local files up to the complete bound. A flush with no such file does nothing, and a file above the bound stays for the
     * next flush.
     *
     * <p>With an upload executor the upload of a run goes on while the next run is merged, and with {@code waitForUploads} false the
     * upload of the last run goes on after this method has returned: its rows are in the table later, and the next flush merges its own
     * files first and then waits for that upload. Until then the files of that run stay in the directory and
     * {@link #uploadingLocalBytes()} counts them. A failure of that upload is logged, and the next flush finishes the run from its marker.
     *
     * @param waitForUploads whether the rows of every run, also of an earlier flush, are in the table when the method returns
     */
    public Result flush(boolean waitForUploads) throws IOException {
        InFlight running = inFlight;
        if (running != null && running.task().isDone()) {
            // An upload of an earlier flush has ended. If it failed it logged why, and its marker is still in the commit data, so the
            // recovery below finishes the run.
            inFlight = null;
            running = null;
        }
        if (running == null) {
            recoverPending();
        }
        // The files of a run that is being uploaded are neither leftovers nor to do. This is read before the commit data, so that a run
        // that ends in between has its files skipped here or its commit seen below.
        final long uploadingThrough = running == null ? -1 : running.pending().to();

        // The bound is read before the directory is listed: a file at or below it is complete when it is listed.
        final long complete = completeThroughSeqNo.getAsLong();
        final long through = ShardCommitData.uploadedThroughSeqNo(commits.userData());
        // the staged copies of rows that the table lists are redundant: a leftover of a crash, or of a staging upload that finished
        // after the flush that had picked its local file up
        deleteStagedUpTo(through);
        final List<LocalFile> todo = new ArrayList<>();
        boolean deletedLeftovers = false;
        for (LocalFile file : LocalParquetFiles.list(directory)) {
            if (file.toSeqNo() <= through) {
                delete(file.name()); // a leftover of a crash after the table listed its rows
                deletedLeftovers = true;
            } else if (file.toSeqNo() <= uploadingThrough) {
                continue; // being uploaded by an earlier flush
            } else if (file.toSeqNo() <= complete) {
                todo.add(file);
            }
            // else: a bulk with a lower range may still be writing, so the next flush takes this file
        }
        if (deletedLeftovers) {
            runCommitted(); // the engine counted these files when they were written, and they are gone now
        }
        int files = 0;
        long rows = 0;
        long bytes = 0;
        try {
            for (List<LocalFile> run : runs(todo, settings.get().maxFileBytes())) {
                final Merged merged = mergeRun(run);
                // one marker at a time: the run before this one must be in the table before this one is announced
                awaitUpload();
                final Pending pending = writeMarker(merged);
                startUpload(pending, merged.localBytes());
                rows += merged.merged().rows();
                bytes += merged.merged().length();
                files++;
            }
            if (waitForUploads) {
                awaitUpload();
            }
        } catch (IOException | RuntimeException | Error e) {
            // a flush that fails does not leave an upload behind that a later flush, or a recovery after a restart, could meet half way
            try {
                awaitUpload();
            } catch (IOException | RuntimeException | Error suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        return new Result(files, rows, bytes);
    }

    /**
     * The local bytes of the run that is being uploaded in the background, which are still in the directory but must not count as data
     * that waits for a flush.
     */
    public long uploadingLocalBytes() {
        final InFlight running = inFlight;
        return running == null || running.task().isDone() ? 0 : running.localBytes();
    }

    /**
     * Groups the files in the order of their sequence numbers into runs of at most {@code maxBytes} (a larger file is a run alone). The
     * runs have about the same size: the number of runs is the least that fits, and each run takes files until it reaches its share.
     * Filling a run to the maximum and putting the rest in the last run leaves a tiny file whenever the local data is just above the
     * maximum, which is the usual case when the flush threshold is close to the maximum file size (a measurement with a threshold of
     * 1 GB and a maximum of 1 GB uploaded a file of 5,000 rows after every large one).
     */
    public static List<List<LocalFile>> runs(List<LocalFile> files, long maxBytes) {
        long total = 0;
        for (LocalFile file : files) {
            total += file.length();
        }
        final long count = Math.max(1, (total + maxBytes - 1) / maxBytes);
        final long share = (total + count - 1) / count;
        final List<List<LocalFile>> runs = new ArrayList<>();
        List<LocalFile> run = new ArrayList<>();
        long bytes = 0;
        for (LocalFile file : files) {
            if (run.isEmpty() == false && bytes + file.length() > maxBytes) {
                runs.add(run);
                run = new ArrayList<>();
                bytes = 0;
            }
            run.add(file);
            bytes += file.length();
            if (bytes >= share && runs.size() < count - 1) {
                runs.add(run);
                run = new ArrayList<>();
                bytes = 0;
            }
        }
        if (run.isEmpty() == false) {
            runs.add(run);
        }
        return runs;
    }

    /** A run that is merged: its file, and the local bytes that it was made from. */
    private record Merged(ParquetMerger.Merged merged, long from, long to, long localBytes) {}

    /** Step 1: merge, with the ids of the table (adding the columns that this run has and the table lacks). */
    private Merged mergeRun(List<LocalFile> run) throws IOException {
        final long from = run.get(0).fromSeqNo();
        final long to = run.get(run.size() - 1).toSeqNo();
        final Settings now = settings.get();

        final MessageType union = ParquetMerger.unionSchema(directory, run, mergeExecutor, now.mergeParallelism());
        final Map<String, Integer> ids;
        synchronized (tableLock) {
            ids = table.ensureColumns(union);
        }
        final MessageType schema = ShardTable.withFieldIds(union, ids);
        final String mergedName = LocalParquetFiles.mergedName(from, to);
        delete(mergedName); // the leftover of an earlier try of this range that did not get to the pending marker
        final long mergeStart = System.nanoTime();
        final ParquetMerger.Merged merged = ParquetMerger.merge(
            directory,
            run,
            schema,
            mergedName,
            now.codec(),
            now.rowGroupBytes(),
            mergeExecutor,
            now.mergeParallelism()
        );
        logger.debug(
            "merged [{}] local files with [{}] rows into [{}] bytes in [{}] ms with up to [{}] threads",
            run.size(),
            merged.rows(),
            merged.length(),
            (System.nanoTime() - mergeStart) / 1_000_000,
            now.mergeParallelism()
        );
        hooks.reached("merged");
        long localBytes = 0;
        for (LocalFile file : run) {
            localBytes += file.length();
        }
        return new Merged(merged, from, to, localBytes);
    }

    /** Step 2: the pending marker. */
    private Pending writeMarker(Merged run) throws IOException {
        final Pending pending = new Pending(run.merged().name(), run.from(), run.to(), run.merged().rows(), ShardTable.newAttempt());
        final Map<String, String> userData = commits.userData();
        userData.put(ShardCommitData.PENDING_UPLOAD, pending.encode());
        commits.commit(userData);
        hooks.reached("pending");
        return pending;
    }

    /** Steps 3 to 6, on the upload executor if there is one, and then {@link #awaitUpload} is what waits for them. */
    private void startUpload(Pending pending, long localBytes) throws IOException {
        if (uploadExecutor == null) {
            uploadAndPublish(pending);
            return;
        }
        final Releasable guard = uploadGuard.get();
        final FutureTask<Void> task = new FutureTask<>(() -> {
            try {
                uploadAndPublish(pending);
                return null;
            } catch (IOException | RuntimeException e) {
                logger.warn("could not upload the run [{}]; the next flush finishes it", pending.name(), e);
                throw e;
            } finally {
                guard.close();
            }
        });
        try {
            uploadExecutor.execute(task);
        } catch (RuntimeException e) {
            guard.close();
            uploadAndPublish(pending); // the executor does not take it (it is shut down): upload on this thread
            return;
        }
        inFlight = new InFlight(pending, localBytes, task);
    }

    /** Waits for the upload that runs in the background, uninterruptibly, and throws what made it fail. */
    private void awaitUpload() throws IOException {
        final InFlight running = inFlight;
        if (running == null) {
            return;
        }
        boolean interrupted = false;
        Throwable failure = null;
        while (true) {
            try {
                running.task().get();
                break;
            } catch (ExecutionException e) {
                failure = e.getCause();
                break;
            } catch (InterruptedException e) {
                interrupted = true; // the upload goes on, and the rows are not safe to forget until it ends
            }
        }
        inFlight = null;
        if (interrupted) {
            Thread.currentThread().interrupt();
            final InterruptedIOException e = new InterruptedIOException("interrupted while a run was uploaded");
            if (failure != null) {
                e.addSuppressed(failure);
            }
            throw e;
        }
        if (failure != null) {
            switch (failure) {
                case IOException e -> throw e;
                case RuntimeException e -> throw e;
                case Error e -> throw e;
                default -> throw new IOException(failure);
            }
        }
    }

    /** Steps 3 to 6 for a merged file that exists in the directory. */
    private void uploadAndPublish(Pending pending) throws IOException {
        final long length = directory.fileLength(pending.name());
        try (IndexInput in = directory.openInput(pending.name(), IOContext.DEFAULT)) {
            // S3 needs a stream that supports mark
            table.upload(
                pending.from(),
                pending.to(),
                pending.attempt(),
                new BufferedInputStream(new InputStreamIndexInput(in, length)),
                length
            );
        }
        hooks.reached("uploaded");

        final DataFile file = table.dataFile(
            pending.from(),
            pending.to(),
            pending.attempt(),
            length,
            new IndexInputFile(directory, pending.name())
        );
        synchronized (tableLock) {
            table.append(file, pending.to());
        }
        hooks.reached("appended");

        finish(pending);
    }

    /** Steps 5 and 6, which do not depend on how far the earlier steps got. */
    private void finish(Pending pending) throws IOException {
        final Map<String, String> userData = commits.userData();
        final long through = ShardCommitData.uploadedThroughSeqNo(userData);
        userData.remove(ShardCommitData.PENDING_UPLOAD);
        if (pending.to() > through) {
            // a recovery that finishes a flush twice must not count its rows twice
            userData.put(ShardCommitData.UPLOADED_THROUGH_SEQ_NO, Long.toString(pending.to()));
            userData.put(ShardCommitData.UPLOADED_ROWS, Long.toString(ShardCommitData.uploadedRows(userData) + pending.rows()));
        }
        commits.commit(userData);
        hooks.reached("committed");

        for (LocalFile file : LocalParquetFiles.list(directory)) {
            if (file.toSeqNo() <= pending.to()) {
                delete(file.name());
            }
        }
        delete(pending.name());
        hooks.reached("cleaned");
        runCommitted();
        deleteStagedUpTo(pending.to());
        hooks.reached("unstaged");
    }

    /** Tells the listener. A failure is only logged: the run is committed, and the engine counts again when the flush is over. */
    private void runCommitted() {
        try {
            runListener.runCommitted();
        } catch (IOException | RuntimeException e) {
            logger.warn("could not update the counters of the local files after a run of a flush was committed", e);
        }
    }

    /**
     * Deletes the staged copies of the rows up to {@code through}, which the table lists. A failure is only logged: a staged blob that
     * stays is deleted by the next flush, and it is never read as long as its rows are in the table.
     */
    private void deleteStagedUpTo(long through) {
        if (settings.get().staged() == false || through < 0) {
            return;
        }
        try {
            final List<String> redundant = new ArrayList<>();
            for (ShardTable.StagedFile file : table.listStaged()) {
                if (file.toSeqNo() <= through) {
                    redundant.add(file.name());
                }
            }
            table.deleteStaged(redundant);
        } catch (IOException | RuntimeException e) {
            logger.warn("could not delete the staged files up to sequence number [{}]; the next flush tries again", through, e);
        }
    }

    /** Finishes the flush that a crash cut off, if there is one. */
    private void recoverPending() throws IOException {
        final Map<String, String> userData = commits.userData();
        final String value = userData.get(ShardCommitData.PENDING_UPLOAD);
        if (value == null) {
            // merged files that never got a marker are leftovers of a crash during the merge or right after it
            for (LocalFile file : LocalParquetFiles.listMerged(directory)) {
                delete(file.name());
            }
            return;
        }
        final Pending pending = Pending.decode(value);
        logger.info("finishing the flush of [{}] that did not complete", pending.name());
        // A flush that failed or crashed while it merged the next run (the upload of this one ran meanwhile) left a merged file that has no
        // marker. It is made again from the local files, which are all still here.
        for (LocalFile file : LocalParquetFiles.listMerged(directory)) {
            if (file.name().equals(pending.name()) == false) {
                delete(file.name());
            }
        }
        // The exact key: a file of another history or of another attempt with the same range is not ours.
        if (table.isListed(table.dataLocation(pending.from(), pending.to(), pending.attempt()))) {
            finish(pending);
        } else if (mergedFileExists(pending.name())) {
            // The earlier try may have left a blob under its key (possibly a partial one). A key is written once, so this try gets a new
            // one, which the marker records before the upload; the blob of the earlier try is an orphan that the compactor deletes.
            final Pending retry = pending.withAttempt(ShardTable.newAttempt());
            final Map<String, String> retried = new HashMap<>(userData);
            retried.put(ShardCommitData.PENDING_UPLOAD, retry.encode());
            commits.commit(retried);
            uploadAndPublish(retry);
        } else {
            // The merged file is gone and the table does not list it. The local files that it was made from are deleted only after the
            // table lists the rows, so they are still here: forget the marker, and the next flush merges them again.
            final Map<String, String> cleared = new HashMap<>(userData);
            cleared.remove(ShardCommitData.PENDING_UPLOAD);
            commits.commit(cleared);
        }
    }

    private boolean mergedFileExists(String name) throws IOException {
        for (String existing : directory.listAll()) {
            if (existing.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private void delete(String name) throws IOException {
        try {
            directory.deleteFile(name);
        } catch (NoSuchFileException e) {
            // already gone
        }
    }
}
