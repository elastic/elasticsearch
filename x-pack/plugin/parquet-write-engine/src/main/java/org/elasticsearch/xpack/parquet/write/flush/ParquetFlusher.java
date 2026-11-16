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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles.LocalFile;
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 *   <li>delete the local files of the run and the merged file.</li>
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

    /** The settings that a flush reads when it starts, so that a dynamic update applies to the next flush. */
    public record Settings(long maxFileBytes, long rowGroupBytes, CompressionCodecName codec) {}

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
    private final Hooks hooks;

    public ParquetFlusher(Directory directory, ShardCommitData commits, ShardTable table, Supplier<Settings> settings, Hooks hooks) {
        this.directory = directory;
        this.commits = commits;
        this.table = table;
        this.settings = settings;
        this.hooks = hooks;
    }

    /** Uploads all the complete local files. A flush with no local file does nothing. */
    public Result flush() throws IOException {
        recoverPending();

        final long through = ShardCommitData.uploadedThroughSeqNo(commits.userData());
        final List<LocalFile> todo = new ArrayList<>();
        for (LocalFile file : LocalParquetFiles.list(directory)) {
            if (file.toSeqNo() <= through) {
                delete(file.name()); // a leftover of a crash after the table listed its rows
            } else {
                todo.add(file);
            }
        }
        int files = 0;
        long rows = 0;
        long bytes = 0;
        for (List<LocalFile> run : runs(todo, settings.get().maxFileBytes())) {
            final ParquetMerger.Merged merged = flushRun(run);
            rows += merged.rows();
            bytes += merged.length();
            files++;
        }
        return new Result(files, rows, bytes);
    }

    /**
     * Groups the files in the order of their sequence numbers into runs of at most {@code maxBytes} (a larger file is a run alone). The
     * runs have about the same size: the number of runs is the least that fits, and each run takes files until it reaches its share.
     * Filling a run to the maximum and putting the rest in the last run leaves a tiny file whenever the local data is just above the
     * maximum, which is the usual case when the flush threshold is close to the maximum file size (a measurement with a threshold of
     * 1 GB and a maximum of 1 GB uploaded a file of 5,000 rows after every large one).
     */
    static List<List<LocalFile>> runs(List<LocalFile> files, long maxBytes) {
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

    private ParquetMerger.Merged flushRun(List<LocalFile> run) throws IOException {
        final long from = run.get(0).fromSeqNo();
        final long to = run.get(run.size() - 1).toSeqNo();
        final Settings now = settings.get();

        // 1. merge, with the ids of the table (adding the columns that this run has and the table lacks)
        final MessageType union = ParquetMerger.unionSchema(directory, run);
        final MessageType schema = ShardTable.withFieldIds(union, table.ensureColumns(union));
        final String mergedName = LocalParquetFiles.mergedName(from, to);
        delete(mergedName); // the leftover of an earlier try of this range that did not get to the pending marker
        final ParquetMerger.Merged merged = ParquetMerger.merge(directory, run, schema, mergedName, now.codec(), now.rowGroupBytes());
        hooks.reached("merged");

        // 2. the pending marker
        final Pending pending = new Pending(mergedName, from, to, merged.rows(), ShardTable.newAttempt());
        final Map<String, String> userData = commits.userData();
        userData.put(ShardCommitData.PENDING_UPLOAD, pending.encode());
        commits.commit(userData);
        hooks.reached("pending");

        // 3 to 6
        uploadAndPublish(pending);
        return merged;
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
        table.append(file, pending.to());
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
