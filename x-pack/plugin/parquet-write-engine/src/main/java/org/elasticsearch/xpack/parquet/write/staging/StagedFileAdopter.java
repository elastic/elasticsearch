/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.staging;

import org.apache.iceberg.DataFile;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.schema.MessageType;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.parquet.write.file.IndexInputFile;
import org.elasticsearch.xpack.parquet.write.file.LocalParquetFiles;
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.flush.ParquetFlusher;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Puts the rows of the staged files of lost stores in the table.
 *
 * <p>With staged durability a bulk is acknowledged after a copy of its file is in the repository, below
 * {@code staging/shard-<n>/<history>/}. A flush deletes the copies of the rows that it has put in the table. When a node and its disk
 * are lost, the shard comes back with an empty store, which is a new history, and the staged files of the old history are the only
 * copy of the rows that no flush had uploaded. This class finds them and does the part of the flush that was missing.
 *
 * <p>For every history of the shard except the current one it does this:
 * <ol>
 *   <li>look up the high-water mark of that history in the table, and delete the staged files at or below it: their rows are listed;</li>
 *   <li>group the other staged files in the order of their sequence numbers into runs, like a flush;</li>
 *   <li>merge a run into one local file, reading the staged blobs with ranged reads, like the compactor reads data files;</li>
 *   <li>upload it below {@code data/shard-<n>/<old history>/}, with a new attempt, and append it to the table;</li>
 *   <li>delete the staged files of the run and the local file.</li>
 * </ol>
 *
 * <p>The rows keep their history and their sequence numbers: nothing is renumbered. The table is the record of what was adopted, so the
 * steps are idempotent. A crash after the append leaves staged files at or below the new mark, which the next run deletes; a crash before
 * the append leaves a blob that no snapshot lists (an orphan, which this class never deletes: the writer of that history may still be
 * alive) and the next run merges the run again.
 *
 * <p>This does not fence a writer that is still alive. If the old node is only cut off from the cluster and keeps flushing the same
 * history while a new primary adopts it, the table can list overlapping files. See the README.
 */
public final class StagedFileAdopter {

    private static final Logger logger = LogManager.getLogger(StagedFileAdopter.class);

    /** Called after each step of a run with its name, so that a test can stop the adoption there like a crash. */
    public interface Hooks {
        Hooks NONE = step -> {};

        void reached(String step) throws IOException;
    }

    /** What an adoption did. */
    public record Result(int histories, int files, long rows, int deleted) {}

    private final Directory directory;
    private final ShardTable table;
    private final Supplier<ParquetFlusher.Settings> settings;
    private final BooleanSupplier stop;
    private final Hooks hooks;

    /**
     * @param directory the local directory for the merged file of a run
     * @param table     the table of the current history; the other histories are looked at through {@link ShardTable#forHistory}
     * @param settings  the maximum size of a run, the row groups and the codec, as for a flush
     * @param stop      true when the adoption must end (the shard closes); checked between runs
     */
    public StagedFileAdopter(
        Directory directory,
        ShardTable table,
        Supplier<ParquetFlusher.Settings> settings,
        BooleanSupplier stop,
        Hooks hooks
    ) {
        this.directory = directory;
        this.table = table;
        this.settings = settings;
        this.stop = stop;
        this.hooks = hooks;
    }

    /** Adopts the staged files of every other history of the shard. */
    public Result adopt() throws IOException {
        int histories = 0;
        int files = 0;
        long rows = 0;
        int deleted = 0;
        for (String history : table.listStagedHistories()) {
            if (history.equals(table.historyUuid())) {
                continue;
            }
            final Result result = adoptHistory(table.forHistory(history), history);
            histories++;
            files += result.files();
            rows += result.rows();
            deleted += result.deleted();
        }
        return new Result(histories, files, rows, deleted);
    }

    private Result adoptHistory(ShardTable view, String history) throws IOException {
        // the table is the record of what is uploaded
        final long mark = view.uploadedThroughSeqNo();
        final List<ShardTable.StagedFile> todo = new ArrayList<>();
        final List<String> redundant = new ArrayList<>();
        for (ShardTable.StagedFile file : view.listStaged()) {
            if (file.toSeqNo() <= mark) {
                redundant.add(file.name());
            } else {
                todo.add(file);
            }
        }
        view.deleteStaged(redundant);

        final List<LocalParquetFiles.LocalFile> asLocal = new ArrayList<>(todo.size());
        for (ShardTable.StagedFile file : todo) {
            asLocal.add(new LocalParquetFiles.LocalFile(file.name(), file.fromSeqNo(), file.toSeqNo(), file.bytes()));
        }
        int files = 0;
        long rows = 0;
        for (List<LocalParquetFiles.LocalFile> run : ParquetFlusher.runs(asLocal, settings.get().maxFileBytes())) {
            if (stop.getAsBoolean()) {
                break;
            }
            final List<ShardTable.StagedFile> staged = new ArrayList<>(run.size());
            for (LocalParquetFiles.LocalFile local : run) {
                for (ShardTable.StagedFile candidate : todo) {
                    if (candidate.name().equals(local.name())) {
                        staged.add(candidate);
                    }
                }
            }
            final long runRows = adoptRun(view, staged);
            rows += runRows;
            files += staged.size();
            logger.info("adopted [{}] staged files with [{}] rows of the history [{}]", staged.size(), runRows, history);
        }
        return new Result(1, files, rows, redundant.size());
    }

    /** @return the rows that it put in the table */
    private long adoptRun(ShardTable view, List<ShardTable.StagedFile> run) throws IOException {
        final String name = LocalParquetFiles.adoptedName(run.get(0).fromSeqNo(), run.get(run.size() - 1).toSeqNo());
        try {
            return mergeUploadAndAppend(view, run, name);
        } finally {
            // a failure in this process must not leave a merged file of gigabytes on the disk for as long as the shard runs; after a
            // crash of the process, the next start deletes it
            delete(name);
        }
    }

    private long mergeUploadAndAppend(ShardTable view, List<ShardTable.StagedFile> run, String name) throws IOException {
        final long from = run.get(0).fromSeqNo();
        final long to = run.get(run.size() - 1).toSeqNo();
        final ParquetFlusher.Settings now = settings.get();
        long expectedRows = 0;
        final List<ParquetMerger.Input> inputs = new ArrayList<>(run.size());
        for (ShardTable.StagedFile file : run) {
            inputs.add(new ParquetMerger.Input(file.name(), view.stagedInputFile(file)));
            expectedRows += file.rows();
        }

        // 1. merge, with the ids of the table
        final MessageType union = ParquetMerger.unionSchemaOf(inputs);
        final MessageType schema = ShardTable.withFieldIds(union, view.ensureColumns(union));
        delete(name); // the leftover of an earlier try of this range
        final ParquetMerger.Merged merged = ParquetMerger.mergeInputs(directory, inputs, schema, name, now.codec(), now.rowGroupBytes());
        hooks.reached("merged");
        if (merged.rows() != expectedRows) {
            // Never publish a file that lost rows. The staged files stay.
            delete(name);
            throw new IllegalStateException(
                "the merge of ["
                    + run.size()
                    + "] staged files of the range ["
                    + from
                    + ", "
                    + to
                    + "] wrote ["
                    + merged.rows()
                    + "] rows, and the names of the files count ["
                    + expectedRows
                    + "]"
            );
        }

        // 2. upload below the old history, under a key that is new and that no snapshot lists
        final String attempt = ShardTable.newAttempt();
        try (IndexInput in = directory.openInput(name, IOContext.DEFAULT)) {
            view.upload(from, to, attempt, new BufferedInputStream(new InputStreamIndexInput(in, merged.length())), merged.length());
        }
        hooks.reached("uploaded");

        // 3. the rows become visible
        final DataFile output = view.dataFile(from, to, attempt, merged.length(), new IndexInputFile(directory, name));
        view.append(output, to);
        hooks.reached("appended");

        // 4. the staged copies and the local file are redundant now
        final List<String> names = new ArrayList<>(run.size());
        for (ShardTable.StagedFile file : run) {
            names.add(file.name());
        }
        view.deleteStaged(names);
        delete(name);
        hooks.reached("cleaned");
        return merged.rows();
    }

    private void delete(String name) throws IOException {
        try {
            directory.deleteFile(name);
        } catch (NoSuchFileException e) {
            // already gone
        }
    }
}
