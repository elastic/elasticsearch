/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.compaction;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.exceptions.ValidationException;
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
import org.elasticsearch.xpack.parquet.write.file.ParquetMerger;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Merges the small data files that flushes uploaded into larger ones, so that the number of objects, footers and open files of every
 * reader stays small. A flush uploads what the local files hold when its trigger fires, and a slow shard flushes a few megabytes on age
 * or on idle, so without a compaction the table grows by files that readers like least.
 *
 * <p>A compaction of one shard does this:
 * <ol>
 *   <li>choose runs of <em>adjacent</em> small files in the order of their sequence numbers. Adjacent only, so the order of the rows of
 *       the shard does not change, and a reader that numbers the rows by file order sees the same numbers before and after;</li>
 *   <li>merge the files of a run into one local file, reading the inputs from the repository with ranged reads;</li>
 *   <li>upload it under a new key (the range that it covers and a new attempt). No snapshot lists that key, so no reader looks at it,
 *       and a retry after a crash uses another key and leaves the first as an orphan;</li>
 *   <li>commit one Iceberg rewrite that replaces the inputs by the output. This is the moment readers switch;</li>
 *   <li>delete the local file;</li>
 *   <li>expire the snapshots that are older than the delay, which deletes the replaced files, and delete the files that no snapshot lists
 *       (the output of a compaction that crashed after its upload).</li>
 * </ol>
 *
 * <p>Compaction touches only uploaded rows and none of the commit data of the shard: the rows are the same before and after. Only one
 * compaction of a shard may run at a time; the engine takes care of that. A flush may run at the same time, because it only appends files
 * and Iceberg retries a rewrite that another commit got in front of.
 */
public final class ParquetCompactor {

    private static final Logger logger = LogManager.getLogger(ParquetCompactor.class);

    /** Called after each step with its name, so that a test can stop the compaction there like a crash. */
    public interface Hooks {
        Hooks NONE = step -> {};

        void reached(String step) throws IOException;
    }

    /**
     * The settings that a compaction reads when it starts.
     *
     * @param targetFileBytes  a file below half of it is small
     * @param minFiles         the least number of files of a run
     * @param maxFileBytes     the most bytes that the files of a run may have together
     * @param deleteDelayMillis how long replaced files stay
     */
    public record Settings(
        long targetFileBytes,
        int minFiles,
        long maxFileBytes,
        long rowGroupBytes,
        CompressionCodecName codec,
        long deleteDelayMillis
    ) {}

    /** What a compaction did. */
    public record Result(int runs, int replacedFiles, long rows, int orphansDeleted) {}

    private final Directory directory;
    private final ShardTable table;
    private final Supplier<Settings> settings;
    private final Hooks hooks;
    private final Supplier<Long> clock;

    public ParquetCompactor(Directory directory, ShardTable table, Supplier<Settings> settings, Hooks hooks) {
        this(directory, table, settings, hooks, System::currentTimeMillis);
    }

    ParquetCompactor(Directory directory, ShardTable table, Supplier<Settings> settings, Hooks hooks, Supplier<Long> clock) {
        this.directory = directory;
        this.table = table;
        this.settings = settings;
        this.hooks = hooks;
        this.clock = clock;
    }

    /** Merges what the policy chooses, then deletes what is old. */
    public Result compact() throws IOException {
        final Settings now = settings.get();
        int runs = 0;
        int replaced = 0;
        long rows = 0;
        final ShardTable.Listing listing = table.listing();
        for (List<ShardTable.ShardFile> run : chooseRuns(listing.files(), now.targetFileBytes(), now.maxFileBytes(), now.minFiles())) {
            final long merged = compactRun(run, listing.snapshotId(), now);
            if (merged >= 0) {
                runs++;
                replaced += run.size();
                rows += merged;
            }
        }
        // the cut-off is the delay before now: a reader of an older snapshot has had that long to finish
        table.expireSnapshots(clock.get() - now.deleteDelayMillis());
        final int orphans = table.deleteOrphanFiles();
        return new Result(runs, replaced, rows, orphans);
    }

    /**
     * Groups the files, in the order of their sequence numbers, into runs of adjacent small files. A file of at least half the target is
     * large enough and ends the run, so a run never skips a file and the order of the rows is kept. A run has at most {@code maxBytes}
     * and a run with fewer than {@code minFiles} files is not worth a merge.
     */
    static List<List<ShardTable.ShardFile>> chooseRuns(List<ShardTable.ShardFile> files, long targetBytes, long maxBytes, int minFiles) {
        final List<List<ShardTable.ShardFile>> runs = new ArrayList<>();
        List<ShardTable.ShardFile> run = new ArrayList<>();
        long bytes = 0;
        for (ShardTable.ShardFile file : files) {
            final long size = file.file().bytes();
            if (size >= targetBytes / 2) {
                bytes = close(runs, run, minFiles);
                run = new ArrayList<>();
                continue;
            }
            if (run.isEmpty() == false && bytes + size > maxBytes) {
                close(runs, run, minFiles);
                run = new ArrayList<>();
                bytes = 0;
            }
            run.add(file);
            bytes += size;
        }
        close(runs, run, minFiles);
        return runs;
    }

    private static long close(List<List<ShardTable.ShardFile>> runs, List<ShardTable.ShardFile> run, int minFiles) {
        if (run.size() >= minFiles) {
            runs.add(run);
        }
        return 0;
    }

    /** @return the rows that it merged, or -1 if it gave up because a file of the run is gone */
    private long compactRun(List<ShardTable.ShardFile> run, long fromSnapshotId, Settings now) throws IOException {
        final long from = run.get(0).file().fromSeqNo();
        final long to = run.get(run.size() - 1).file().toSeqNo();
        long expectedRows = 0;
        final List<ParquetMerger.Input> inputs = new ArrayList<>(run.size());
        final List<DataFile> replaced = new ArrayList<>(run.size());
        for (ShardTable.ShardFile file : run) {
            inputs.add(new ParquetMerger.Input(file.file().location(), table.inputFile(file.file())));
            replaced.add(file.dataFile());
            expectedRows += file.file().rows();
        }

        // 1. merge, with the ids of the table
        final MessageType union = ParquetMerger.unionSchemaOf(inputs);
        final MessageType schema = ShardTable.withFieldIds(union, table.ensureColumns(union));
        final String name = LocalParquetFiles.compactedName(from, to);
        delete(name); // the leftover of an earlier try of this range
        final ParquetMerger.Merged merged = ParquetMerger.mergeInputs(directory, inputs, schema, name, now.codec(), now.rowGroupBytes());
        hooks.reached("merged");
        if (merged.rows() != expectedRows) {
            // Never publish a file that lost rows. The inputs stay in the table.
            delete(name);
            throw new IllegalStateException(
                "the merge of ["
                    + run.size()
                    + "] files of the range ["
                    + from
                    + ", "
                    + to
                    + "] wrote ["
                    + merged.rows()
                    + "] rows, and the table counts ["
                    + expectedRows
                    + "]"
            );
        }

        // 2. upload, below a key that is new and that no snapshot lists
        final String attempt = ShardTable.newAttempt();
        try (IndexInput in = directory.openInput(name, IOContext.DEFAULT)) {
            table.upload(from, to, attempt, new BufferedInputStream(new InputStreamIndexInput(in, merged.length())), merged.length());
        }
        hooks.reached("uploaded");

        // 3. the switch
        final DataFile output = table.dataFile(from, to, attempt, merged.length(), new IndexInputFile(directory, name));
        try {
            table.rewrite(replaced, output, fromSnapshotId);
        } catch (ValidationException e) {
            // A file of the run is not in the table any more, so somebody else changed it. Nothing was published: forget the output.
            logger.warn("a file of the compaction of [{}, {}] is gone from the table, so the compaction is dropped", from, to, e);
            table.deleteDataFile(from, to, attempt);
            delete(name);
            return -1;
        }
        hooks.reached("rewritten");

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
