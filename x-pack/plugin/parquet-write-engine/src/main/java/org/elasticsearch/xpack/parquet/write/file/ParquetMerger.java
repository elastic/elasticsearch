/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Merges the small local Parquet files that bulks wrote into one large file with row groups of the configured size and a real
 * compression codec. It decodes the values and encodes them again on purpose: a copy of the row groups would keep the tiny row groups of
 * the bulks, and those are what readers handle worst.
 *
 * <p>The values go from the column readers of the input to the column writers of the output with their repetition and definition levels,
 * and no row is ever assembled. An earlier version built a {@code Group} for each row and spent most of its time in that. This is safe
 * because a column has the same type, and so the same levels, in every file (see {@link #unionSchemaOf}). The row groups are cut like
 * the parquet-mr writer does it, by the size of the buffered data.
 *
 * <p>The files of one index come from one mapping that only grows, so a later file can have columns that an earlier file does not have.
 * The merged schema has all of them, and the rows of an earlier file get a null for the columns that it lacks.
 *
 * <p>The merge can use several threads without changing the file. The columns are split into contiguous groups and each thread encodes
 * the columns of one group of the same row group into its own page store (see {@link ColumnarParquetWriter}); the stores are written to
 * the block one after the other, in the order of the columns. The groups meet after each input, which is the only place where the row
 * group can be cut, so a row group is at most one input row group larger than the target. The inputs are read once for each group, and
 * each reader reads only the columns of its group.
 */
public final class ParquetMerger {

    /** The merged file: the name in the directory, its size and its number of rows. */
    public record Merged(String name, long length, long rows) {}

    /** A file to read the rows of: its name (for messages), and the file itself, which opens its own stream for each read. */
    public record Input(String name, InputFile file) {}

    private ParquetMerger() {}

    private static List<Input> inputsOf(Directory directory, List<LocalParquetFiles.LocalFile> files) {
        final List<Input> inputs = new ArrayList<>(files.size());
        for (LocalParquetFiles.LocalFile file : files) {
            inputs.add(new Input(file.name(), new IndexInputFile(directory, file.name())));
        }
        return inputs;
    }

    /**
     * The union of the schemas of the files, by column name, in the order that the columns first appear. Columns are flat. A column that
     * two files declare with different types is an error: the mapping never changes the type of a field.
     */
    public static MessageType unionSchema(Directory directory, List<LocalParquetFiles.LocalFile> files) throws IOException {
        return unionSchemaOf(inputsOf(directory, files));
    }

    /** {@link #unionSchema} with the footers read on up to {@code parallelism} threads, see {@link #unionSchemaOf(List, Executor, int)}. */
    public static MessageType unionSchema(Directory directory, List<LocalParquetFiles.LocalFile> files, Executor executor, int parallelism)
        throws IOException {
        return unionSchemaOf(inputsOf(directory, files), executor, parallelism);
    }

    /** {@link #unionSchema} for any files, for example the uploaded files that a compaction merges. */
    public static MessageType unionSchemaOf(List<Input> inputs) throws IOException {
        return unionSchemaOf(inputs, null, 1);
    }

    /**
     * {@link #unionSchemaOf(List)} that reads the footers on the calling thread and up to {@code parallelism - 1} tasks of the executor.
     * Every file has to be opened, because a later file can have a column that an earlier one lacks, and a run of a flush can have
     * thousands of small files: opening one costs about 3 ms, so 6,800 files kept the flush thread busy for 20 seconds. The result is the
     * same as the serial one, in the same order; only the reading is parallel. The executor must not be the one that the caller runs on.
     */
    public static MessageType unionSchemaOf(List<Input> inputs, Executor executor, int parallelism) throws IOException {
        final MessageType[] schemas = new MessageType[inputs.size()];
        final int workers = executor == null ? 1 : Math.min(parallelism, inputs.size() / MIN_FOOTERS_PER_WORKER);
        if (workers < 2) {
            for (int i = 0; i < schemas.length; i++) {
                schemas[i] = readSchema(inputs.get(i).file());
            }
        } else {
            readSchemasInParallel(inputs, schemas, executor, workers);
        }
        final Map<String, Type> columns = new LinkedHashMap<>();
        for (int i = 0; i < schemas.length; i++) {
            final Input input = inputs.get(i);
            for (Type field : schemas[i].getFields()) {
                final Type known = columns.putIfAbsent(field.getName(), field);
                if (known != null && sameTypeIgnoringIds(known, field) == false) {
                    throw new IllegalStateException(
                        "column ["
                            + field.getName()
                            + "] has the type ["
                            + known
                            + "] in one file and ["
                            + field
                            + "] in ["
                            + input.name()
                            + "]"
                    );
                }
            }
        }
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        for (Type field : columns.values()) {
            builder.addField(field);
        }
        return builder.named("doc");
    }

    /** The fewest footers for each worker: a task costs more than reading a few footers. */
    private static final int MIN_FOOTERS_PER_WORKER = 64;

    /** Fills {@code schemas} with the schema of each input, on up to {@code workers} threads (see {@link #runParallel}). */
    private static void readSchemasInParallel(List<Input> inputs, MessageType[] schemas, Executor executor, int workers)
        throws IOException {
        runParallel(executor, workers, new AtomicBoolean(), schemas.length, i -> schemas[i] = readSchema(inputs.get(i).file()));
    }

    /** One piece of work of {@link #runParallel}, by its number. */
    @FunctionalInterface
    private interface NumberedTask {
        void run(int number) throws IOException;
    }

    /**
     * Runs {@code task(0)} to {@code task(items - 1)} on up to {@code workers} threads: the calling thread and tasks of the executor. A
     * worker takes the next number until none is left, so work of different sizes balances itself, and a worker that the executor rejects
     * is not needed. The first failure makes the others stop before their next number, and the method returns or throws only when every
     * worker has ended, because nothing may write to the output (or read a stream that the caller closes) after it returns.
     */
    private static void runParallel(Executor executor, int workers, AtomicBoolean failed, int items, NumberedTask task) throws IOException {
        final AtomicInteger next = new AtomicInteger();
        final Callable<Void> worker = () -> {
            try {
                int number;
                while (failed.get() == false && (number = next.getAndIncrement()) < items) {
                    task.run(number);
                }
                return null;
            } catch (Exception | Error e) {
                failed.set(true);
                throw e;
            }
        };
        final List<FutureTask<Void>> tasks = new ArrayList<>(workers);
        for (int w = 1; w < workers; w++) {
            final FutureTask<Void> task1 = new FutureTask<>(worker);
            try {
                executor.execute(task1);
                tasks.add(task1);
            } catch (RuntimeException e) {
                break; // the executor does not take it (a rejection, or a shutdown): the workers that run take its numbers
            }
        }
        final FutureTask<Void> own = new FutureTask<>(worker);
        own.run();
        tasks.add(0, own);
        await(tasks, failed);
    }

    private static boolean sameTypeIgnoringIds(Type a, Type b) {
        if (ParquetLists.isList(a) || ParquetLists.isList(b)) {
            // a field is a list in every file or in none: the mapping decides, and it never changes
            return ParquetLists.isList(a)
                && ParquetLists.isList(b)
                && sameTypeIgnoringIds(ParquetLists.elementOf(a), ParquetLists.elementOf(b));
        }
        return a.isPrimitive()
            && b.isPrimitive()
            && a.asPrimitiveType().getPrimitiveTypeName() == b.asPrimitiveType().getPrimitiveTypeName()
            && java.util.Objects.equals(a.getLogicalTypeAnnotation(), b.getLogicalTypeAnnotation());
    }

    private static MessageType readSchema(InputFile file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(file, readOptions())) {
            return reader.getFooter().getFileMetaData().getSchema();
        }
    }

    private static ParquetReadOptions readOptions() {
        return ParquetReadOptionsFactory.create(new ParquetCodecs());
    }

    /**
     * Writes the rows of {@code inputs}, in the order of the list, to a new file of the directory that is synced and renamed to
     * {@code outputName} when it is complete.
     *
     * @param outputSchema    the schema of the merged file, with the field ids of the table
     * @param codec           the compression of the merged file
     * @param rowGroupBytes   the size of the buffered data at which a row group is written
     */
    public static Merged merge(
        Directory directory,
        List<LocalParquetFiles.LocalFile> inputs,
        MessageType outputSchema,
        String outputName,
        CompressionCodecName codec,
        long rowGroupBytes
    ) throws IOException {
        return mergeInputs(directory, inputsOf(directory, inputs), outputSchema, outputName, codec, rowGroupBytes);
    }

    /**
     * {@link #merge} with up to {@code parallelism} threads, see
     * {@link #mergeInputs(Directory, List, MessageType, String, CompressionCodecName, long, Executor, int)}.
     */
    public static Merged merge(
        Directory directory,
        List<LocalParquetFiles.LocalFile> inputs,
        MessageType outputSchema,
        String outputName,
        CompressionCodecName codec,
        long rowGroupBytes,
        Executor executor,
        int parallelism
    ) throws IOException {
        return mergeInputs(directory, inputsOf(directory, inputs), outputSchema, outputName, codec, rowGroupBytes, executor, parallelism);
    }

    /** {@link #merge} for any files; the output is always a file of the directory, because the upload needs its length. */
    public static Merged mergeInputs(
        Directory directory,
        List<Input> inputs,
        MessageType outputSchema,
        String outputName,
        CompressionCodecName codec,
        long rowGroupBytes
    ) throws IOException {
        return mergeInputs(directory, inputs, outputSchema, outputName, codec, rowGroupBytes, null, 1);
    }

    /**
     * {@link #mergeInputs} with up to {@code parallelism} threads: the calling thread and tasks that it hands to {@code executor}, and it
     * waits for all of them. The merge is serial, as with the method above, when the parallelism is 1, when the schema has one field, or
     * when an input has a row group that is too large for the groups to meet after it (a row group of a compacted file).
     *
     * <p>The executor must not be one that the caller runs on, or the tasks may never get a thread. A task that the executor rejects is
     * not needed: the other workers, and the calling thread, take its groups.
     */
    public static Merged mergeInputs(
        Directory directory,
        List<Input> inputs,
        MessageType outputSchema,
        String outputName,
        CompressionCodecName codec,
        long rowGroupBytes,
        Executor executor,
        int parallelism
    ) throws IOException {
        final Plan plan = executor == null ? Plan.serial(outputSchema) : plan(inputs, outputSchema, parallelism, rowGroupBytes);
        final List<MessageType> groups = plan.groups();
        long rows = 0;
        String tempName = null;
        boolean success = false;
        try (IndexOutput out = directory.createTempOutput(LocalParquetFiles.TEMP_PREFIX, "parquet", IOContext.DEFAULT)) {
            tempName = out.getName();
            final BatchParquetWriter.IndexOutputFile file = new BatchParquetWriter.IndexOutputFile(out);
            try (
                ColumnarParquetWriter writer = groups.size() == 1
                    ? ColumnarParquetWriter.forRows(file, outputSchema, codec, rowGroupBytes)
                    : ColumnarParquetWriter.forRowGroups(file, outputSchema, codec, rowGroupBytes, groups)
            ) {
                if (groups.size() == 1) {
                    for (Input input : inputs) {
                        rows += copyRows(input, outputSchema, writer);
                    }
                } else {
                    rows = copyInParallel(inputs, plan, writer, executor, parallelism);
                }
                writer.finish();
            }
            success = true;
        } finally {
            if (success == false && tempName != null) {
                try {
                    directory.deleteFile(tempName);
                } catch (IOException e) {
                    // removed with the other temporary files when the engine starts again
                }
            }
        }
        directory.sync(List.of(tempName));
        directory.rename(tempName, outputName);
        directory.syncMetaData();
        return new Merged(outputName, directory.fileLength(outputName), rows);
    }

    /**
     * The estimated cost of encoding a value, and of a byte of it, in nanoseconds. The columns are balanced by the cost of encoding them,
     * which is not the compressed size: a column of high-entropy numbers is large when compressed and cheap to encode, and a column of
     * text that compresses well is small and costs much (the statistics, the dictionary and the compression all work on the bytes that
     * the column had before it was compressed).
     */
    private static final long NANOS_PER_VALUE = 30;
    private static final long NANOS_PER_BYTE = 3;

    /** The footers that are read to balance the groups. */
    private static final int MAX_SAMPLED_FOOTERS = 8;

    /**
     * The largest input row group for which the groups meet after each input. The row group of the output is cut there, so this is how far
     * a row group can overshoot, in rows. A local file of a bulk has at most 5,000 rows.
     */
    private static final long MAX_PARALLEL_INPUT_ROWS = 100_000;

    /**
     * How many groups there are for each thread. The columns of a group are encoded by one thread, so the groups cannot be balanced
     * better than the cost of the most expensive column, and an estimate of the cost is never exact. A thread that takes the next group
     * when it has finished one balances the rest by itself, at the price of one more read of the footer of each input for each group. On
     * 600,000 real rows 1 to 3 groups for each thread merged equally fast and 4 were slower.
     */
    static final int GROUPS_PER_THREAD = 2;

    /**
     * How many row groups an epoch is a part of at most: a row group is cut only between epochs, so it is at most an epoch larger than
     * {@code row_group_size}, which is an eighth of it.
     */
    static final int EPOCHS_PER_ROW_GROUP = 8;

    /** The most files of an epoch, so that the footers that an epoch holds in memory stay a few megabytes. */
    static final int MAX_INPUTS_PER_EPOCH = 64;

    /**
     * The groups of the columns of a merge, the order in which the threads take them (the most expensive first, so that the last groups
     * that are taken are the cheap ones), and how many inputs make an epoch.
     */
    record Plan(List<MessageType> groups, int[] dispatchOrder, int inputsPerEpoch) {
        static Plan serial(MessageType schema) {
            return new Plan(List.of(schema), new int[] { 0 }, 1);
        }
    }

    /**
     * Splits the fields of the output into at most {@code parallelism} times {@link #GROUPS_PER_THREAD} contiguous groups of about the
     * same cost, or returns the whole schema as the only group when the merge has to be serial. The cost of a field is an estimate of the
     * time to encode its column chunks in a sample of the inputs, from the number of values and the bytes that they had before
     * compression.
     *
     * <p>An epoch is the number of inputs that is about an eighth of a row group of {@code rowGroupBytes}, from the bytes for each row and
     * the rows for each input of the same sample, and at least one input. The bytes before compression of the inputs are at least what
     * the writer buffers (it has dictionaries, they do not), so an epoch is not larger than an eighth of a row group.
     */
    static Plan plan(List<Input> inputs, MessageType outputSchema, int parallelism, long rowGroupBytes) throws IOException {
        final int fields = outputSchema.getFieldCount();
        final int wanted = (int) Math.min((long) parallelism * GROUPS_PER_THREAD, fields);
        if (parallelism < 2 || wanted < 2 || inputs.isEmpty()) {
            return Plan.serial(outputSchema);
        }
        final long[] weight = new long[fields];
        Arrays.fill(weight, 1); // a field that the sample lacks still costs something
        long sampledRows = 0;
        long sampledBytes = 0;
        final int samples = Math.min(inputs.size(), MAX_SAMPLED_FOOTERS);
        for (int sample = 0; sample < samples; sample++) {
            final Input input = inputs.get((int) ((long) sample * inputs.size() / samples));
            try (ParquetFileReader reader = ParquetFileReader.open(input.file(), readOptions())) {
                for (BlockMetaData block : reader.getFooter().getBlocks()) {
                    if (block.getRowCount() > MAX_PARALLEL_INPUT_ROWS) {
                        return Plan.serial(outputSchema);
                    }
                    sampledRows += block.getRowCount();
                    for (ColumnChunkMetaData chunk : block.getColumns()) {
                        sampledBytes += chunk.getTotalUncompressedSize();
                        final String field = chunk.getPath().toArray()[0];
                        if (outputSchema.containsField(field)) {
                            weight[outputSchema.getFieldIndex(field)] += chunk.getValueCount() * NANOS_PER_VALUE + chunk
                                .getTotalUncompressedSize() * NANOS_PER_BYTE;
                        }
                    }
                }
            }
        }
        long remaining = 0;
        for (long w : weight) {
            remaining += w;
        }
        final List<MessageType> groups = new ArrayList<>(wanted);
        final long[] groupWeight = new long[wanted];
        int start = 0;
        for (int group = 0; group < wanted; group++) {
            final int groupsLeft = wanted - group;
            int end;
            if (groupsLeft == 1) {
                end = fields;
                groupWeight[group] = remaining;
            } else {
                // at least one field, and at least one for each group that follows
                final long target = remaining / groupsLeft;
                final int maxEnd = fields - (groupsLeft - 1);
                end = start;
                long taken = 0;
                do {
                    taken += weight[end++];
                } while (end < maxEnd && taken < target);
                remaining -= taken;
                groupWeight[group] = taken;
            }
            groups.add(new MessageType(outputSchema.getName(), outputSchema.getFields().subList(start, end)));
            start = end;
        }
        final Integer[] order = new Integer[wanted];
        for (int i = 0; i < wanted; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (x, y) -> Long.compare(groupWeight[y], groupWeight[x]));
        final int[] dispatchOrder = new int[wanted];
        for (int i = 0; i < wanted; i++) {
            dispatchOrder[i] = order[i];
        }
        final long bytesPerRow = Math.max(1, sampledBytes / Math.max(1, sampledRows));
        final long rowsPerInput = Math.max(1, sampledRows / samples);
        final long epochRows = rowGroupBytes / bytesPerRow / EPOCHS_PER_ROW_GROUP;
        final int inputsPerEpoch = (int) Math.max(1, Math.min(MAX_INPUTS_PER_EPOCH, epochRows / rowsPerInput));
        return new Plan(groups, dispatchOrder, inputsPerEpoch);
    }

    /**
     * Copies the inputs in epochs of {@link Plan#inputsPerEpoch} inputs, one epoch after the other. For each epoch the footers of its
     * inputs are read once, on up to {@code parallelism} threads, and then the threads take the groups one at a time (see
     * {@link #runParallel}): a thread copies its group of the columns of every input of the epoch, in the order of the inputs, from the
     * footers that are already read, into the page stores of that group. The groups meet after each epoch, and that is the only place where
     * the writer cuts a row group. Reading a footer for every group of every input took 37% of the CPU of the merge with 2,000-row files,
     * and a meeting after every input kept the threads waiting for each other.
     */
    private static long copyInParallel(List<Input> inputs, Plan plan, ColumnarParquetWriter writer, Executor executor, int parallelism)
        throws IOException {
        final List<MessageType> groups = plan.groups();
        final AtomicBoolean failed = new AtomicBoolean();
        long rows = 0;
        for (int start = 0; start < inputs.size(); start += plan.inputsPerEpoch()) {
            final List<Input> epoch = inputs.subList(start, Math.min(inputs.size(), start + plan.inputsPerEpoch()));
            final ParquetMetadata[] footers = new ParquetMetadata[epoch.size()];
            runParallel(executor, Math.min(parallelism, epoch.size()), failed, epoch.size(), i -> footers[i] = readFooter(epoch.get(i)));
            final long[] groupRows = new long[groups.size()]; // each index is written by one worker, and read after all have ended
            runParallel(executor, Math.min(parallelism, groups.size()), failed, groups.size(), number -> {
                final int group = plan.dispatchOrder()[number];
                groupRows[group] = copyGroup(epoch, footers, group, groups.get(group), writer, failed);
            });
            final long copied = groupRows[0];
            for (long groupCopied : groupRows) {
                if (groupCopied != copied) {
                    throw new IllegalStateException(
                        "the groups copied different numbers of rows of [" + epoch.get(0).name() + "] and after it"
                    );
                }
            }
            writer.inputDone(copied); // the one place where a row group can be cut
            rows += copied;
        }
        return rows;
    }

    private static ParquetMetadata readFooter(Input input) throws IOException {
        try (SeekableInputStream stream = input.file().newStream()) {
            return ParquetFileReader.readFooter(input.file(), readOptions(), stream);
        }
    }

    /** Waits for all workers of one input, uninterruptibly. The first failure is thrown, and the others are added to it. */
    private static void await(List<FutureTask<Void>> tasks, AtomicBoolean failed) throws IOException {
        Throwable failure = null;
        boolean interrupted = false;
        for (FutureTask<Void> task : tasks) {
            while (true) {
                try {
                    task.get();
                    break;
                } catch (ExecutionException e) {
                    failed.set(true);
                    if (failure == null) {
                        failure = e.getCause();
                    } else {
                        failure.addSuppressed(e.getCause());
                    }
                    break;
                } catch (InterruptedException e) {
                    // the task still runs, and nothing may be released until it ends: wait again and report the interrupt at the end
                    interrupted = true;
                    failed.set(true);
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            final InterruptedIOException e = new InterruptedIOException("interrupted while the groups of a merge ran");
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

    /** The values of the columns of one group in every input of an epoch, in the order of the inputs. */
    private static long copyGroup(
        List<Input> epoch,
        ParquetMetadata[] footers,
        int group,
        MessageType groupSchema,
        ColumnarParquetWriter writer,
        AtomicBoolean failed
    ) throws IOException {
        long rows = 0;
        for (int i = 0; i < epoch.size() && failed.get() == false; i++) {
            rows += copyGroupOfInput(epoch.get(i), footers[i], group, groupSchema, writer, failed);
        }
        return rows;
    }

    /** A reader that uses the footer that is already read. It closes the stream that it opens, also when it cannot be created. */
    private static ParquetFileReader openReader(Input input, ParquetMetadata footer) throws IOException {
        final SeekableInputStream stream = input.file().newStream();
        try {
            return ParquetFileReader.open(input.file(), footer, readOptions(), stream);
        } catch (IOException | RuntimeException e) {
            try {
                stream.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    /** The values of the columns of one group in the row groups of an input, to the page stores of that group. */
    private static long copyGroupOfInput(
        Input input,
        ParquetMetadata footer,
        int group,
        MessageType groupSchema,
        ColumnarParquetWriter writer,
        AtomicBoolean failed
    ) throws IOException {
        long rows = 0;
        try (ParquetFileReader reader = openReader(input, footer)) {
            final FileMetaData metaData = footer.getFileMetaData();
            final MessageType fileSchema = metaData.getSchema();
            final List<Type> present = new ArrayList<>();
            for (Type field : groupSchema.getFields()) {
                if (fileSchema.containsField(field.getName())) {
                    present.add(fileSchema.getType(field.getName()));
                }
            }
            final List<ColumnDescriptor> outputColumns = groupSchema.getColumns();
            if (present.isEmpty()) {
                // an earlier file that has none of the columns of the group: a null in each, and nothing to read
                final long count = reader.getRecordCount();
                for (long row = 0; row < count; row++) {
                    for (int i = 0; i < outputColumns.size(); i++) {
                        writer.columnWriter(group, i).writeNull(0, 0);
                    }
                    writer.endRecord(group);
                }
                return count;
            }
            final MessageType requested = new MessageType(fileSchema.getName(), present);
            reader.setRequestedSchema(requested);
            final Map<String, ColumnDescriptor> inputColumns = new HashMap<>();
            for (ColumnDescriptor column : requested.getColumns()) {
                inputColumns.put(String.join(".", column.getPath()), column);
            }
            final GroupConverter converter = new GroupRecordConverter(requested).getRootConverter();
            PageReadStore pages;
            while (failed.get() == false && (pages = reader.readNextRowGroup()) != null) {
                final ColumnReadStoreImpl readStore = new ColumnReadStoreImpl(pages, converter, requested, metaData.getCreatedBy());
                final ColumnCopier[] copiers = new ColumnCopier[outputColumns.size()];
                for (int i = 0; i < copiers.length; i++) {
                    final ColumnDescriptor target = outputColumns.get(i);
                    final ColumnDescriptor source = inputColumns.get(String.join(".", target.getPath()));
                    copiers[i] = source == null
                        ? new ColumnCopier(writer, group, i, null, target, input.name())
                        : new ColumnCopier(writer, group, i, readStore.getColumnReader(source), target, input.name());
                }
                final long count = pages.getRowCount();
                for (long row = 0; row < count; row++) {
                    for (ColumnCopier copier : copiers) {
                        copier.copyRecord();
                    }
                    writer.endRecord(group);
                }
                rows += count;
            }
        }
        return rows;
    }

    private static long copyRows(Input input, MessageType outputSchema, ColumnarParquetWriter writer) throws IOException {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(input.file(), readOptions())) {
            final FileMetaData metaData = reader.getFooter().getFileMetaData();
            final MessageType inputSchema = metaData.getSchema();
            final Map<String, ColumnDescriptor> inputColumns = new HashMap<>();
            for (ColumnDescriptor column : inputSchema.getColumns()) {
                inputColumns.put(String.join(".", column.getPath()), column);
            }
            final List<ColumnDescriptor> outputColumns = outputSchema.getColumns();
            // the converter is only a required argument of the read store: no value is ever converted
            final GroupConverter converter = new GroupRecordConverter(inputSchema).getRootConverter();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                final ColumnReadStoreImpl readStore = new ColumnReadStoreImpl(pages, converter, inputSchema, metaData.getCreatedBy());
                final ColumnCopier[] copiers = new ColumnCopier[outputColumns.size()];
                for (int i = 0; i < copiers.length; i++) {
                    final ColumnDescriptor target = outputColumns.get(i);
                    final ColumnDescriptor source = inputColumns.get(String.join(".", target.getPath()));
                    // a column that an earlier file lacks (added later by a mapping update) is a null in every row
                    copiers[i] = source == null
                        ? new ColumnCopier(writer, 0, i, null, target, input.name())
                        : new ColumnCopier(writer, 0, i, readStore.getColumnReader(source), target, input.name());
                }
                final long count = pages.getRowCount();
                for (long row = 0; row < count; row++) {
                    for (ColumnCopier copier : copiers) {
                        copier.copyRecord();
                    }
                    writer.endRecord();
                }
                rows += count;
            }
        }
        return rows;
    }

    /**
     * Copies the values of one column of the current row group of an input to the output, one record (a row) at a time. A record is the
     * values up to the next one with the repetition level 0, which is one value for a flat column and all the elements of a list.
     */
    private static final class ColumnCopier {
        private final ColumnarParquetWriter writer;
        private final int group;
        private final int index;
        /** Null when the input file has no such column. */
        private final ColumnReader reader;
        private final int maxDefinitionLevel;
        private final PrimitiveType.PrimitiveTypeName type;
        private long remaining;

        ColumnCopier(ColumnarParquetWriter writer, int group, int index, ColumnReader reader, ColumnDescriptor target, String inputName) {
            this.writer = writer;
            this.group = group;
            this.index = index;
            this.reader = reader;
            this.maxDefinitionLevel = target.getMaxDefinitionLevel();
            this.type = target.getPrimitiveType().getPrimitiveTypeName();
            if (reader != null) {
                final ColumnDescriptor source = reader.getDescriptor();
                if (source.getMaxDefinitionLevel() != target.getMaxDefinitionLevel()
                    || source.getMaxRepetitionLevel() != target.getMaxRepetitionLevel()
                    || source.getPrimitiveType().getPrimitiveTypeName() != type) {
                    // the levels of one column are the same in every file; copying them as they are would write a broken file otherwise
                    throw new IllegalStateException(
                        "column [" + String.join(".", target.getPath()) + "] has other levels or another type in [" + inputName + "]"
                    );
                }
                this.remaining = reader.getTotalValueCount();
            }
        }

        void copyRecord() {
            final ColumnWriter out = writer.columnWriter(group, index);
            if (reader == null) {
                out.writeNull(0, 0);
                return;
            }
            do {
                final int repetitionLevel = reader.getCurrentRepetitionLevel();
                final int definitionLevel = reader.getCurrentDefinitionLevel();
                if (definitionLevel < maxDefinitionLevel) {
                    out.writeNull(repetitionLevel, definitionLevel); // a null, an empty list or a list with no value
                } else {
                    switch (type) {
                        case INT32 -> out.write(reader.getInteger(), repetitionLevel, definitionLevel);
                        case INT64 -> out.write(reader.getLong(), repetitionLevel, definitionLevel);
                        case FLOAT -> out.write(reader.getFloat(), repetitionLevel, definitionLevel);
                        case DOUBLE -> out.write(reader.getDouble(), repetitionLevel, definitionLevel);
                        case BOOLEAN -> out.write(reader.getBoolean(), repetitionLevel, definitionLevel);
                        case BINARY -> out.write(reader.getBinary(), repetitionLevel, definitionLevel);
                        default -> throw new IllegalStateException(
                            "column [" + reader.getDescriptor() + "] has the unsupported type [" + type + "]"
                        );
                    }
                }
                reader.consume();
                remaining--;
            } while (remaining > 0 && reader.getCurrentRepetitionLevel() != 0);
        }
    }
}
