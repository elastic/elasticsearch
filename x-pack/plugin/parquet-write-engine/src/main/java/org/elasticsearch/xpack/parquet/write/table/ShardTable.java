/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.parquet.write.file.BlobInputFile;
import org.elasticsearch.xpack.parquet.write.file.ParquetLists;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One shard's view of the Iceberg table of its index. All the shards of an index append to the same table, and Iceberg's optimistic
 * commit (see {@link RepositoryTableOperations}) makes that safe with no coordination.
 *
 * <p>The data files of a shard are {@code data/shard-<n>/<from>-<to>.parquet}, named by the range of sequence numbers of their rows. The
 * name is what makes the table the source of truth for what a shard has uploaded: the high-water mark of a shard is the largest end of
 * a range among its files in the current snapshot.
 *
 * <p>The columns of the table have stable ids, which the data files carry as Parquet field ids. A column that a mapping update adds
 * gets the next id, and a column never changes its type.
 */
public final class ShardTable {

    private static final Pattern DATA_FILE = Pattern.compile(".*/data/shard-(\\d+)/(\\d{19})-(\\d{19})\\.parquet");

    /** Iceberg retries a commit that lost the race, 4 times by default. Shards that flush at the same moment need more. */
    private static final Map<String, String> TABLE_PROPERTIES = Map.of(
        "commit.retry.num-retries",
        "100",
        "commit.retry.min-wait-ms",
        "5",
        "commit.retry.max-wait-ms",
        "100"
    );

    private final Table table;
    private final RepositoryFileIO io;
    private final int shard;

    private ShardTable(Table table, RepositoryFileIO io, int shard) {
        this.table = table;
        this.io = io;
        this.shard = shard;
    }

    /** Opens the table at {@code location}, and creates it (with no column yet) when it does not exist. */
    public static ShardTable open(BlobStore blobStore, TableLocation location, int shard) {
        return open(blobStore, location, shard, null);
    }

    /**
     * Opens the table like {@link #open(BlobStore, TableLocation, int)}, and every commit of the table (also the one that creates it)
     * then creates or updates the entry of the table in the catalog, when {@code registration} is not null.
     */
    public static ShardTable open(BlobStore blobStore, TableLocation location, int shard, @Nullable CatalogRegistration registration) {
        final RepositoryFileIO io = new RepositoryFileIO(blobStore, location.blobPath(), location.rootUri());
        final RepositoryTableOperations operations = new RepositoryTableOperations(io, registration);
        if (operations.current() == null) {
            try {
                operations.commit(
                    null,
                    TableMetadata.newTableMetadata(
                        new Schema(),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        location.rootUri(),
                        TABLE_PROPERTIES
                    )
                );
            } catch (CommitFailedException e) {
                // another shard created the table first
            }
        }
        return new ShardTable(new BaseTable(operations, location.rootUri()), io, shard);
    }

    public int shard() {
        return shard;
    }

    // ---- schema ----

    /**
     * Makes sure that the table has every column of {@code schema}, and returns the id of each column by name. A column that the table
     * lacks is added with the next id. Shards add columns concurrently, so a column that appears between the check and the update is
     * simply found at the next try.
     */
    public Map<String, Integer> ensureColumns(MessageType schema) {
        for (int attempt = 0; attempt < 20; attempt++) {
            table.refresh();
            final Map<String, Types.NestedField> columns = new HashMap<>();
            for (Types.NestedField column : table.schema().columns()) {
                columns.put(column.name(), column);
            }
            final List<org.apache.parquet.schema.Type> missing = new ArrayList<>();
            final Map<String, Integer> wanted = new LinkedHashMap<>();
            for (org.apache.parquet.schema.Type field : schema.getFields()) {
                final Types.NestedField column = columns.get(field.getName());
                if (column == null) {
                    missing.add(field);
                    continue;
                }
                final boolean fileList = ParquetLists.isList(field);
                if (fileList != column.type().isListType()) {
                    // the mapping decides, and it never changes, so a field is a list in every file or in none
                    throw new IllegalStateException(
                        "column ["
                            + field.getName()
                            + "] is "
                            + (fileList ? "a list" : "not a list")
                            + " in the file and "
                            + (fileList ? "not " : "")
                            + "in the table"
                    );
                }
                wanted.put(field.getName(), column.fieldId());
                if (fileList) {
                    wanted.put(elementKey(field.getName()), column.type().asListType().elementId());
                }
            }
            if (missing.isEmpty()) {
                return wanted;
            }
            try {
                final var update = table.updateSchema();
                for (org.apache.parquet.schema.Type field : missing) {
                    // a null parent: the name is the literal name of a top-level column, and a dot in it is not a path
                    update.addColumn(null, field.getName(), icebergType(field));
                }
                update.commit();
            } catch (CommitFailedException | IllegalArgumentException e) {
                // another shard added a column: look again
            }
        }
        throw new IllegalStateException("could not add the columns of the schema to the table after repeated attempts");
    }

    /**
     * The key of the id of the element of a list column in the map that {@link #ensureColumns} returns. A NUL character cannot be in a
     * name that a user wrote, so the key cannot be the name of a column.
     */
    public static String elementKey(String listColumn) {
        return listColumn + "\u0000element";
    }

    /** The Parquet schema with the ids of the table on its fields, for the file that will be uploaded. */
    public static MessageType withFieldIds(MessageType schema, Map<String, Integer> ids) {
        final org.apache.parquet.schema.Types.MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
        for (org.apache.parquet.schema.Type field : schema.getFields()) {
            final Integer id = ids.get(field.getName());
            if (id == null) {
                throw new IllegalArgumentException("the column [" + field.getName() + "] has no id in the table");
            }
            if (ParquetLists.isList(field)) {
                // Iceberg reads a list by the ids of the list and of its element; the repeated group in between has none
                final Integer elementId = ids.get(elementKey(field.getName()));
                if (elementId == null) {
                    throw new IllegalArgumentException("the list column [" + field.getName() + "] has no element id in the table");
                }
                final PrimitiveType element = ParquetLists.elementOf(field);
                builder.addField(
                    ParquetLists.listOf(field.getName(), element.getPrimitiveTypeName(), element.getLogicalTypeAnnotation(), elementId)
                        .withId(id)
                );
                continue;
            }
            final PrimitiveType primitive = field.asPrimitiveType();
            org.apache.parquet.schema.Types.PrimitiveBuilder<PrimitiveType> column = org.apache.parquet.schema.Types.optional(
                primitive.getPrimitiveTypeName()
            );
            if (primitive.getLogicalTypeAnnotation() != null) {
                column = column.as(primitive.getLogicalTypeAnnotation());
            }
            builder.addField(column.id(id).named(field.getName()));
        }
        return builder.named("doc");
    }

    private static Type icebergType(org.apache.parquet.schema.Type field) {
        if (ParquetLists.isList(field)) {
            // the id of the element is a placeholder: the table gives a new column its real ids
            return Types.ListType.ofOptional(0, icebergPrimitive(ParquetLists.elementOf(field)));
        }
        return icebergPrimitive(field.asPrimitiveType());
    }

    private static Type icebergPrimitive(PrimitiveType primitive) {
        final LogicalTypeAnnotation logical = primitive.getLogicalTypeAnnotation();
        return switch (primitive.getPrimitiveTypeName()) {
            case INT64 -> logical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
                ? Types.TimestampType.withZone()
                : Types.LongType.get();
            case INT32 -> Types.IntegerType.get();
            case FLOAT -> Types.FloatType.get();
            case DOUBLE -> Types.DoubleType.get();
            case BOOLEAN -> Types.BooleanType.get();
            case BINARY -> Types.StringType.get();
            default -> throw new IllegalArgumentException(
                "column ["
                    + primitive.getName()
                    + "] has the type ["
                    + primitive.getPrimitiveTypeName()
                    + "], which tables do not support yet"
            );
        };
    }

    // ---- data files ----

    /** The location of the data file of this shard for the range of sequence numbers. */
    public String dataLocation(long fromSeqNo, long toSeqNo) {
        return io.rootLocation() + "/data/shard-" + shard + "/" + dataFileName(fromSeqNo, toSeqNo);
    }

    public static String dataFileName(long fromSeqNo, long toSeqNo) {
        return String.format(Locale.ROOT, "%019d-%019d.parquet", fromSeqNo, toSeqNo);
    }

    /** Uploads the bytes of a data file. Writing the same name again replaces it: a retry after a crash writes the same bytes. */
    public void upload(long fromSeqNo, long toSeqNo, InputStream bytes, long length) throws IOException {
        final String location = dataLocation(fromSeqNo, toSeqNo);
        final BlobContainer container = io.containerOf(location);
        container.writeBlob(RepositoryFileIO.PURPOSE, io.blobNameOf(location), bytes, length, false);
    }

    /** Builds the entry of the data file for the manifest, from the footer of the file. */
    public DataFile dataFile(long fromSeqNo, long toSeqNo, long length, InputFile parquetFile) throws IOException {
        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(dataLocation(fromSeqNo, toSeqNo))
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(length)
            .withMetrics(ParquetFileMetrics.read(parquetFile))
            .build();
    }

    /** Publishes the file: from this commit on, readers of the table see its rows. */
    public void append(DataFile file, long throughSeqNo) {
        table.newAppend()
            .appendFile(file)
            .set("parquet.shard", Integer.toString(shard))
            .set("parquet.through_seq_no", Long.toString(throughSeqNo))
            .commit();
    }

    /** The files of this shard in the current snapshot. */
    public List<UploadedFile> uploadedFiles() {
        final List<UploadedFile> files = new ArrayList<>();
        for (ShardFile file : shardFiles()) {
            files.add(file.file());
        }
        return files;
    }

    /** The files of this shard in the current snapshot, with their manifest entries, in the order of their sequence numbers. */
    public List<ShardFile> shardFiles() {
        return listing().files();
    }

    /**
     * The files of this shard in one snapshot, and that snapshot. A change that is planned from these files must name the snapshot, see
     * {@link #rewrite}.
     *
     * @param snapshotId the snapshot that lists the files, or -1 when the table has none yet
     */
    public record Listing(long snapshotId, List<ShardFile> files) {}

    /** The files of this shard in the current snapshot, with that snapshot. */
    public Listing listing() {
        table.refresh();
        final List<ShardFile> files = new ArrayList<>();
        final Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return new Listing(-1, files);
        }
        try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
            for (FileScanTask task : tasks) {
                final DataFile dataFile = task.file();
                final Matcher matcher = DATA_FILE.matcher(dataFile.location());
                if (matcher.matches() && Integer.parseInt(matcher.group(1)) == shard) {
                    files.add(
                        new ShardFile(
                            new UploadedFile(
                                dataFile.location(),
                                Long.parseLong(matcher.group(2)),
                                Long.parseLong(matcher.group(3)),
                                dataFile.recordCount(),
                                dataFile.fileSizeInBytes()
                            ),
                            dataFile.copy() // the scan may reuse its entry objects
                        )
                    );
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        files.sort((a, b) -> Long.compare(a.file().fromSeqNo(), b.file().fromSeqNo()));
        return new Listing(snapshot.snapshotId(), files);
    }

    /** A data file of a shard in the table. */
    public record UploadedFile(String location, long fromSeqNo, long toSeqNo, long rows, long bytes) {}

    /** A data file together with the entry of the manifest, which a rewrite needs to name the file that it replaces. */
    public record ShardFile(UploadedFile file, DataFile dataFile) {}

    // ---- compaction ----

    /** A reader of a data file of this shard in the repository, with ranged reads. */
    public InputFile inputFile(UploadedFile file) {
        return new BlobInputFile(io.containerOf(file.location()), io.blobNameOf(file.location()), file.bytes());
    }

    /**
     * Replaces the files of a compaction by the one file that holds their rows, in one commit: from this commit on, readers see the new
     * file and not the old ones, and a reader that holds an older snapshot still sees the old ones, which stay in the repository until
     * that snapshot expires.
     *
     * @param fromSnapshotId the snapshot that the replaced files were listed in (see {@link #listing}). Iceberg validates a rewrite that
     *                       it retries after another commit against the history since that snapshot. Without it, it walks the whole
     *                       history, which fails with "cannot determine history" as soon as a snapshot of the table has expired.
     * @throws ValidationException if a replaced file is no longer in the table
     */
    public void rewrite(Collection<DataFile> replaced, DataFile replacement, long fromSnapshotId) {
        final RewriteFiles rewrite = table.newRewrite()
            .rewriteFiles(Set.copyOf(replaced), Set.of(replacement))
            .set("parquet.shard", Integer.toString(shard))
            .set("parquet.compaction.replaced_files", Integer.toString(replaced.size()));
        if (fromSnapshotId >= 0) {
            rewrite.validateFromSnapshot(fromSnapshotId);
        }
        rewrite.commit();
    }

    /** Deletes the data file of the range from the repository, for a file that was uploaded and never listed in the table. */
    public void deleteDataFile(long fromSeqNo, long toSeqNo) {
        io.deleteFile(dataLocation(fromSeqNo, toSeqNo));
    }

    /**
     * Expires the snapshots that are older than the cut-off, except the current one, and deletes the files that only they list: the
     * data files that a compaction replaced and the metadata files of old snapshots. The cut-off is the time that a reader of an older
     * snapshot has to finish. Iceberg deletes a replaced file when the snapshot of the rewrite that replaced it expires, and the current
     * snapshot never expires, so the files of the latest compaction stay until a later commit makes its snapshot old.
     */
    public void expireSnapshots(long olderThanMillis) {
        table.refresh();
        if (table.snapshots().iterator().hasNext() == false) {
            return;
        }
        table.expireSnapshots().expireOlderThan(olderThanMillis).retainLast(1).cleanExpiredFiles(true).commit();
    }

    /**
     * Deletes the data files of this shard that no snapshot lists and whose rows are already uploaded: the leftover of a compaction that
     * crashed between its upload and its commit. A file above the high-water mark is not touched, because it can be the upload of a flush
     * that has not appended it yet. Must run on the compactor of the shard, so that no compaction of the shard is between its upload and
     * its commit.
     *
     * @return how many files it deleted
     */
    public int deleteOrphanFiles() throws IOException {
        // The order matters. A flush may append a file between our two looks at the table. The high-water mark comes first, so that a file
        // at or below it was appended before we listed the snapshots, and so is in the listing. Taken after the listing, the mark could
        // include a file that the listing missed, and we would delete the file of a flush that has just committed.
        final long through = uploadedThroughSeqNo();
        table.refresh();
        final Set<String> listed = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
                for (FileScanTask task : tasks) {
                    listed.add(task.file().location());
                }
            }
        }
        final String prefix = io.rootLocation() + "/data/shard-" + shard + "/";
        final BlobContainer container = io.containerOf(dataLocation(0, 0));
        int deleted = 0;
        for (String name : container.listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            final Matcher matcher = DATA_FILE.matcher(prefix + name);
            if (matcher.matches() && listed.contains(prefix + name) == false && Long.parseLong(matcher.group(3)) <= through) {
                io.deleteFile(prefix + name);
                deleted++;
            }
        }
        return deleted;
    }

    /** The largest sequence number of this shard in the current snapshot, or -1 when it has no file. */
    public long uploadedThroughSeqNo() {
        long through = -1;
        for (UploadedFile file : uploadedFiles()) {
            through = Math.max(through, file.toSeqNo());
        }
        return through;
    }

    /** The current Iceberg schema, for tests and for readers of the plan. */
    public Schema schema() {
        table.refresh();
        return table.schema();
    }
}
