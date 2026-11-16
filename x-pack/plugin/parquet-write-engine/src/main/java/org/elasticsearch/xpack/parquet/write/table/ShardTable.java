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
import org.elasticsearch.common.UUIDs;
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
 * <p>The data files of a shard are {@code data/shard-<n>/<history uuid>/<from>-<to>-<attempt>.parquet}. The range is the sequence numbers
 * of their rows, and it is what makes the table the source of truth for what a shard has uploaded: the high-water mark of a shard is the
 * largest end of a range among its files <em>of the current history</em> in the current snapshot.
 *
 * <p>A shard that starts with a new store (an empty primary after the loss of a node, or a restore) has a new history UUID and starts
 * again at sequence number 0, while the table still lists the files of the earlier history. The history in the path keeps the keys
 * apart, and a {@code ShardTable} only looks at, compacts and deletes the files of its own history. The attempt is new for every upload
 * try, so that a key is written once ({@code failIfAlreadyExists}): a retry after a crash never overwrites a blob that a crash left
 * partial, and the blob of the first try becomes an orphan that the cleanup deletes.
 *
 * <p>A table belongs to one index: it records the uuid of the index when it is created, and a shard of another index refuses to open it.
 *
 * <p>The columns of the table have stable ids, which the data files carry as Parquet field ids. A column that a mapping update adds
 * gets the next id, and a column never changes its type.
 */
public final class ShardTable {

    private static final Pattern DATA_FILE = Pattern.compile(".*/data/shard-(\\d+)/([^/]+)/(\\d{19})-(\\d{19})-([^/]+)\\.parquet");

    /** Table properties that name the index that owns the table. */
    static final String OWNER_INDEX_UUID = "elasticsearch.index_uuid";
    static final String OWNER_INDEX = "elasticsearch.index";

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
    private final String historyUuid;

    private ShardTable(Table table, RepositoryFileIO io, int shard, String historyUuid) {
        this.table = table;
        this.io = io;
        this.shard = shard;
        this.historyUuid = historyUuid;
    }

    /**
     * Opens the table at {@code location}, and creates it (with no column yet) when it does not exist. Every commit of the table (also
     * the one that creates it) then creates or updates the entry of the table in the catalog, when {@code registration} is not null.
     *
     * @param indexName   the index that owns the table, for the message of a refusal and the property of a new table
     * @param indexUuid   the uuid of that index. A table that another index created, or that does not name an owner, is refused.
     * @param historyUuid the history of the shard: the files that this object lists, compacts and deletes are the ones of this history
     * @throws IllegalStateException if the table belongs to another index
     */
    public static ShardTable open(
        BlobStore blobStore,
        TableLocation location,
        int shard,
        String indexName,
        String indexUuid,
        String historyUuid,
        @Nullable CatalogRegistration registration
    ) {
        final RepositoryFileIO io = new RepositoryFileIO(blobStore, location.blobPath(), location.rootUri());
        final RepositoryTableOperations operations = new RepositoryTableOperations(io, registration);
        if (operations.current() == null) {
            final Map<String, String> properties = new HashMap<>(TABLE_PROPERTIES);
            properties.put(OWNER_INDEX_UUID, indexUuid);
            properties.put(OWNER_INDEX, indexName);
            try {
                operations.commit(
                    null,
                    TableMetadata.newTableMetadata(
                        new Schema(),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        location.rootUri(),
                        properties
                    )
                );
            } catch (CommitFailedException e) {
                // another shard created the table first
            }
        }
        final Map<String, String> properties = operations.refresh().properties();
        final String owner = properties.get(OWNER_INDEX_UUID);
        if (indexUuid.equals(owner) == false) {
            throw new IllegalStateException(
                "the table at ["
                    + location.rootUri()
                    + "] "
                    + (owner == null
                        ? "does not name the index that owns it"
                        : "belongs to the index [" + properties.get(OWNER_INDEX) + "/" + owner + "]")
                    + ", and not to the index ["
                    + indexName
                    + "/"
                    + indexUuid
                    + "]; set another [index.parquet.base_path]"
            );
        }
        return new ShardTable(new BaseTable(operations, location.rootUri()), io, shard, historyUuid);
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

    /** The directory of the data files of this shard in its current history, with the slash at the end. */
    public String historyLocation() {
        return io.rootLocation() + "/data/shard-" + shard + "/" + historyUuid + "/";
    }

    /** The location of the data file of this shard and history for the range of sequence numbers and the upload attempt. */
    public String dataLocation(long fromSeqNo, long toSeqNo, String attempt) {
        return historyLocation() + dataFileName(fromSeqNo, toSeqNo, attempt);
    }

    public static String dataFileName(long fromSeqNo, long toSeqNo, String attempt) {
        return String.format(Locale.ROOT, "%019d-%019d-%s.parquet", fromSeqNo, toSeqNo, attempt);
    }

    /** A new name for an upload try: a key is never written twice. */
    public static String newAttempt() {
        return UUIDs.randomBase64UUID();
    }

    /**
     * Uploads the bytes of a data file. The key is new for every try, so a write to a key that exists is a bug and fails.
     *
     * @throws java.nio.file.FileAlreadyExistsException (or another {@code IOException} on a store that reports it differently) if the key
     *                                                  exists
     */
    public void upload(long fromSeqNo, long toSeqNo, String attempt, InputStream bytes, long length) throws IOException {
        final String location = dataLocation(fromSeqNo, toSeqNo, attempt);
        final BlobContainer container = io.containerOf(location);
        container.writeBlob(RepositoryFileIO.PURPOSE, io.blobNameOf(location), bytes, length, true);
    }

    /** Builds the entry of the data file for the manifest, from the footer of the file. */
    public DataFile dataFile(long fromSeqNo, long toSeqNo, String attempt, long length, InputFile parquetFile) throws IOException {
        return DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(dataLocation(fromSeqNo, toSeqNo, attempt))
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

    /** The files of this shard and history in the current snapshot. */
    public List<UploadedFile> uploadedFiles() {
        final List<UploadedFile> files = new ArrayList<>();
        for (ShardFile file : shardFiles()) {
            files.add(file.file());
        }
        return files;
    }

    /**
     * The files of this shard and history in the current snapshot, with their manifest entries, in the order of their sequence numbers.
     * The files of an earlier history of the shard stay in the table, and this object does not see them.
     */
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
                if (matcher.matches() && Integer.parseInt(matcher.group(1)) == shard && matcher.group(2).equals(historyUuid)) {
                    files.add(
                        new ShardFile(
                            new UploadedFile(
                                dataFile.location(),
                                Long.parseLong(matcher.group(3)),
                                Long.parseLong(matcher.group(4)),
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
    public void deleteDataFile(long fromSeqNo, long toSeqNo, String attempt) {
        io.deleteFile(dataLocation(fromSeqNo, toSeqNo, attempt));
    }

    /**
     * Whether the current snapshot lists the file at {@code location}, for a file of any shard or history. Recovery of a flush asks it by
     * the exact location, so that the file of another history with the same range is never taken for its own upload.
     */
    public boolean isListed(String location) {
        table.refresh();
        final Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return false;
        }
        try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().location().equals(location)) {
                    return true;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
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
     * Deletes the data files of this shard and history that no snapshot lists and whose rows are already uploaded: the leftover of a
     * compaction that crashed between its upload and its commit, and the blob of an upload try that a retry replaced. A file above the
     * high-water mark is not touched, because it can be the upload of a flush that has not appended it yet. The files of other histories
     * are never touched: their writer can still be alive. Must run on the compactor of the shard, so that no compaction of the shard is
     * between its upload and its commit.
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
        final String prefix = historyLocation();
        final BlobContainer container = io.containerOf(prefix + "x");
        int deleted = 0;
        for (String name : container.listBlobs(RepositoryFileIO.PURPOSE).keySet()) {
            final Matcher matcher = DATA_FILE.matcher(prefix + name);
            if (matcher.matches() && listed.contains(prefix + name) == false && Long.parseLong(matcher.group(4)) <= through) {
                io.deleteFile(prefix + name);
                deleted++;
            }
        }
        return deleted;
    }

    /** The largest sequence number of this shard and history in the current snapshot, or -1 when it has no file. */
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
