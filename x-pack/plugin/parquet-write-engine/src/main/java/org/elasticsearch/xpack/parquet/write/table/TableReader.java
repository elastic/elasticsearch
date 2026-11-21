/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Reads the current data files of an Iceberg table in a repository, and never writes: it does not create the table, does not commit, and
 * does not touch the catalog. The search replicas of an index of the write engine read their table with it, and so does an index that
 * mounts a table.
 *
 * <p>Every read lists the metadata of the table again, so it sees the commits of every writer. The version of a read is the {@code N} of
 * the {@code v<N>.metadata.json} that it read, which only grows: a writer that committed version {@code N} knows that a reader that has
 * read version {@code N} or later sees its files. A table that a catalog wrote ({@code <N>-<uuid>.metadata.json}) has the version
 * {@code N + 1}.
 */
public final class TableReader {

    private final TableLocation location;
    private final RepositoryTableOperations operations;
    private final BaseTable table;

    private TableReader(TableLocation location, RepositoryTableOperations operations) {
        this.location = location;
        this.operations = operations;
        this.table = new BaseTable(operations, location.rootUri());
    }

    /** A reader of the table at {@code location}, which need not exist yet. Opening it makes no request. */
    public static TableReader open(BlobStore blobStore, TableLocation location) {
        final RepositoryFileIO io = new RepositoryFileIO(blobStore, location.blobPath(), location.rootUri());
        return new TableReader(location, new RepositoryTableOperations(io, null));
    }

    public TableLocation location() {
        return location;
    }

    /**
     * A data file of the table.
     *
     * @param location       the location that the table names, below the root of the table
     * @param length         the length of the file in bytes, as the manifest gives it
     * @param rows           the number of rows of the file
     * @param sequenceNumber the sequence number of the commit that added the file (0 in a table of format version 1), which a later
     *                       append never makes smaller
     */
    public record DataFileEntry(String location, long length, long rows, long sequenceNumber) {

        public DataFileEntry(String location, long length, long rows) {
            this(location, length, rows, 0);
        }

        /** The path of the file relative to the root of the table, with {@code /} between the segments. */
        public String pathInTable(TableLocation table) {
            final String root = table.rootUri() + "/";
            if (location.startsWith(root) == false) {
                throw new IllegalStateException("the data file [" + location + "] is not below the table [" + table.rootUri() + "]");
            }
            return location.substring(root.length());
        }
    }

    /**
     * The files of one version of the table.
     *
     * @param version    the version of the metadata that was read, or 0 when the table does not exist yet
     * @param properties the properties of the table, empty when it does not exist yet
     * @param files      the data files of the current snapshot, in the order of the manifests
     */
    public record Contents(long version, Map<String, String> properties, List<DataFileEntry> files) {
        public static final Contents NONE = new Contents(0, Map.of(), List.of());
    }

    /**
     * The data files of the current snapshot of the table.
     *
     * @throws UnsupportedOperationException if the snapshot has delete files, which no reader of the format applies yet
     */
    public Contents read() {
        final TableMetadata metadata = operations.refresh();
        if (metadata == null) {
            return Contents.NONE;
        }
        final long version = versionOf(metadata.metadataFileLocation());
        final Snapshot snapshot = metadata.currentSnapshot();
        if (snapshot == null) {
            return new Contents(version, metadata.properties(), List.of());
        }
        final List<DataFileEntry> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.deletes().isEmpty() == false) {
                    throw new UnsupportedOperationException(
                        "the snapshot ["
                            + snapshot.snapshotId()
                            + "] of the table ["
                            + location.rootUri()
                            + "] has delete files, which are not supported yet"
                    );
                }
                final DataFile file = task.file();
                final Long sequenceNumber = file.fileSequenceNumber();
                files.add(
                    new DataFileEntry(
                        file.location(),
                        file.fileSizeInBytes(),
                        file.recordCount(),
                        sequenceNumber == null ? 0 : sequenceNumber
                    )
                );
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Contents(version, metadata.properties(), List.copyOf(files));
    }

    /**
     * The version of a metadata file {@code .../v<N>.metadata.json}, or of the {@code .../<N>-<uuid>.metadata.json} of a catalog, as
     * {@link RepositoryTableOperations#versionOfMetadataFile} counts it.
     */
    static long versionOf(String metadataFileLocation) {
        final int version = RepositoryTableOperations.versionOfMetadataFile(
            metadataFileLocation.substring(metadataFileLocation.lastIndexOf('/') + 1)
        );
        if (version < 0) {
            throw new IllegalStateException("not a metadata file of a version [" + metadataFileLocation + "]");
        }
        return version;
    }

    /**
     * Where a data file of the write engine sits in its table: {@code data/shard-<n>/<history uuid>/<from>-<to>-<attempt>.parquet}.
     *
     * @return the shard, history and first sequence number of the file, or {@code null} if the file does not have such a name
     */
    @Nullable
    public static ShardFileName parseShardFileName(String location) {
        final Matcher matcher = ShardTable.DATA_FILE.matcher(location);
        if (matcher.matches() == false) {
            return null;
        }
        return new ShardFileName(Integer.parseInt(matcher.group(1)), matcher.group(2), Long.parseLong(matcher.group(3)));
    }

    /** The shard, history and first sequence number that the name of a data file of the write engine holds. */
    public record ShardFileName(int shard, String historyUuid, long fromSeqNo) {}

    /** The uuid of the index that owns the table, which the write engine records when it creates the table, or {@code null}. */
    @Nullable
    public static String ownerIndexUuid(Map<String, String> properties) {
        return properties.get(ShardTable.OWNER_INDEX_UUID);
    }
}
