/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.Locale;

/**
 * The index settings of the Parquet write engine. This is a prototype, so the names are experimental.
 */
public final class ParquetFormatSettings {

    /** The storage format of an index: normal Lucene indices, or the Parquet write engine. */
    public enum IndexFormat {
        NATIVE,
        PARQUET;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * Selects the engine of an index. The rules of the format (the strictly columnar mode, no replicas, a repository) are checked by
     * {@link ParquetIndexSettingProvider} at index creation, because a validator of this setting only sees the settings of the request,
     * before the settings of the matching templates are merged in. A settings update of the replicas of an existing index is not checked
     * at all by a setting validator; see the allocation decider in the plan.
     */
    public static final Setting<IndexFormat> INDEX_FORMAT = Setting.enumSetting(
        IndexFormat.class,
        "index.storage_format",
        IndexFormat.NATIVE,
        Property.IndexScope,
        Property.Final
    );

    /** The name of the registered snapshot repository that holds the Parquet files of the index. */
    public static final Setting<String> REPOSITORY = Setting.simpleString("index.parquet.repository", Property.IndexScope, Property.Final);

    /**
     * The path of the table below the base path of the repository, for example {@code logs/2026}. The default is the name and the uuid
     * of the index, so that two indices never share a table.
     */
    public static final Setting<String> BASE_PATH = Setting.simpleString("index.parquet.base_path", Property.IndexScope, Property.Final);

    /**
     * Whether every commit of the table creates or updates the entry of the table in the file-based catalog of the repository
     * ({@code <base path>/_catalog/<namespace>/<index name>.json}).
     */
    public static final Setting<Boolean> CATALOG_ENABLED = Setting.boolSetting(
        "index.parquet.catalog.enabled",
        true,
        Property.IndexScope,
        Property.Final
    );

    /** The namespace of the table in the catalog; a dotted value is a nested namespace. */
    public static final Setting<String> CATALOG_NAMESPACE = Setting.simpleString("index.parquet.catalog.namespace", "default", value -> {
        for (String level : value.split("\\.", -1)) {
            if (level.matches("[A-Za-z0-9_-]+") == false) {
                throw new IllegalArgumentException(
                    "[index.parquet.catalog.namespace] must be levels of letters, digits, [_] and [-] separated by dots, but was ["
                        + value
                        + "]"
                );
            }
        }
    }, Property.IndexScope, Property.Final);

    /** When a bulk is acknowledged: with its file on the local disk only, or also with a copy of the file in the object store. */
    public enum Durability {
        /**
         * The bulk is acknowledged when its local file is durable. The loss of the node or of its disk loses every acknowledged row that
         * no flush has uploaded yet.
         */
        LOCAL,
        /**
         * The bulk is acknowledged when a copy of its file is also in the repository, below {@code staging/}. The copy plays the role of
         * the translog: a shard that starts with a new store adopts the staged files of the old history. A flush deletes the staged files
         * that it has put in the table. The repository must exist and accept writes at the time of the bulk.
         */
        STAGED;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final Setting<Durability> DURABILITY = Setting.enumSetting(
        Durability.class,
        "index.parquet.durability",
        Durability.STAGED,
        Property.IndexScope,
        Property.Final
    );

    /** The compression of the files that are uploaded. */
    public enum Compression {
        UNCOMPRESSED,
        SNAPPY,
        ZSTD;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final Setting<Compression> COMPRESSION = Setting.enumSetting(
        Compression.class,
        "index.parquet.compression",
        Compression.ZSTD,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * The compression of the local files that every bulk writes, and so of their staged copies. They live until the next flush merges
     * them, and the flush reads them once. Compression makes them smaller on the disk and in the counters of the flush threshold and of
     * the backpressure, and costs CPU on the path that acknowledges the bulk. With ZSTD the level is 1 and the files have no dictionary
     * (see {@code ColumnarParquetWriter}), because the merge of the flush encodes the rows again, and the file that it uploads keeps the
     * dictionary and the level 3. On 600,000 distinct documents with 105 fields, the local files were 345 bytes for each row and the
     * uploaded file 310, and the write threads used 6 microseconds of CPU for each document (11 to 12 with a dictionary and level 3).
     */
    public static final Setting<Compression> LOCAL_COMPRESSION = Setting.enumSetting(
        Compression.class,
        "index.parquet.local_compression",
        Compression.ZSTD,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * The size of the local files at which the periodic flush merges and uploads them. It is a size in bytes and not a number of rows,
     * because a row can be 20 bytes or 2 KB, and the readers of the files want a predictable file size.
     */
    public static final Setting<ByteSizeValue> FLUSH_THRESHOLD_SIZE = Setting.byteSizeSetting(
        "index.parquet.flush_threshold_size",
        ByteSizeValue.ofGb(1),
        Property.IndexScope,
        Property.Dynamic
    );

    /** The age of the oldest local file at which the periodic flush uploads, so that a slow shard does not keep rows only on its disk. */
    public static final Setting<TimeValue> FLUSH_THRESHOLD_AGE = Setting.timeSetting(
        "index.parquet.flush_threshold_age",
        TimeValue.timeValueMinutes(30),
        Property.IndexScope,
        Property.Dynamic
    );

    /** The size of the local files at which new bulks are rejected with a 429, until a flush frees space. */
    public static final Setting<ByteSizeValue> MAX_LOCAL_SIZE = Setting.byteSizeSetting(
        "index.parquet.max_local_size",
        ByteSizeValue.ofGb(4),
        Property.IndexScope,
        Property.Dynamic
    );

    /** The largest file that a flush uploads. A flush of more local data uploads several files. */
    public static final Setting<ByteSizeValue> MAX_FILE_SIZE = Setting.byteSizeSetting(
        "index.parquet.max_file_size",
        ByteSizeValue.ofGb(1),
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * The number of threads that encode the columns of one run of a flush: the thread of the flush and tasks of the {@code parquet_merge}
     * pool. The columns are split into that many groups, and the file that is written is the same as the one that one thread writes.
     * 1 merges on the thread of the flush alone. The tasks come from a pool of at most half the processors of the node, so a larger
     * value than the pool has threads for does not make the merge faster.
     */
    public static final Setting<Integer> FLUSH_MERGE_PARALLELISM = Setting.intSetting(
        "index.parquet.flush_merge_parallelism",
        4,
        1,
        16,
        Property.IndexScope,
        Property.Dynamic
    );

    /** The size of the buffered data at which the writer of an uploaded file starts a new row group. */
    public static final Setting<ByteSizeValue> ROW_GROUP_SIZE = Setting.byteSizeSetting(
        "index.parquet.row_group_size",
        ByteSizeValue.ofMb(128),
        Property.IndexScope,
        Property.Dynamic
    );

    /** Whether the engine merges small uploaded files into larger ones in the background. */
    public static final Setting<Boolean> COMPACTION_ENABLED = Setting.boolSetting(
        "index.parquet.compaction.enabled",
        true,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * The size that a compaction aims for. A file below half of it is small and is merged with its neighbours, a file at or above half
     * of it is left alone, and no merged file is larger than {@code index.parquet.max_file_size}.
     */
    public static final Setting<ByteSizeValue> COMPACTION_TARGET_FILE_SIZE = Setting.byteSizeSetting(
        "index.parquet.compaction.target_file_size",
        ByteSizeValue.ofGb(1),
        Property.IndexScope,
        Property.Dynamic
    );

    /** The least number of adjacent small files that a compaction merges; with fewer the merge is not worth its traffic. */
    public static final Setting<Integer> COMPACTION_MIN_FILES = Setting.intSetting(
        "index.parquet.compaction.min_files",
        2,
        2,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * How long a file that a compaction replaced stays in the repository, which is the time that a reader of an older snapshot of the
     * table has to finish. Snapshots that are older than this are expired.
     */
    public static final Setting<TimeValue> COMPACTION_DELETE_DELAY = Setting.timeSetting(
        "index.parquet.compaction.delete_delay",
        TimeValue.timeValueHours(1),
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * How often a shard looks for files to compact when no flush asks for it, for example after a restart, or when a compaction was
     * dropped. Not dynamic: the engine schedules the timer when it starts.
     */
    public static final Setting<TimeValue> COMPACTION_INTERVAL = Setting.timeSetting(
        "index.parquet.compaction.interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueSeconds(1),
        Property.IndexScope
    );

    public static List<Setting<?>> settings() {
        return List.of(
            INDEX_FORMAT,
            REPOSITORY,
            BASE_PATH,
            DURABILITY,
            CATALOG_ENABLED,
            CATALOG_NAMESPACE,
            COMPRESSION,
            LOCAL_COMPRESSION,
            FLUSH_THRESHOLD_SIZE,
            FLUSH_THRESHOLD_AGE,
            MAX_LOCAL_SIZE,
            MAX_FILE_SIZE,
            FLUSH_MERGE_PARALLELISM,
            ROW_GROUP_SIZE,
            COMPACTION_ENABLED,
            COMPACTION_TARGET_FILE_SIZE,
            COMPACTION_MIN_FILES,
            COMPACTION_DELETE_DELAY,
            COMPACTION_INTERVAL
        );
    }

    private ParquetFormatSettings() {}
}
