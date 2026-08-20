/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry of well-known file metadata virtual columns for external data sources.
 * Uses dot-namespaced names under {@code _file.*} to avoid collisions with
 * Hive partition columns (which cannot contain dots).
 * Separate from {@code MetadataAttribute.ATTRIBUTES_MAP} which covers ES index metadata.
 */
public final class FileMetadataColumns {

    public static final String PATH = "_file.path";
    public static final String NAME = "_file.name";
    public static final String DIRECTORY = "_file.directory";
    public static final String SIZE = "_file.size";
    public static final String MODIFIED = "_file.modified";
    /**
     * Opaque, stable, per-record reference. Unlike the other {@code _file.*} columns (per-file
     * constants via {@link #extractValues}), this one varies per record and is sourced from the
     * reader's row-position channel
     * ({@link org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor#ROW_POSITION_COLUMN}).
     * Shape is format-defined and opaque to consumers — equality is the only defined relation,
     * independent of split layout. Deliberately excluded from {@link #extractValues}.
     */
    public static final String RECORD_REF = "_file.record_ref";

    public static final Map<String, DataType> COLUMNS;

    static {
        var map = new LinkedHashMap<String, DataType>();
        map.put(PATH, DataType.KEYWORD);
        map.put(NAME, DataType.KEYWORD);
        map.put(DIRECTORY, DataType.KEYWORD);
        map.put(SIZE, DataType.LONG);
        map.put(MODIFIED, DataType.DATETIME);
        map.put(RECORD_REF, DataType.LONG);
        COLUMNS = Collections.unmodifiableMap(map);
    }

    public static final Set<String> NAMES = COLUMNS.keySet();

    private FileMetadataColumns() {}

    public static boolean isFileMetadataColumn(String name) {
        return COLUMNS.containsKey(name);
    }

    /**
     * Object-typed entry point. Pass {@code lastModified == null} for SQL {@code NULL};
     * any non-null {@link Instant} (including {@link Instant#EPOCH}) is rendered as the
     * corresponding epoch-millis timestamp. Callers that hold a primitive epoch-millis with
     * {@code 0L} as the "unknown" sentinel (e.g. {@link FileList}) should normalise to
     * {@code null} before calling this method, or use {@link #extractValues(FileList, int)}.
     */
    public static Map<String, Object> extractValues(StoragePath path, long length, Instant lastModified) {
        var map = new LinkedHashMap<String, Object>(8);
        map.put(PATH, new BytesRef(path.toString()));
        map.put(NAME, new BytesRef(path.objectName()));
        StoragePath parent = path.parentDirectory();
        map.put(DIRECTORY, parent != null ? new BytesRef(parent.toString()) : null);
        map.put(SIZE, length);
        map.put(MODIFIED, lastModified != null ? lastModified.toEpochMilli() : null);
        return Collections.unmodifiableMap(map);
    }

    /**
     * Convenience overload for callers that already hold a {@link StorageEntry}. Note that
     * {@code StorageEntry} normalises a {@code null} {@code lastModified} to {@link Instant#EPOCH}
     * at construction time, so this overload cannot distinguish "modified at the epoch" from
     * "unknown mtime". For SQL-{@code NULL} semantics, use {@link #extractValues(FileList, int)}.
     */
    public static Map<String, Object> extractValues(StorageEntry entry) {
        return extractValues(entry.path(), entry.length(), entry.lastModified());
    }

    /**
     * Index-based accessor for {@link FileList}, which exposes file metadata as primitives.
     * A zero value for {@code lastModifiedMillis} is treated as "unknown" and yields a {@code null}
     * value for {@link #MODIFIED} so downstream layers can render it as SQL {@code NULL}. This is
     * the recommended overload for production paths.
     */
    public static Map<String, Object> extractValues(FileList fileList, int index) {
        long modifiedMillis = fileList.lastModifiedMillis(index);
        Instant modified = modifiedMillis == 0L ? null : Instant.ofEpochMilli(modifiedMillis);
        return extractValues(fileList.path(index), fileList.size(index), modified);
    }
}
