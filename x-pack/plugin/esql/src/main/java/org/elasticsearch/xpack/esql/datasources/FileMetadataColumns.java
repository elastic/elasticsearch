/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
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

    /**
     * {@link #PATH}, {@link #NAME}, and {@link #DIRECTORY}. Derived from the file URI at read time
     * rather than stored on the survivor map. {@link #SIZE} and {@link #MODIFIED} stay stored;
     * {@link #RECORD_REF} is composed per row.
     */
    public static final Set<String> LOCATION_NAMES = Set.of(PATH, NAME, DIRECTORY);

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
        putValues(map, path, length, lastModified, null, LOCATION_NAMES);
        return Collections.unmodifiableMap(map);
    }

    /**
     * Writes size, modified, and whichever location names {@code locationNames} contains.
     * Callers that already own a map skip the throwaway map {@link #extractValues} allocates.
     * An empty {@code locationNames} writes no path, name, or directory: discovery uses that when no
     * bound filter reads them. {@code directoryIntern}, when non-null, reuses one {@link BytesRef} per
     * distinct parent path for this call; full {@link #PATH} URIs are never interned. A null parent or a
     * null {@code lastModified} is stored as a null value.
     */
    static void putValues(
        Map<String, Object> dest,
        StoragePath path,
        long length,
        @Nullable Instant lastModified,
        @Nullable Map<String, BytesRef> directoryIntern,
        Set<String> locationNames
    ) {
        if (locationNames.contains(PATH)) {
            putLocationValue(dest, path, PATH, null);
        }
        if (locationNames.contains(NAME)) {
            putLocationValue(dest, path, NAME, null);
        }
        if (locationNames.contains(DIRECTORY)) {
            putLocationValue(dest, path, DIRECTORY, directoryIntern);
        }
        dest.put(SIZE, length);
        dest.put(MODIFIED, lastModified != null ? lastModified.toEpochMilli() : null);
    }

    /**
     * One location column derived from {@code path}. {@code directoryIntern}, when non-null, reuses one
     * directory {@link BytesRef} per parent. Path and name are never interned.
     */
    private static void putLocationValue(
        Map<String, Object> dest,
        StoragePath path,
        String name,
        @Nullable Map<String, BytesRef> directoryIntern
    ) {
        switch (name) {
            case PATH -> dest.put(PATH, new BytesRef(path.toString()));
            case NAME -> dest.put(NAME, new BytesRef(path.objectName()));
            case DIRECTORY -> dest.put(DIRECTORY, directoryValue(path, directoryIntern));
            default -> throw new IllegalArgumentException("unexpected location column [" + name + "]");
        }
    }

    private static Object directoryValue(StoragePath path, @Nullable Map<String, BytesRef> directoryIntern) {
        StoragePath parent = path.parentDirectory();
        if (parent == null) {
            return null;
        }
        String parentText = parent.toString();
        if (directoryIntern == null) {
            return new BytesRef(parentText);
        }
        BytesRef directory = directoryIntern.get(parentText);
        if (directory == null) {
            directory = new BytesRef(parentText);
            directoryIntern.put(parentText, directory);
        }
        return directory;
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

    /**
     * Fills any of {@link #PATH}, {@link #NAME}, and {@link #DIRECTORY} that {@code neededNames} asks for
     * and {@code map} does not already contain. Returns {@code map} itself when none of those names are
     * needed or every needed one is already present, including an explicit null. Otherwise returns a copy
     * with only the missing location keys filled from {@code path}. Does not write {@link #SIZE} or
     * {@link #MODIFIED}: a span split's length is not the file size, and a frozen survivor map is shared
     * across span siblings, so this must not mutate {@code map}.
     */
    public static Map<String, Object> overlayLocation(
        @Nullable Map<String, Object> map,
        StoragePath path,
        @Nullable Set<String> neededNames
    ) {
        boolean needPath = needsLocation(neededNames, PATH) && containsKey(map, PATH) == false;
        boolean needName = needsLocation(neededNames, NAME) && containsKey(map, NAME) == false;
        boolean needDirectory = needsLocation(neededNames, DIRECTORY) && containsKey(map, DIRECTORY) == false;
        if (needPath == false && needName == false && needDirectory == false) {
            return map;
        }
        LinkedHashMap<String, Object> copy = map == null ? new LinkedHashMap<>() : new LinkedHashMap<>(map);
        if (needPath) {
            putLocationValue(copy, path, PATH, null);
        }
        if (needName) {
            putLocationValue(copy, path, NAME, null);
        }
        if (needDirectory) {
            putLocationValue(copy, path, DIRECTORY, null);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static boolean needsLocation(@Nullable Set<String> neededNames, String name) {
        return neededNames != null && neededNames.contains(name);
    }

    private static boolean containsKey(@Nullable Map<String, Object> map, String key) {
        return map != null && map.containsKey(key);
    }
}
