/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The columns of a source that are nested: a list of structures, whose elements are a child relation of the source's rows,
 * as a nested field's objects are child documents of an index's. The schema keeps such a column as one
 * {@link DataType#UNSUPPORTED} attribute; this says that it is nested and what each element holds, and is what the analyzer
 * builds the nested scope from. It is found when the schema is discovered and travels in
 * {@link SourceMetadata#sourceMetadata()} under {@link #KEY}, which only this class reads or writes.
 *
 * @param columns by dotted path
 */
public record NestedColumns(Map<String, NestedColumn> columns) {

    /** The key in {@link SourceMetadata#sourceMetadata()}. Outside the {@code _stats.} prefix, which multi-file merges strip. */
    public static final String KEY = "_nested.columns";

    private static final String LEAVES = "leaves";
    private static final String CONFLICT = "conflict";
    private static final String UNREADABLE = "unreadable";

    public static final NestedColumns NONE = new NestedColumns(Map.of());

    public NestedColumns {
        columns = Map.copyOf(columns);
    }

    /**
     * One nested column. Either reason is raised at a {@code NESTED_EXPAND} of it, as its nested scope's conflict.
     *
     * @param path       dotted path, as the query names it
     * @param leaves     each element's fields, by name relative to {@code path}, with their types, sorted by name
     * @param conflict   why the dataset's files disagree about the column, or {@code null} when they agree
     * @param unreadable why the reader cannot read the column's shape one element at a time, or {@code null} when it can
     */
    public record NestedColumn(String path, Map<String, DataType> leaves, @Nullable String conflict, @Nullable String unreadable) {
        public NestedColumn {
            leaves = new TreeMap<>(leaves);
        }

        /** A column with no reason it cannot be read, or one whose files disagree. */
        public NestedColumn(String path, Map<String, DataType> leaves, @Nullable String conflict) {
            this(path, leaves, conflict, null);
        }

        /** Whether a query can read the column one element at a time. */
        public boolean readable() {
            return conflict == null && unreadable == null;
        }
    }

    public static NestedColumns of(List<NestedColumn> columns) {
        Map<String, NestedColumn> byPath = new HashMap<>();
        for (NestedColumn column : columns) {
            byPath.put(column.path(), column);
        }
        return new NestedColumns(byPath);
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    @Nullable
    public NestedColumn get(String path) {
        return columns.get(path);
    }

    /**
     * The nested columns recorded in {@code sourceMetadata}, or {@link #NONE} when it records none.
     *
     * @throws IllegalArgumentException when the record is not one {@link #writeTo} writes
     */
    public static NestedColumns read(@Nullable Map<String, Object> sourceMetadata) {
        Object raw = sourceMetadata == null ? null : sourceMetadata.get(KEY);
        if (raw == null) {
            return NONE;
        }
        Map<String, NestedColumn> columns = new HashMap<>();
        for (Map.Entry<?, ?> entry : asMap(raw, KEY).entrySet()) {
            String path = asString(entry.getKey(), KEY);
            Map<?, ?> encoded = asMap(entry.getValue(), path);
            Map<String, DataType> leaves = new HashMap<>();
            for (Map.Entry<?, ?> leaf : asMap(encoded.get(LEAVES), path).entrySet()) {
                String typeName = asString(leaf.getValue(), path);
                DataType type = DataType.fromTypeName(typeName);
                if (type == null) {
                    throw malformed(path);
                }
                leaves.put(asString(leaf.getKey(), path), type);
            }
            columns.put(
                path,
                new NestedColumn(path, leaves, optionalString(encoded.get(CONFLICT), path), optionalString(encoded.get(UNREADABLE), path))
            );
        }
        return new NestedColumns(columns);
    }

    private static Map<?, ?> asMap(@Nullable Object value, String where) {
        if (value instanceof Map<?, ?> map) {
            return map;
        }
        throw malformed(where);
    }

    private static String asString(@Nullable Object value, String where) {
        if (value instanceof String string) {
            return string;
        }
        throw malformed(where);
    }

    @Nullable
    private static String optionalString(@Nullable Object value, String where) {
        return value == null ? null : asString(value, where);
    }

    private static IllegalArgumentException malformed(String where) {
        return new IllegalArgumentException("malformed nested column description at [" + where + "]");
    }

    /**
     * An estimate of the heap the record in {@code sourceMetadata} holds, deep: every path, field name, type name and reason,
     * which a two-level map estimate does not reach.
     */
    public static long estimatedBytes(@Nullable Map<String, Object> sourceMetadata) {
        return sourceMetadata == null ? 0L : HeapEstimates.deepBytes(sourceMetadata.get(KEY));
    }

    /**
     * A copy of {@code sourceMetadata} that records these columns, in a form the generic stream writer carries, or that records
     * none when this is empty.
     */
    public Map<String, Object> writeTo(@Nullable Map<String, Object> sourceMetadata) {
        Map<String, Object> copy = sourceMetadata == null ? new HashMap<>() : new HashMap<>(sourceMetadata);
        if (columns.isEmpty()) {
            copy.remove(KEY);
            return copy;
        }
        Map<String, Object> byPath = new HashMap<>();
        for (NestedColumn column : columns.values()) {
            Map<String, Object> leaves = new HashMap<>();
            column.leaves().forEach((name, type) -> leaves.put(name, type.typeName()));
            Map<String, Object> encoded = new HashMap<>();
            encoded.put(LEAVES, leaves);
            if (column.conflict() != null) {
                encoded.put(CONFLICT, column.conflict());
            }
            if (column.unreadable() != null) {
                encoded.put(UNREADABLE, column.unreadable());
            }
            byPath.put(column.path(), encoded);
        }
        copy.put(KEY, byPath);
        return copy;
    }
}
