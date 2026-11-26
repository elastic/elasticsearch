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

    public static final NestedColumns NONE = new NestedColumns(Map.of());

    public NestedColumns {
        columns = Map.copyOf(columns);
    }

    /**
     * One nested column.
     *
     * @param path     dotted path, as the query names it
     * @param leaves   each element's fields, by name relative to {@code path}, with their types, sorted by name
     * @param conflict why the column cannot be read as nested, or {@code null} when it can
     */
    public record NestedColumn(String path, Map<String, DataType> leaves, @Nullable String conflict) {
        public NestedColumn {
            leaves = new TreeMap<>(leaves);
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

    /** The nested columns recorded in {@code sourceMetadata}, or {@link #NONE}. */
    public static NestedColumns read(@Nullable Map<String, Object> sourceMetadata) {
        Object raw = sourceMetadata == null ? null : sourceMetadata.get(KEY);
        if (raw instanceof Map<?, ?> == false) {
            return NONE;
        }
        Map<?, ?> byPath = (Map<?, ?>) raw;
        Map<String, NestedColumn> columns = new HashMap<>();
        for (Map.Entry<?, ?> entry : byPath.entrySet()) {
            String path = (String) entry.getKey();
            Map<?, ?> encoded = (Map<?, ?>) entry.getValue();
            Map<String, DataType> leaves = new HashMap<>();
            for (Map.Entry<?, ?> leaf : ((Map<?, ?>) encoded.get(LEAVES)).entrySet()) {
                leaves.put((String) leaf.getKey(), DataType.fromTypeName((String) leaf.getValue()));
            }
            columns.put(path, new NestedColumn(path, leaves, (String) encoded.get(CONFLICT)));
        }
        return new NestedColumns(columns);
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
            byPath.put(column.path(), encoded);
        }
        copy.put(KEY, byPath);
        return copy;
    }
}
