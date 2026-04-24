/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cache entry for schema inference results. Stores raw schema data (names, types,
 * nullabilities) instead of Attribute objects to avoid NameId sharing across queries.
 * Each call to {@link #toAttributes()} reconstructs fresh ReferenceAttribute instances
 * with fresh NameIds, ensuring safe concurrent use.
 */
public record SchemaCacheEntry(
    String[] columnNames,
    DataType[] columnTypes,
    Nullability[] columnNullabilities,
    boolean[] columnSynthetics,
    String sourceType,
    String location,
    Map<String, Object> safeMetadata,
    Map<String, Object> connectorConfig,
    long cachedAtMillis
) {
    public SchemaCacheEntry {
        if (columnNames.length != columnTypes.length
            || columnNames.length != columnNullabilities.length
            || columnNames.length != columnSynthetics.length) {
            throw new IllegalArgumentException("All column arrays must have the same length");
        }
        safeMetadata = safeMetadata != null ? Map.copyOf(safeMetadata) : Map.of();
        connectorConfig = connectorConfig != null ? Map.copyOf(connectorConfig) : Map.of();
    }

    public static SchemaCacheEntry from(
        List<Attribute> schema,
        String sourceType,
        String location,
        Map<String, Object> metadata,
        Map<String, Object> connectorConfig
    ) {
        int size = schema.size();
        String[] names = new String[size];
        DataType[] types = new DataType[size];
        Nullability[] nullabilities = new Nullability[size];
        boolean[] synthetics = new boolean[size];
        for (int i = 0; i < size; i++) {
            Attribute attr = schema.get(i);
            names[i] = attr.name();
            types[i] = attr.dataType();
            nullabilities[i] = attr.nullable();
            synthetics[i] = attr.synthetic();
        }
        return new SchemaCacheEntry(
            names,
            types,
            nullabilities,
            synthetics,
            sourceType,
            location,
            metadata,
            connectorConfig,
            System.currentTimeMillis()
        );
    }

    /** Reconstructs fresh Attributes with fresh NameIds -- safe for concurrent queries */
    public List<Attribute> toAttributes() {
        List<Attribute> result = new ArrayList<>(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            result.add(
                new ReferenceAttribute(
                    Source.EMPTY,
                    null,
                    columnNames[i],
                    columnTypes[i],
                    columnNullabilities[i],
                    null,
                    columnSynthetics[i]
                )
            );
        }
        return result;
    }

    public long estimatedBytes() {
        // object header + reference fields
        long bytes = 64;
        for (String name : columnNames) {
            // per-String: ~40B object overhead + char data
            bytes += 40 + (name != null ? name.length() * (long) Character.BYTES : 0);
        }
        // enum references stored as pointers
        bytes += columnTypes.length * (long) Long.BYTES;
        bytes += columnNullabilities.length * (long) Long.BYTES;
        bytes += columnSynthetics.length;
        bytes += sourceType != null ? sourceType.length() * (long) Character.BYTES : 0;
        bytes += location != null ? location.length() * (long) Character.BYTES : 0;
        // rough estimate: ~100B per metadata entry (key String + value Object)
        bytes += safeMetadata.size() * 100L;
        bytes += connectorConfig.size() * 100L;
        return bytes;
    }
}
