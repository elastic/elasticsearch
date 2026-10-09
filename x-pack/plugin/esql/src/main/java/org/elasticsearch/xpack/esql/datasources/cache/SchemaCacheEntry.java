/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.HeapEstimates;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.WidenedColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Cache entry for schema inference results. Stores raw schema data (names, types,
 * nullabilities) instead of Attribute objects to avoid NameId sharing across queries.
 * Each call to {@link #toAttributes()} reconstructs fresh ReferenceAttribute instances
 * with fresh NameIds, ensuring safe concurrent use.
 * <p>
 * A class rather than a record so that {@link #estimatedBytes()} can be computed once. The shared
 * {@code Cache} runs its weigher TWICE on every hit that is not already at the LRU head:
 * {@code Cache.promote} sends an existing entry through {@code relinkAtHead}, whose {@code unlink}
 * subtracts {@code weigher.applyAsLong} from the running weight and whose {@code linkAtHead} adds it
 * back. The weight here is not a constant - it walks every column name, every warning, and both
 * metadata maps including the nested per-stripe maps - so recomputing it was the dominant cost of a warm
 * schema hit. The entry is immutable, so one computation in the constructor is exact; the enrichment
 * helper {@link #withSafeMetadata} builds a new entry and recomputes there.
 */
public final class SchemaCacheEntry {

    private final String[] columnNames;
    private final DataType[] columnTypes;
    private final Nullability[] columnNullabilities;
    private final boolean[] columnSynthetics;
    private final String sourceType;
    private final String location;
    private final Map<String, Object> safeMetadata;
    private final Map<String, Object> connectorConfig;
    private final List<String> warnings;
    private final List<WidenedColumn> widenedColumns;
    private final long estimatedBytes;

    public SchemaCacheEntry(
        String[] columnNames,
        DataType[] columnTypes,
        Nullability[] columnNullabilities,
        boolean[] columnSynthetics,
        String sourceType,
        String location,
        Map<String, Object> safeMetadata,
        Map<String, Object> connectorConfig,
        List<String> warnings,
        List<WidenedColumn> widenedColumns
    ) {
        if (columnNames.length != columnTypes.length
            || columnNames.length != columnNullabilities.length
            || columnNames.length != columnSynthetics.length) {
            throw new IllegalArgumentException("All column arrays must have the same length");
        }
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnNullabilities = columnNullabilities;
        this.columnSynthetics = columnSynthetics;
        this.sourceType = sourceType;
        this.location = location;
        this.safeMetadata = safeMetadata != null ? Map.copyOf(safeMetadata) : Map.of();
        this.connectorConfig = connectorConfig != null ? Map.copyOf(connectorConfig) : Map.of();
        this.warnings = warnings != null ? List.copyOf(warnings) : List.of();
        this.widenedColumns = widenedColumns != null ? List.copyOf(widenedColumns) : List.of();
        this.estimatedBytes = computeEstimatedBytes();
    }

    public String[] columnNames() {
        return columnNames;
    }

    public DataType[] columnTypes() {
        return columnTypes;
    }

    public Nullability[] columnNullabilities() {
        return columnNullabilities;
    }

    public boolean[] columnSynthetics() {
        return columnSynthetics;
    }

    public String sourceType() {
        return sourceType;
    }

    public String location() {
        return location;
    }

    public Map<String, Object> safeMetadata() {
        return safeMetadata;
    }

    public Map<String, Object> connectorConfig() {
        return connectorConfig;
    }

    public List<String> warnings() {
        return warnings;
    }

    /** Columns the reader widened within one file; cached for the same reason the schema is. */
    public List<WidenedColumn> widenedColumns() {
        return widenedColumns;
    }

    /**
     * Component-wise, which compares the four arrays by reference and not by content - the semantics a
     * component-wise {@code Objects.equals} gives. Two entries holding equal column names in different arrays
     * are therefore unequal.
     * <p>
     * Nothing in production or test compares two entries. This is written out so that the comparison is a
     * decision on the page rather than a property of a declaration form.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof SchemaCacheEntry other) {
            return columnNames == other.columnNames
                && columnTypes == other.columnTypes
                && columnNullabilities == other.columnNullabilities
                && columnSynthetics == other.columnSynthetics
                && Objects.equals(sourceType, other.sourceType)
                && Objects.equals(location, other.location)
                && Objects.equals(safeMetadata, other.safeMetadata)
                && Objects.equals(connectorConfig, other.connectorConfig)
                && Objects.equals(warnings, other.warnings)
                && Objects.equals(widenedColumns, other.widenedColumns);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            System.identityHashCode(columnNames),
            System.identityHashCode(columnTypes),
            System.identityHashCode(columnNullabilities),
            System.identityHashCode(columnSynthetics),
            sourceType,
            location,
            safeMetadata,
            connectorConfig,
            warnings,
            widenedColumns
        );
    }

    /**
     * An identical entry whose {@code safeMetadata} is replaced with {@code metadata} — the schema-cache
     * enrichment helper: entries are immutable, so a stats commit copies the metadata, mutates the copy,
     * and swaps the whole entry.
     */
    public SchemaCacheEntry withSafeMetadata(Map<String, Object> metadata) {
        return new SchemaCacheEntry(
            columnNames,
            columnTypes,
            columnNullabilities,
            columnSynthetics,
            sourceType,
            location,
            metadata,
            connectorConfig,
            warnings,
            widenedColumns
        );
    }

    public static SchemaCacheEntry from(
        List<Attribute> schema,
        String sourceType,
        String location,
        Map<String, Object> metadata,
        Map<String, Object> connectorConfig
    ) {
        return from(schema, sourceType, location, metadata, connectorConfig, List.of(), List.of());
    }

    /**
     * @param warnings see {@link SourceMetadata#warnings()}; cached so a warm resolve replays them like a cold one.
     * @param widenedColumns see {@link SourceMetadata#widenedColumns()}; cached for the same reason.
     */
    public static SchemaCacheEntry from(
        List<Attribute> schema,
        String sourceType,
        String location,
        Map<String, Object> metadata,
        Map<String, Object> connectorConfig,
        List<String> warnings,
        List<WidenedColumn> widenedColumns
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
            warnings,
            widenedColumns
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

    /** Flattens a {@link SourceMetadata}'s stats into its metadata map. Replaces the
     *  inlined flatten-and-build at the cache-loader call sites. */
    public static SchemaCacheEntry from(SourceMetadata meta) {
        Map<String, Object> enrichedMeta = meta.statistics()
            .map(stats -> SourceStatisticsSerializer.embedStatistics(meta.sourceMetadata(), stats))
            .orElse(meta.sourceMetadata());
        return from(meta.schema(), meta.sourceType(), meta.location(), enrichedMeta, meta.config(), meta.warnings(), meta.widenedColumns());
    }

    /** The weight computed once at construction; see the class javadoc for why this is not computed per call. */
    public long estimatedBytes() {
        return estimatedBytes;
    }

    private long computeEstimatedBytes() {
        // object header + reference fields
        long bytes = 64;
        for (String name : columnNames) {
            bytes += estimatedStringBytes(name);
        }
        // enum references stored as pointers
        bytes += columnTypes.length * (long) Long.BYTES;
        bytes += columnNullabilities.length * (long) Long.BYTES;
        bytes += columnSynthetics.length;
        bytes += estimatedStringBytes(sourceType);
        bytes += estimatedStringBytes(location);
        for (String warning : warnings) {
            bytes += estimatedStringBytes(warning);
        }
        for (WidenedColumn widened : widenedColumns) {
            bytes += estimatedStringBytes(widened.columnName()) + estimatedStringBytes(widened.value()) + 48;
        }
        // ~100B per map entry (key String + value Object) plus the payload of variable-width values
        // (keyword/text extrema as String or BytesRef). Nested maps (per-stripe stats under
        // _stats.stripe.<k>) weigh their inner entries the same way so a many-striped file doesn't
        // under-count against the cache budget.
        bytes += HeapEstimates.mapBytes(safeMetadata);
        bytes += HeapEstimates.mapBytes(connectorConfig);
        return bytes;
    }

    static long estimatedStringBytes(@Nullable String s) {
        return HeapEstimates.stringBytes(s);
    }

}
