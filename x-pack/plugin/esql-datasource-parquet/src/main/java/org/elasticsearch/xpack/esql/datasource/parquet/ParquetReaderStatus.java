/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Typed {@link FormatReaderStatus} for the Parquet reader. The per-column counters ride as a typed
 * {@code Map<String, PerColumnStatus>} — {@link PerColumnStatus} is itself {@code Writeable} so it
 * crosses the wire directly, replacing the {@code Map}-flattening that the previous generic-map
 * carrier required.
 *
 * @param predicateColumns columns referenced by pushed-down predicates, sorted for determinism
 * @param columns          per-column read counters keyed by column path
 */
public record ParquetReaderStatus(
    long rowsEmitted,
    boolean predicatePushdownUsed,
    long footerReadNanos,
    long footerSizeBytes,
    long footerCacheHits,
    long footerCacheMisses,
    long rowGroupsInFile,
    long rowGroupsTotal,
    long rowGroupsKept,
    boolean pageIndexUsed,
    long rowsInKeptRowGroups,
    long rowsAfterPageIndex,
    boolean lateMaterializationEnabled,
    boolean lateMaterializationUsed,
    List<String> predicateColumns,
    long readNanos,
    Map<String, PerColumnStatus> columns
) implements FormatReaderStatus {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "parquet",
        ParquetReaderStatus::new
    );

    public ParquetReaderStatus(StreamInput in) throws IOException {
        this(
            in.readVLong(),                  // rowsEmitted
            in.readBoolean(),                // predicatePushdownUsed
            in.readVLong(),                  // footerReadNanos
            in.readVLong(),                  // footerSizeBytes
            in.readVLong(),                  // footerCacheHits
            in.readVLong(),                  // footerCacheMisses
            in.readVLong(),                  // rowGroupsInFile
            in.readVLong(),                  // rowGroupsTotal
            in.readVLong(),                  // rowGroupsKept
            in.readBoolean(),                // pageIndexUsed
            in.readVLong(),                  // rowsInKeptRowGroups
            in.readVLong(),                  // rowsAfterPageIndex
            in.readBoolean(),                // lateMaterializationEnabled
            in.readBoolean(),                // lateMaterializationUsed
            in.readStringCollectionAsList(), // predicateColumns
            in.readVLong(),                  // readNanos
            in.readMap(PerColumnStatus::new) // columns
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowsEmitted);
        out.writeBoolean(predicatePushdownUsed);
        out.writeVLong(footerReadNanos);
        out.writeVLong(footerSizeBytes);
        out.writeVLong(footerCacheHits);
        out.writeVLong(footerCacheMisses);
        out.writeVLong(rowGroupsInFile);
        out.writeVLong(rowGroupsTotal);
        out.writeVLong(rowGroupsKept);
        out.writeBoolean(pageIndexUsed);
        out.writeVLong(rowsInKeptRowGroups);
        out.writeVLong(rowsAfterPageIndex);
        out.writeBoolean(lateMaterializationEnabled);
        out.writeBoolean(lateMaterializationUsed);
        out.writeStringCollection(predicateColumns);
        out.writeVLong(readNanos);
        out.writeMap(columns, StreamOutput::writeWriteable);
    }

    @Override
    public String format() {
        return "parquet";
    }

    @Override
    public long rowsEmitted() {
        return rowsEmitted;
    }

    @Override
    public long readNanos() {
        return readNanos;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("format", format());
        builder.field("rows_emitted", rowsEmitted);
        builder.field("predicate_pushdown_used", predicatePushdownUsed);
        builder.field("footer_read_nanos", footerReadNanos);
        builder.field("footer_size_bytes", footerSizeBytes);
        builder.field("footer_cache_hits", footerCacheHits);
        builder.field("footer_cache_misses", footerCacheMisses);
        builder.field("row_groups_in_file", rowGroupsInFile);
        builder.field("row_groups_total", rowGroupsTotal);
        builder.field("row_groups_kept", rowGroupsKept);
        builder.field("page_index_used", pageIndexUsed);
        builder.field("rows_in_kept_row_groups", rowsInKeptRowGroups);
        builder.field("rows_after_page_index", rowsAfterPageIndex);
        builder.field("late_materialization_enabled", lateMaterializationEnabled);
        builder.field("late_materialization_used", lateMaterializationUsed);
        builder.field("predicate_columns", predicateColumns);
        builder.field("read_nanos", readNanos);
        if (columns.isEmpty() == false) {
            builder.startObject("columns");
            // TreeMap for deterministic column ordering; readMap yields an unordered map on the wire.
            for (Map.Entry<String, PerColumnStatus> e : new TreeMap<>(columns).entrySet()) {
                builder.field(e.getKey());
                e.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }
}
