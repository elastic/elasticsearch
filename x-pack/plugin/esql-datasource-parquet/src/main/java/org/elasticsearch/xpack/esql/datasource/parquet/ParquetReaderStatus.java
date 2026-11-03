/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.ProfileSchema;
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
    Map<String, PerColumnStatus> columns
) implements FormatReaderStatus {

    private static final TransportVersion ESQL_READ_CPU_NANOS = TransportVersion.fromName("esql_read_cpu_nanos");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "parquet",
        ParquetReaderStatus::new
    );
    public static final ProfileSchema<ParquetReaderStatus> PROFILE_SCHEMA = ProfileSchema.of(
        ParquetReaderStatus.class,
        (builder, status, params) -> {
            builder.field("format", status.format());
            builder.field("rows_emitted", status.rowsEmitted);
            builder.field("predicate_pushdown_used", status.predicatePushdownUsed);
            builder.field("footer_read_nanos", status.footerReadNanos);
            builder.field("footer_size_bytes", status.footerSizeBytes);
            builder.field("footer_cache_hits", status.footerCacheHits);
            builder.field("footer_cache_misses", status.footerCacheMisses);
            builder.field("row_groups_in_file", status.rowGroupsInFile);
            builder.field("row_groups_total", status.rowGroupsTotal);
            builder.field("row_groups_kept", status.rowGroupsKept);
            builder.field("page_index_used", status.pageIndexUsed);
            builder.field("rows_in_kept_row_groups", status.rowsInKeptRowGroups);
            builder.field("rows_after_page_index", status.rowsAfterPageIndex);
            builder.field("late_materialization_enabled", status.lateMaterializationEnabled);
            builder.field("late_materialization_used", status.lateMaterializationUsed);
            builder.field("predicate_columns", status.predicateColumns);
            if (status.columns.isEmpty() == false) {
                builder.startObject("columns");
                // TreeMap for deterministic column ordering; readMap yields an unordered map on the wire.
                for (Map.Entry<String, PerColumnStatus> entry : new TreeMap<>(status.columns).entrySet()) {
                    builder.field(entry.getKey());
                    PerColumnStatus.PROFILE_SCHEMA.write(builder, entry.getValue(), params);
                }
                builder.endObject();
            }
            return builder;
        }
    )
        .required("format", ProfileSchema.constant("parquet"))
        .required("rows_emitted", ProfileSchema.integer())
        .required("predicate_pushdown_used", ProfileSchema.bool())
        .required("footer_read_nanos", ProfileSchema.integer())
        .required("footer_size_bytes", ProfileSchema.integer())
        .required("footer_cache_hits", ProfileSchema.integer())
        .required("footer_cache_misses", ProfileSchema.integer())
        .required("row_groups_in_file", ProfileSchema.integer())
        .required("row_groups_total", ProfileSchema.integer())
        .required("row_groups_kept", ProfileSchema.integer())
        .required("page_index_used", ProfileSchema.bool())
        .required("rows_in_kept_row_groups", ProfileSchema.integer())
        .required("rows_after_page_index", ProfileSchema.integer())
        .required("late_materialization_enabled", ProfileSchema.bool())
        .required("late_materialization_used", ProfileSchema.bool())
        .required("predicate_columns", ProfileSchema.array(ProfileSchema.string()))
        .optional("columns", ProfileSchema.dynamicObject(PerColumnStatus.PROFILE_SCHEMA.schema()))
        .build();

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
            readColumnsSkippingNanos(in)     // columns (skips removed readNanos/readCpuNanos fields)
        );
    }

    // readNanos and readCpuNanos were removed from the record but must still be consumed from the wire.
    // They appear between predicateColumns and columns, so we read and discard them here before reading columns.
    private static Map<String, PerColumnStatus> readColumnsSkippingNanos(StreamInput in) throws IOException {
        in.readVLong(); // readNanos: removed field, preserved for wire compatibility
        if (in.getTransportVersion().supports(ESQL_READ_CPU_NANOS)) {
            in.readVLong(); // readCpuNanos: removed field, preserved for wire compatibility
        }
        return in.readMap(PerColumnStatus::new);
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
        out.writeVLong(0L); // readNanos: removed field, preserved for wire compatibility
        if (out.getTransportVersion().supports(ESQL_READ_CPU_NANOS)) {
            out.writeVLong(0L); // readCpuNanos: removed field, preserved for wire compatibility
        }
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
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return PROFILE_SCHEMA.write(builder, this, params);
    }
}
