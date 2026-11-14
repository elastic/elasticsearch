/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.ProfileSchema;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;
import java.util.List;

/**
 * Typed {@link FormatReaderStatus} for the ORC reader.
 *
 * @param predicateColumns columns referenced by pushed-down predicates, sorted for determinism
 */
public record OrcReaderStatus(
    long rowsEmitted,
    long footerReadNanos,
    long footerSizeBytes,
    long footerCacheHits,
    long footerCacheMisses,
    long stripesInFile,
    long stripesTotal,
    boolean predicatePushdownUsed,
    List<String> predicateColumns,
    long columnsProjected,
    long columnsTotal,
    long readNanos,
    long readCpuNanos
) implements FormatReaderStatus {

    private static final TransportVersion ESQL_READ_CPU_NANOS = TransportVersion.fromName("esql_read_cpu_nanos");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "orc",
        OrcReaderStatus::new
    );
    public static final ProfileSchema<OrcReaderStatus> PROFILE_SCHEMA = ProfileSchema.of(
        OrcReaderStatus.class,
        (builder, status, params) -> {
            builder.field("format", status.format());
            builder.field("rows_emitted", status.rowsEmitted);
            builder.field("footer_read_nanos", status.footerReadNanos);
            builder.field("footer_size_bytes", status.footerSizeBytes);
            builder.field("footer_cache_hits", status.footerCacheHits);
            builder.field("footer_cache_misses", status.footerCacheMisses);
            builder.field("stripes_in_file", status.stripesInFile);
            builder.field("stripes_total", status.stripesTotal);
            builder.field("predicate_pushdown_used", status.predicatePushdownUsed);
            builder.field("predicate_columns", status.predicateColumns);
            builder.field("columns_projected", status.columnsProjected);
            builder.field("columns_total", status.columnsTotal);
            return builder;
        }
    )
        .required("format", ProfileSchema.constant("orc"))
        .required("rows_emitted", ProfileSchema.integer())
        .required("footer_read_nanos", ProfileSchema.integer())
        .required("footer_size_bytes", ProfileSchema.integer())
        .required("footer_cache_hits", ProfileSchema.integer())
        .required("footer_cache_misses", ProfileSchema.integer())
        .required("stripes_in_file", ProfileSchema.integer())
        .required("stripes_total", ProfileSchema.integer())
        .required("predicate_pushdown_used", ProfileSchema.bool())
        .required("predicate_columns", ProfileSchema.array(ProfileSchema.string()))
        .required("columns_projected", ProfileSchema.integer())
        .required("columns_total", ProfileSchema.integer())
        .build();

    public OrcReaderStatus(StreamInput in) throws IOException {
        this(
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.readBoolean(),
            in.readStringCollectionAsList(),
            in.readVLong(),
            in.readVLong(),
            in.readVLong(),
            in.getTransportVersion().supports(ESQL_READ_CPU_NANOS) ? in.readVLong() : 0L
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowsEmitted);
        out.writeVLong(footerReadNanos);
        out.writeVLong(footerSizeBytes);
        out.writeVLong(footerCacheHits);
        out.writeVLong(footerCacheMisses);
        out.writeVLong(stripesInFile);
        out.writeVLong(stripesTotal);
        out.writeBoolean(predicatePushdownUsed);
        out.writeStringCollection(predicateColumns);
        out.writeVLong(columnsProjected);
        out.writeVLong(columnsTotal);
        out.writeVLong(readNanos);
        if (out.getTransportVersion().supports(ESQL_READ_CPU_NANOS)) {
            out.writeVLong(readCpuNanos);
        }
    }

    @Override
    public String format() {
        return "orc";
    }

    @Override
    public long readCpuNanos() {
        return readCpuNanos;
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
