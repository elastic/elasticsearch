/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
    long readNanos
) implements FormatReaderStatus {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "orc",
        OrcReaderStatus::new
    );

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
            in.readVLong()
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
    }

    @Override
    public String format() {
        return "orc";
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("format", format());
        builder.field("rows_emitted", rowsEmitted);
        builder.field("footer_read_nanos", footerReadNanos);
        builder.field("footer_size_bytes", footerSizeBytes);
        builder.field("footer_cache_hits", footerCacheHits);
        builder.field("footer_cache_misses", footerCacheMisses);
        builder.field("stripes_in_file", stripesInFile);
        builder.field("stripes_total", stripesTotal);
        builder.field("predicate_pushdown_used", predicatePushdownUsed);
        builder.field("predicate_columns", predicateColumns);
        builder.field("columns_projected", columnsProjected);
        builder.field("columns_total", columnsTotal);
        builder.field("read_nanos", readNanos);
        return builder;
    }
}
