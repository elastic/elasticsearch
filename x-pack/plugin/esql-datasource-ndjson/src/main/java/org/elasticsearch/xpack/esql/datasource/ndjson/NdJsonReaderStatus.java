/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;

/**
 * Typed {@link FormatReaderStatus} for the NDJSON reader.
 */
public record NdJsonReaderStatus(long rowsEmitted, long parseErrors, long readNanos, long readCpuNanos) implements FormatReaderStatus {

    private static final TransportVersion ESQL_READ_CPU_NANOS = TransportVersion.fromName("esql_read_cpu_nanos");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "ndjson",
        NdJsonReaderStatus::new
    );

    public NdJsonReaderStatus(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(),
            in.getTransportVersion().supports(ESQL_READ_CPU_NANOS) ? in.readVLong() : 0L);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowsEmitted);
        out.writeVLong(parseErrors);
        out.writeVLong(readNanos);
        if (out.getTransportVersion().supports(ESQL_READ_CPU_NANOS)) {
            out.writeVLong(readCpuNanos);
        }
    }

    @Override
    public String format() {
        return "ndjson";
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
        builder.field("format", format());
        builder.field("rows_emitted", rowsEmitted);
        builder.field("parse_errors", parseErrors);
        builder.field("read_nanos", readNanos);
        builder.field("read_cpu_nanos", readCpuNanos);
        return builder;
    }
}
