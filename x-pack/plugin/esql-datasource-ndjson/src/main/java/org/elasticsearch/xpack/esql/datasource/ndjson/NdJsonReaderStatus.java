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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;

/**
 * Typed {@link FormatReaderStatus} for the NDJSON reader.
 */
public record NdJsonReaderStatus(long rowsEmitted, long parseErrors, long readNanos) implements FormatReaderStatus {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "ndjson",
        NdJsonReaderStatus::new
    );

    public NdJsonReaderStatus(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(rowsEmitted);
        out.writeVLong(parseErrors);
        out.writeVLong(readNanos);
    }

    @Override
    public String format() {
        return "ndjson";
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
        return builder;
    }
}
