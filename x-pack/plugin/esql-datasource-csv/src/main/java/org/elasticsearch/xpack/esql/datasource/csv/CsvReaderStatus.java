/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderStatus;

import java.io.IOException;

/**
 * Typed {@link FormatReaderStatus} for the delimited-text reader. One implementation serves both
 * {@code csv} and {@code tsv}; {@link #format} carries which one produced the snapshot — the
 * format (csv/tsv), not the quoting {@code mode}.
 */
public record CsvReaderStatus(String format, long rowsEmitted, long parseErrors, boolean headerDetected, long readNanos)
    implements
        FormatReaderStatus {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        FormatReaderStatus.class,
        "csv",
        CsvReaderStatus::new
    );

    public CsvReaderStatus(StreamInput in) throws IOException {
        this(in.readString(), in.readVLong(), in.readVLong(), in.readBoolean(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(format);
        out.writeVLong(rowsEmitted);
        out.writeVLong(parseErrors);
        out.writeBoolean(headerDetected);
        out.writeVLong(readNanos);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("format", format);
        builder.field("rows_emitted", rowsEmitted);
        builder.field("parse_errors", parseErrors);
        builder.field("header_detected", headerDetected);
        builder.field("read_nanos", readNanos);
        return builder;
    }
}
