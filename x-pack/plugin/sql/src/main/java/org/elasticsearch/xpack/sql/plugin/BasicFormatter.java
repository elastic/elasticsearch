/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter;

import java.io.IOException;
import java.util.List;

/**
 * Formats {@link SqlQueryResponse} for the CLI and for the TEXT format. {@linkplain Writeable} so
 * that its state can be saved between pages of results.
 */
public class BasicFormatter extends SimpleFormatter implements Writeable {
    /**
     * Create a new {@linkplain BasicFormatter} for formatting responses similar
     * to the provided columns and rows.
     */
    public BasicFormatter(List<ColumnInfo> columns, List<List<Object>> rows, FormatOption formatOption) {
        super(columns, rows, formatOption);
    }

    public BasicFormatter(StreamInput in) throws IOException {
        super(in.readIntArray(), in.readEnum(FormatOption.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeIntArray(width);
        out.writeEnum(formatOption);
    }

    @Override
    public int estimateSize(int rows) {
        return super.estimateSize(rows);
    }
}
