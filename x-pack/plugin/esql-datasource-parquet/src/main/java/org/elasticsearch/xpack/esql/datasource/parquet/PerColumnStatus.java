/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Immutable per-column read summary held under the {@code columns} key of
 * {@link ParquetReaderStatus}. A record (rather than a bare string) so the per-column object can
 * grow back the byte/timing counters once the per-page decode loop is instrumented, without a
 * JSON shape change.
 *
 * @param materialization {@code "eager"} or {@code "late"} — which materialization path this
 *                        column went through during the read
 */
public record PerColumnStatus(String materialization) implements Writeable, ToXContentObject {

    public static final String MATERIALIZATION_EAGER = "eager";
    public static final String MATERIALIZATION_LATE = "late";

    public PerColumnStatus(StreamInput in) throws IOException {
        this(in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(materialization);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (materialization != null) {
            builder.field("materialization", materialization);
        }
        builder.endObject();
        return builder;
    }
}
