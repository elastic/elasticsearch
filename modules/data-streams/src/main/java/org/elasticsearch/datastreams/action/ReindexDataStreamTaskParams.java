/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ReindexDataStreamTaskParams implements PersistentTaskParams {
    public static final String NAME = ReindexDataStreamTask.TASK_NAME;
    private final String sourceDataStream;

    public ReindexDataStreamTaskParams(String sourceDataStream) {
        this.sourceDataStream = sourceDataStream;
    }

    public ReindexDataStreamTaskParams(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.REINDEX_DATA_STREAMS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceDataStream);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("reindex_data_stream_task_params", "asdf");
        builder.endObject();
        return builder;
    }

    public String getSourceDataStream() {
        return sourceDataStream;
    }

    public static PersistentTaskParams fromXContent(XContentParser xContentParser) {
        throw new RuntimeException("TODO");
        // return new ReindexDataStreamTaskParams();
    }
}
