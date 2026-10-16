/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.querysampling.storage.GoldenUpdater;

import java.io.IOException;

public final class QuerySamplingGoldenUpdateResponse extends ActionResponse implements ToXContentObject {

    private final GoldenUpdater.Result result;

    public QuerySamplingGoldenUpdateResponse(GoldenUpdater.Result result) {
        this.result = result;
    }

    @Nullable
    private static Long orNull(long version) {
        return version == 0 ? null : version;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("source_version", orNull(result.sourceVersion()))
            .field("version", orNull(result.version()))
            .field("copied", result.copied())
            .field("recomputed", result.recomputed())
            .field("added", result.added())
            .field("dropped", result.dropped())
            .field("failed", result.failed())
            .endObject();
    }
}
