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
import org.elasticsearch.xpack.querysampling.storage.GoldenStaleness;

import java.io.IOException;

/**
 * What a check of a version of the golden dataset found. Like the request it stays on the node that ran it.
 */
public final class QuerySamplingGoldenCheckResponse extends ActionResponse implements ToXContentObject {

    private final GoldenStaleness.Result result;

    public QuerySamplingGoldenCheckResponse(GoldenStaleness.Result result) {
        this.result = result;
    }

    public GoldenStaleness.Result result() {
        return result;
    }

    @Nullable
    private Long version() {
        return result.version() == 0 ? null : result.version();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("version", version())
            .field("checked", result.checked())
            .field("fresh", result.fresh())
            .field("stale", result.stale())
            .field("unknown", result.unknown())
            .field("failed", result.failed())
            .endObject();
    }
}
