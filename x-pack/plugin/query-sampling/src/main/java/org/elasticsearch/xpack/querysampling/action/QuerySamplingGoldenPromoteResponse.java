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

import java.io.IOException;

/**
 * What a promotion did. Like the request it stays on the node that ran it.
 */
public final class QuerySamplingGoldenPromoteResponse extends ActionResponse implements ToXContentObject {

    private final Long version;
    private final int promoted;
    private final int failed;

    /**
     * @param version  the version of the dataset that was made, {@code null} if there was nothing to promote
     * @param promoted queries that are in the version
     * @param failed   queries that could not be written to it
     */
    public QuerySamplingGoldenPromoteResponse(@Nullable Long version, int promoted, int failed) {
        this.version = version;
        this.promoted = promoted;
        this.failed = failed;
    }

    @Nullable
    public Long version() {
        return version;
    }

    public int promoted() {
        return promoted;
    }

    public int failed() {
        return failed;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("version", version).field("promoted", promoted).field("failed", failed).endObject();
    }
}
