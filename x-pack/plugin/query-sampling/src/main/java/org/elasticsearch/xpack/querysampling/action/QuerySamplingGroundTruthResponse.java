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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * What a computation of ground truth did. Like the request it stays on the node that ran it.
 */
public final class QuerySamplingGroundTruthResponse extends ActionResponse implements ToXContentObject {

    private final int computed;
    private final int failed;

    /**
     * @param computed queries that got their ground truth
     * @param failed   queries whose ground truth could not be computed or stored, they stay pending
     */
    public QuerySamplingGroundTruthResponse(int computed, int failed) {
        this.computed = computed;
        this.failed = failed;
    }

    public int computed() {
        return computed;
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
        return builder.startObject().field("computed", computed).field("failed", failed).endObject();
    }
}
