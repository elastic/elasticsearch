/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class GetTopNFunctionsResponse extends ActionResponse implements ToXContentObject {
    private final long selfCount;
    private final long totalCount;
    private final List<TopNFunction> topNFunctions;

    public GetTopNFunctionsResponse(long selfCount, long totalCount, List<TopNFunction> topNFunctions) {
        this.selfCount = selfCount;
        this.totalCount = totalCount;
        this.topNFunctions = topNFunctions;
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }

    public long getSelfCount() {
        return selfCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public List<TopNFunction> getTopN() {
        return topNFunctions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("self_count", selfCount);
        builder.field("total_count", totalCount);
        builder.xContentList("topn", topNFunctions);
        builder.endObject();
        return builder;
    }
}
