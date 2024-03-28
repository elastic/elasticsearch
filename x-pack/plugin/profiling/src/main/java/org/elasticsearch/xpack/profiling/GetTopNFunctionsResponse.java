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
    private final double samplingRate;
    private final long totalSamples;
    private final long selfCPU;
    private final long totalCPU;
    private final List<TopNFunction> topNFunctions;

    public GetTopNFunctionsResponse(double samplingRate, long totalSamples, long selfCPU, long totalCPU, List<TopNFunction> topNFunctions) {
        this.samplingRate = samplingRate;
        this.totalSamples = totalSamples;
        this.selfCPU = selfCPU;
        this.totalCPU = totalCPU;
        this.topNFunctions = topNFunctions;
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public long getTotalSamples() {
        return totalSamples;
    }

    public long getSelfCPU() {
        return selfCPU;
    }

    public long getTotalCPU() {
        return totalCPU;
    }

    public List<TopNFunction> getTopN() {
        return topNFunctions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("sampling_rate", samplingRate);
        builder.field("total_count", totalSamples);
        builder.field("self_cpu", selfCPU);
        builder.field("total_cpu", totalCPU);
        builder.xContentList("topn", topNFunctions);
        builder.endObject();
        return builder;
    }
}
