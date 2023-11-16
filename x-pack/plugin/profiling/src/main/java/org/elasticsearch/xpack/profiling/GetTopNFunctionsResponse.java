/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class GetTopNFunctionsResponse extends ActionResponse implements ChunkedToXContentObject {
    private final double samplingRate;
    private final long totalCount;
    private final long selfCPU;
    private final long totalCPU;
    private final List<TopNFunction> topNFunctions;

    public GetTopNFunctionsResponse(StreamInput in) throws IOException {
        this.samplingRate = in.readDouble();
        this.totalCount = in.readLong();
        this.selfCPU = in.readLong();
        this.totalCPU = in.readLong();
        this.topNFunctions = in.readCollectionAsList(TopNFunction::new);
    }

    public GetTopNFunctionsResponse(double samplingRate, long totalCount, long selfCPU, long totalCPU, List<TopNFunction> topNFunctions) {
        this.samplingRate = samplingRate;
        this.totalCount = totalCount;
        this.selfCPU = selfCPU;
        this.totalCPU = totalCPU;
        this.topNFunctions = topNFunctions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(this.samplingRate);
        out.writeLong(this.totalCount);
        out.writeLong(this.selfCPU);
        out.writeLong(this.totalCPU);
        out.writeCollection(this.topNFunctions);
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public long getTotalCount() {
        return totalCount;
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
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((b, p) -> b.field("SamplingRate", samplingRate)),
            Iterators.single((b, p) -> b.field("TotalCount", totalCount)),
            Iterators.single((b, p) -> b.field("SelfCPU", selfCPU)),
            Iterators.single((b, p) -> b.field("TotalCPU", totalCPU)),
            ChunkedToXContentHelper.endObject()
        );
    }
}
