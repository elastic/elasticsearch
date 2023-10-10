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

public class GetTopNFunctionsResponse extends ActionResponse implements ChunkedToXContentObject {
    private final int size;
    private final double samplingRate;

    public GetTopNFunctionsResponse(StreamInput in) throws IOException {
        this.size = in.readInt();
        this.samplingRate = in.readDouble();
    }

    public GetTopNFunctionsResponse(int size, double samplingRate) {
        this.size = size;
        this.samplingRate = samplingRate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(this.size);
        out.writeDouble(this.samplingRate);
    }

    public int getSize() {
        return size;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((b, p) -> b.field("Size", size)),
            Iterators.single((b, p) -> b.field("SamplingRate", samplingRate)),
            ChunkedToXContentHelper.endObject()
        );
    }
}
