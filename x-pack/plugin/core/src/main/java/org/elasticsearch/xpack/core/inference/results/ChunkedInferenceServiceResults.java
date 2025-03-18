/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ChunkedInferenceServiceResults implements InferenceServiceResults {

    private final String NAME = "chunked_inference_service_result";
    private final List<ChunkedInference> chunks;

    public ChunkedInferenceServiceResults(List<ChunkedInference> chunks) {
        this.chunks = chunks;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return List.of();
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return List.of();
    }

    @Override
    public Map<String, Object> asMap() {
        return Map.of();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        return;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContentHelper.array(NAME, chunks.iterator());
    }
}
