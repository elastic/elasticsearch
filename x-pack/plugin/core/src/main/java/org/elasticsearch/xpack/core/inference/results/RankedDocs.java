/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RankedDocs implements InferenceServiceResults {
    List<RankedDoc> rankedDocs;

    /**
     * A record representing a document that has been ranked by the cohere rerank API
     * @param id the index of the document when it was passed to the cohere rerank API
     * @param relevanceScore
     * @param text
     */
    public record RankedDoc(String id, double relevanceScore, String text) {};

    public RankedDocs() {
        this.rankedDocs = new ArrayList<RankedDoc>(0);
    }

    public void addRankedDoc(String id, double relevanceScore, String text) {
        this.rankedDocs.add(new RankedDoc(id, relevanceScore, text));
    }

    public void addRankedDoc(RankedDoc rankedDoc) {
        this.rankedDocs.add(rankedDoc);
    }

    public List<RankedDoc> getRankedDocs() {
        return this.rankedDocs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    @Override
    public String getWriteableName() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        return null;
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        return null;
    }

    @Override
    public Map<String, Object> asMap() {
        return null;
    }

}
