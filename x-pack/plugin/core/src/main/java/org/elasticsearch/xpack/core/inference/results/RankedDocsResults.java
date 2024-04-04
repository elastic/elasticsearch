/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RankedDocsResults implements InferenceServiceResults {
    public static final String NAME = "rerank_service_results";
    public static final String RERANK = TaskType.RERANK.toString();

    List<RankedDoc> rankedDocs;

    public RankedDocsResults(List<RankedDoc> rankedDocs) {
        this.rankedDocs = rankedDocs;
    }

    /**
     * A record representing a document that has been ranked by the cohere rerank API
     * @param index the index of the document when it was passed to the cohere rerank API
     * @param relevanceScore
     * @param text
     */
    public record RankedDoc(String index, String relevanceScore, String text) implements Writeable, ToXContentObject {

        public static final String NAME = "ranked_doc";
        public static final String INDEX = "index";
        public static final String RELEVANCE_SCORE = "relevance_score";
        public static final String TEXT = "text";

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(INDEX, index);
            builder.field(RELEVANCE_SCORE, relevanceScore);
            builder.field(TEXT, text);

            builder.endObject();

            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(relevanceScore);
            out.writeString(text);
        }

        public Map<String, Object> asMap() {
            return Map.of(NAME, Map.of(INDEX, index, RELEVANCE_SCORE, relevanceScore, TEXT, text));
        }

        public String toString() {
            return "RankedDoc{"
                + "index='"
                + index
                + '\''
                + ", relevanceScore='"
                + relevanceScore
                + '\''
                + ", text='"
                + text
                + '\''
                + ", hashcode="
                + hashCode()
                + '}';
        }
    };

    public RankedDocsResults() {
        this.rankedDocs = new ArrayList<RankedDoc>(0);
    }

    public List<RankedDoc> getRankedDocs() {
        return this.rankedDocs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(RERANK);
        for (RankedDoc rankedDoc : rankedDocs) {
            rankedDoc.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(rankedDocs);
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Coordination format not supported by " + NAME);
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("Legacy format not supported by " + NAME);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(RERANK, rankedDocs.stream().map(RankedDoc::asMap).collect(Collectors.toList()));
        return map;
    }

}
