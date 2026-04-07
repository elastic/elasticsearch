/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MlChunkedTextExpansionResults extends ChunkedNlpInferenceResults {
    public static final String NAME = "chunked_text_expansion_result";

    public record ChunkedResult(String matchedText, List<WeightedToken> weightedTokens) implements Writeable, ToXContentObject {

        public ChunkedResult(StreamInput in) throws IOException {
            this(in.readString(), in.readCollectionAsList(WeightedToken::new));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(matchedText);
            out.writeCollection(weightedTokens);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TEXT, matchedText);
            builder.startObject(INFERENCE);
            for (var weightedToken : weightedTokens) {
                weightedToken.toXContent(builder, params);
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }

        public Map<String, Object> asMap() {
            var map = new HashMap<String, Object>();
            map.put(TEXT, matchedText);
            map.put(INFERENCE, weightedTokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight)));
            return map;
        }
    }

    private final String resultsField;
    private final List<ChunkedResult> chunks;

    public MlChunkedTextExpansionResults(String resultField, List<ChunkedResult> chunks, boolean isTruncated) {
        super(isTruncated);
        this.resultsField = resultField;
        this.chunks = chunks;
    }

    public MlChunkedTextExpansionResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
        this.chunks = in.readCollectionAsList(ChunkedResult::new);
    }

    @Override
    void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(resultsField);
        for (var chunk : chunks) {
            chunk.toXContent(builder, params);
        }
        builder.endArray();
    }

    @Override
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeCollection(chunks);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, chunks.stream().map(ChunkedResult::asMap).collect(Collectors.toList()));
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        var map = super.asMap(outputField);
        map.put(resultsField, chunks.stream().map(ChunkedResult::asMap).collect(Collectors.toList()));
        return map;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        MlChunkedTextExpansionResults that = (MlChunkedTextExpansionResults) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(chunks, that.chunks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, chunks);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    public List<ChunkedResult> getChunks() {
        return chunks;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }
}
