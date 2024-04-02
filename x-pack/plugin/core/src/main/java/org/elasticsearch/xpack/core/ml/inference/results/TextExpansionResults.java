/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TextExpansionResults extends NlpInferenceResults {

    public static final String NAME = "text_expansion_result";

    public record QueryVector(String token, float weight) implements Writeable, ToXContentFragment {

        public QueryVector(StreamInput in) throws IOException {
            this(in.readString(), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(token);
            out.writeFloat(weight);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(token, weight);
            return builder;
        }

        public Map<String, Object> asMap() {
            return Map.of(token, weight);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    private final String resultsField;
    private final List<QueryVector> queryVectors;

    public TextExpansionResults(String resultField, List<QueryVector> queryVectors, boolean isTruncated) {
        super(isTruncated);
        this.resultsField = resultField;
        this.queryVectors = queryVectors;
    }

    public TextExpansionResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
        this.queryVectors = in.readCollectionAsList(QueryVector::new);
    }

    public List<QueryVector> getVectorDimensions() {
        return queryVectors;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public Object predictedValue() {
        throw new UnsupportedOperationException("[" + NAME + "] does not support a single predicted value");
    }

    @Override
    void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(resultsField);
        for (var vectorDimension : queryVectors) {
            vectorDimension.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        TextExpansionResults that = (TextExpansionResults) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(queryVectors, that.queryVectors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, queryVectors);
    }

    @Override
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeCollection(queryVectors);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, queryVectors.stream().collect(Collectors.toMap(QueryVector::token, QueryVector::weight)));
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        var map = super.asMap(outputField);
        map.put(outputField, queryVectors.stream().collect(Collectors.toMap(QueryVector::token, QueryVector::weight)));
        return map;
    }
}
