/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TextExpansionResults extends NlpInferenceResults {

    public static final String NAME = "text_expansion_result";

    private final String resultsField;
    private final List<WeightedToken> weightedTokens;

    public TextExpansionResults(String resultField, List<WeightedToken> weightedTokens, boolean isTruncated) {
        super(isTruncated);
        this.resultsField = resultField;
        this.weightedTokens = weightedTokens;
    }

    public TextExpansionResults(StreamInput in) throws IOException {
        super(in);
        this.resultsField = in.readString();
        this.weightedTokens = in.readCollectionAsList(WeightedToken::new);
    }

    public List<WeightedToken> getWeightedTokens() {
        return weightedTokens;
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
        for (var weightedToken : weightedTokens) {
            weightedToken.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        TextExpansionResults that = (TextExpansionResults) o;
        return Objects.equals(resultsField, that.resultsField) && Objects.equals(weightedTokens, that.weightedTokens);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultsField, weightedTokens);
    }

    @Override
    void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeCollection(weightedTokens);
    }

    @Override
    void addMapFields(Map<String, Object> map) {
        map.put(resultsField, weightedTokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight)));
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        var map = super.asMap(outputField);
        map.put(outputField, weightedTokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight)));
        return map;
    }
}
