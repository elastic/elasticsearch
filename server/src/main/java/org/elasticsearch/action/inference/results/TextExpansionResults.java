/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TextExpansionResults extends NlpInferenceResults {

    public static final String NAME = "text_expansion_result";

    public record WeightedToken(String token, float weight) implements Writeable, ToXContentFragment {

        public WeightedToken(StreamInput in) throws IOException {
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
    protected void doXContentBody(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
        out.writeCollection(weightedTokens);
    }

    @Override
    public void addMapFields(Map<String, Object> map) {
        map.put(resultsField, weightedTokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight)));
    }

    @Override
    public Map<String, Object> asMap(String outputField) {
        var map = super.asMap(outputField);
        map.put(outputField, weightedTokens.stream().collect(Collectors.toMap(WeightedToken::token, WeightedToken::weight)));
        return map;
    }
}
