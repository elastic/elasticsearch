/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class DataFrameAnalyticsDest implements ToXContentObject {

    public static DataFrameAnalyticsDest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField RESULTS_FIELD = new ParseField("results_field");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("data_frame_analytics_dest", true, Builder::new);

    static {
        PARSER.declareString(Builder::setIndex, INDEX);
        PARSER.declareString(Builder::setResultsField, RESULTS_FIELD);
    }

    private final String index;
    private final String resultsField;

    private DataFrameAnalyticsDest(String index, @Nullable String resultsField) {
        this.index = requireNonNull(index);
        this.resultsField = resultsField;
    }

    public String getIndex() {
        return index;
    }

    public String getResultsField() {
        return resultsField;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsDest other = (DataFrameAnalyticsDest) o;
        return Objects.equals(index, other.index)
            && Objects.equals(resultsField, other.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, resultsField);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {

        private String index;
        private String resultsField;

        private Builder() {}

        public Builder setIndex(String index) {
            this.index = index;
            return this;
        }

        public Builder setResultsField(String resultsField) {
            this.resultsField = resultsField;
            return this;
        }

        public DataFrameAnalyticsDest build() {
            return new DataFrameAnalyticsDest(index, resultsField);
        }
    }
}
