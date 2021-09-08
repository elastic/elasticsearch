/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DataFrameAnalyticsDest implements Writeable, ToXContentObject {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField RESULTS_FIELD = new ParseField("results_field");

    private static final String DEFAULT_RESULTS_FIELD = "ml";

    public static ConstructingObjectParser<DataFrameAnalyticsDest, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DataFrameAnalyticsDest, Void> parser = new ConstructingObjectParser<>("data_frame_analytics_dest",
            ignoreUnknownFields, a -> new DataFrameAnalyticsDest((String) a[0], (String) a[1]));
        parser.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), RESULTS_FIELD);
        return parser;
    }

    private final String index;
    private final String resultsField;

    public DataFrameAnalyticsDest(String index, @Nullable String resultsField) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX);
        if (index.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must be non-empty", INDEX);
        }
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
    }

    public DataFrameAnalyticsDest(StreamInput in) throws IOException {
        index = in.readString();
        resultsField = in.readString();
    }

    public DataFrameAnalyticsDest(DataFrameAnalyticsDest other) {
        this.index = other.index;
        this.resultsField = other.resultsField;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(resultsField);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsDest other = (DataFrameAnalyticsDest) o;
        return Objects.equals(index, other.index) && Objects.equals(resultsField, other.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, resultsField);
    }

    public String getIndex() {
        return index;
    }

    public String getResultsField() {
        return resultsField;
    }
}
