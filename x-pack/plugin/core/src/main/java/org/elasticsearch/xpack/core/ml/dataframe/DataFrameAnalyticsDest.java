/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.ParseField;
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

    public static final ConstructingObjectParser<DataFrameAnalyticsDest, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<DataFrameAnalyticsDest, Void> LENIENT_PARSER = createParser(true);

    public static ConstructingObjectParser<DataFrameAnalyticsDest, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<DataFrameAnalyticsDest, Void> parser = new ConstructingObjectParser<>("data_frame_analytics_dest",
            ignoreUnknownFields, a -> new DataFrameAnalyticsDest((String) a[0]));
        parser.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        return parser;
    }

    private final String index;

    public DataFrameAnalyticsDest(String index) {
        this.index = ExceptionsHelper.requireNonNull(index, INDEX);
        if (index.isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must be non-empty", INDEX);
        }
    }

    public DataFrameAnalyticsDest(StreamInput in) throws IOException {
        index = in.readString();
    }

    public DataFrameAnalyticsDest(DataFrameAnalyticsDest other) {
        this.index = other.index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataFrameAnalyticsDest other = (DataFrameAnalyticsDest) o;
        return Objects.equals(index, other.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }

    public String getIndex() {
        return index;
    }
}
