/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper.requireNonNull;

public record NamedQueryExtractorBuilder(String queryName, String featureName) implements LearnToRankFeatureExtractorBuilder {

    public static final ParseField NAME = new ParseField("named_query");
    public static final ParseField QUERY_NAME = new ParseField("query_name");

    private static final ConstructingObjectParser<NamedQueryExtractorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        a -> new NamedQueryExtractorBuilder((String) a[0], (String) a[1])
    );
    private static final ConstructingObjectParser<NamedQueryExtractorBuilder, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        NAME.getPreferredName(),
        true,
        a -> new NamedQueryExtractorBuilder((String) a[0], (String) a[1])
    );
    static {
        PARSER.declareString(constructorArg(), QUERY_NAME);
        PARSER.declareString(optionalConstructorArg(), FEATURE_NAME);
        LENIENT_PARSER.declareString(constructorArg(), QUERY_NAME);
        LENIENT_PARSER.declareString(optionalConstructorArg(), FEATURE_NAME);
    }

    public static NamedQueryExtractorBuilder fromXContent(XContentParser parser, Object context) {
        boolean lenient = Boolean.TRUE.equals(context);
        return lenient ? LENIENT_PARSER.apply(parser, null) : PARSER.apply(parser, null);
    }

    public NamedQueryExtractorBuilder(String queryName, @Nullable String featureName) {
        this.queryName = requireNonNull(queryName, QUERY_NAME);
        this.featureName = featureName;
    }

    public NamedQueryExtractorBuilder(StreamInput input) throws IOException {
        this(input.readString(), input.readOptionalString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(QUERY_NAME.getPreferredName(), queryName);
        if (featureName != null) {
            builder.field(FEATURE_NAME.getPreferredName(), featureName);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryName);
        out.writeOptionalString(featureName);
    }

    @Override
    public String featureName() {
        return featureName == null ? queryName : featureName;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedQueryExtractorBuilder that = (NamedQueryExtractorBuilder) o;
        return Objects.equals(queryName, that.queryName) && Objects.equals(featureName, that.featureName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryName, featureName);
    }

    @Override
    public String toString() {
        return "NamedQueryExtractorBuilder{" +
            "queryName='" + queryName + '\'' +
            ", featureName='" + featureName + '\'' +
            '}';
    }
}
