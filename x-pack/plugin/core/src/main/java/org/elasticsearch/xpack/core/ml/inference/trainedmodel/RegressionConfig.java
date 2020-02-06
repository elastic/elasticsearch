/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RegressionConfig implements InferenceConfig {

    public static final ParseField NAME = new ParseField("regression");
    private static final Version MIN_SUPPORTED_VERSION = Version.V_7_6_0;
    public static final ParseField RESULTS_FIELD = new ParseField("results_field");
    private static final String DEFAULT_RESULTS_FIELD = "predicted_value";

    public static RegressionConfig EMPTY_PARAMS = new RegressionConfig(DEFAULT_RESULTS_FIELD);

    public static RegressionConfig fromMap(Map<String, Object> map) {
        Map<String, Object> options = new HashMap<>(map);
        String resultsField = (String)options.remove(RESULTS_FIELD.getPreferredName());
        if (options.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("Unrecognized fields {}.", map.keySet());
        }
        return new RegressionConfig(resultsField);
    }

    private static final ConstructingObjectParser<RegressionConfig, Void> PARSER =
            new ConstructingObjectParser<>(NAME.getPreferredName(), args -> new RegressionConfig((String) args[0]));

    static {
        PARSER.declareString(optionalConstructorArg(), RESULTS_FIELD);
    }

    public static RegressionConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String resultsField;

    public RegressionConfig(String resultsField) {
        this.resultsField = resultsField == null ? DEFAULT_RESULTS_FIELD : resultsField;
    }

    public RegressionConfig(StreamInput in) throws IOException {
        this.resultsField = in.readString();
    }

    public String getResultsField() {
        return resultsField;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resultsField);
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegressionConfig that = (RegressionConfig)o;
        return Objects.equals(this.resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resultsField);
    }

    @Override
    public boolean isTargetTypeSupported(TargetType targetType) {
        return TargetType.REGRESSION.equals(targetType);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return MIN_SUPPORTED_VERSION;
    }

}
