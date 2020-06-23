/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig.RESULTS_FIELD;

/**
 * A config update that sets the results field only.
 * Supports any type of {@link InferenceConfig}
 */
public class ResultsFieldUpdate implements InferenceConfigUpdate {

    public static final ParseField NAME = new ParseField("field_update");

    private static final ConstructingObjectParser<ResultsFieldUpdate, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(), args -> new ResultsFieldUpdate((String) args[0]));

    static {
        PARSER.declareString(constructorArg(), RESULTS_FIELD);
    }

    public static ResultsFieldUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String resultsField;

    public ResultsFieldUpdate(String resultsField) {
        this.resultsField = Objects.requireNonNull(resultsField);
    }

    public ResultsFieldUpdate(StreamInput in) throws IOException {
        resultsField = in.readString();
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof ClassificationConfig) {
            ClassificationConfigUpdate update = new ClassificationConfigUpdate(null, resultsField, null, null, null);
            return update.apply(originalConfig);
        } else if (originalConfig instanceof RegressionConfig) {
            RegressionConfigUpdate update = new RegressionConfigUpdate(resultsField, null);
            return update.apply(originalConfig);
        } else {
            throw ExceptionsHelper.badRequestException(
                "Inference config of unknown type [{}] can not be updated", originalConfig.getName());
        }
    }

    @Override
    public InferenceConfig toConfig() {
        return new RegressionConfig(resultsField);
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return true;
    }

    @Override
    public String getResultsField() {
        return resultsField;
    }

    @Override
    public InferenceConfigUpdate duplicateWithResultsField(String resultsField) {
        return new ResultsFieldUpdate(resultsField);
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
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultsFieldUpdate that = (ResultsFieldUpdate) o;
        return Objects.equals(resultsField, that.resultsField);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(resultsField);
    }
}
