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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig.RESULTS_FIELD;

/**
 * A config update that applies no changes.
 * Supports any type of {@link InferenceConfig}
 */
public class EmptyConfigUpdate implements InferenceConfigUpdate {

    public static final ParseField NAME = new ParseField("empty");

    private static final ConstructingObjectParser<EmptyConfigUpdate, Void> PARSER =
        new ConstructingObjectParser<>(NAME.getPreferredName(), args -> new EmptyConfigUpdate((String) args[0]));

    static {
        PARSER.declareString(optionalConstructorArg(), RESULTS_FIELD);
    }

    public static EmptyConfigUpdate fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String resultsField;

    public EmptyConfigUpdate(String resultsField) {
        this.resultsField = resultsField;
    }

    public EmptyConfigUpdate(StreamInput in) throws IOException {
        resultsField = in.readOptionalString();
    }

    @Override
    public InferenceConfig apply(InferenceConfig originalConfig) {
        if (originalConfig instanceof ClassificationConfig) {
            ClassificationConfig config = (ClassificationConfig)originalConfig;

            ClassificationConfigUpdate update = new ClassificationConfigUpdate()
        }
    }

    @Override
    public InferenceConfig toConfig() {
        return RegressionConfig.EMPTY_PARAMS;
    }

    @Override
    public boolean isSupported(InferenceConfig config) {
        return true;
    }

    @Override
    public String getResultsField() {
        return null;
    }

    @Override
    public InferenceConfigUpdate duplicateWithResultsField(String resultsField) {
        return new EmptyConfigUpdate(resultsField);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(resultsField);

    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (resultsField != null) {
            builder.field(RESULTS_FIELD.getPreferredName(), resultsField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        // Equal if o is not null and the same class
        return (o == null || getClass() != o.getClass()) == false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(resultsField);
    }
}
