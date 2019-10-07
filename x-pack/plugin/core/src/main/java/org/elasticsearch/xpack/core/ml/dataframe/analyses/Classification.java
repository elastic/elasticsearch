/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class Classification implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("classification");

    public static final ParseField DEPENDENT_VARIABLE = new ParseField("dependent_variable");
    public static final ParseField PREDICTION_FIELD_NAME = new ParseField("prediction_field_name");
    public static final ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");
    public static final ParseField TRAINING_PERCENT = new ParseField("training_percent");

    private static final ConstructingObjectParser<Classification, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Classification, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Classification, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Classification, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new Classification(
                (String) a[0],
                new BoostedTreeParams((Double) a[1], (Double) a[2], (Double) a[3], (Integer) a[4], (Double) a[5]),
                (String) a[6],
                (Integer) a[7],
                (Double) a[8]));
        parser.declareString(constructorArg(), DEPENDENT_VARIABLE);
        BoostedTreeParams.declareFields(parser);
        parser.declareString(optionalConstructorArg(), PREDICTION_FIELD_NAME);
        parser.declareInt(optionalConstructorArg(), NUM_TOP_CLASSES);
        parser.declareDouble(optionalConstructorArg(), TRAINING_PERCENT);
        return parser;
    }

    public static Classification fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final String dependentVariable;
    private final BoostedTreeParams boostedTreeParams;
    private final String predictionFieldName;
    private final int numTopClasses;
    private final double trainingPercent;

    public Classification(String dependentVariable,
                          BoostedTreeParams boostedTreeParams,
                          @Nullable String predictionFieldName,
                          @Nullable Integer numTopClasses,
                          @Nullable Double trainingPercent) {
        if (numTopClasses != null && (numTopClasses < 0 || numTopClasses > 1000)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [0, 1000]", NUM_TOP_CLASSES.getPreferredName());
        }
        if (trainingPercent != null && (trainingPercent < 1.0 || trainingPercent > 100.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [1, 100]", TRAINING_PERCENT.getPreferredName());
        }
        this.dependentVariable = ExceptionsHelper.requireNonNull(dependentVariable, DEPENDENT_VARIABLE);
        this.boostedTreeParams = ExceptionsHelper.requireNonNull(boostedTreeParams, BoostedTreeParams.NAME);
        this.predictionFieldName = predictionFieldName;
        this.numTopClasses = numTopClasses == null ? 0 : numTopClasses;
        this.trainingPercent = trainingPercent == null ? 100.0 : trainingPercent;
    }

    public Classification(String dependentVariable) {
        this(dependentVariable, new BoostedTreeParams(), null, null, null);
    }

    public Classification(StreamInput in) throws IOException {
        dependentVariable = in.readString();
        boostedTreeParams = new BoostedTreeParams(in);
        predictionFieldName = in.readOptionalString();
        numTopClasses = in.readOptionalVInt();
        trainingPercent = in.readDouble();
    }

    public String getDependentVariable() {
        return dependentVariable;
    }

    public double getTrainingPercent() {
        return trainingPercent;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(dependentVariable);
        boostedTreeParams.writeTo(out);
        out.writeOptionalString(predictionFieldName);
        out.writeOptionalVInt(numTopClasses);
        out.writeDouble(trainingPercent);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        boostedTreeParams.toXContent(builder, params);
        builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (predictionFieldName != null) {
            builder.field(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        builder.field(TRAINING_PERCENT.getPreferredName(), trainingPercent);
        builder.endObject();
        return builder;
    }

    @Override
    public Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        params.put(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        params.putAll(boostedTreeParams.getParams());
        params.put(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (predictionFieldName != null) {
            params.put(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        return params;
    }

    @Override
    public boolean supportsCategoricalFields() {
        return true;
    }

    @Override
    public List<RequiredField> getRequiredFields() {
        return Collections.singletonList(new RequiredField(dependentVariable, Types.categorical()));
    }

    @Override
    public boolean supportsMissingValues() {
        return true;
    }

    @Override
    public boolean persistsState() {
        return false;
    }

    @Override
    public String getStateDocId(String jobId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Classification that = (Classification) o;
        return Objects.equals(dependentVariable, that.dependentVariable)
            && Objects.equals(boostedTreeParams, that.boostedTreeParams)
            && Objects.equals(predictionFieldName, that.predictionFieldName)
            && Objects.equals(numTopClasses, that.numTopClasses)
            && trainingPercent == that.trainingPercent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependentVariable, boostedTreeParams, predictionFieldName, numTopClasses, trainingPercent);
    }
}
