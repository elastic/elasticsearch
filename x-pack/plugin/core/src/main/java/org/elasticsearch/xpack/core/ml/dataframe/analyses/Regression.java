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

public class Regression implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("regression");

    public static final ParseField DEPENDENT_VARIABLE = new ParseField("dependent_variable");
    public static final ParseField LAMBDA = new ParseField("lambda");
    public static final ParseField GAMMA = new ParseField("gamma");
    public static final ParseField ETA = new ParseField("eta");
    public static final ParseField MAXIMUM_NUMBER_TREES = new ParseField("maximum_number_trees");
    public static final ParseField FEATURE_BAG_FRACTION = new ParseField("feature_bag_fraction");
    public static final ParseField PREDICTION_FIELD_NAME = new ParseField("prediction_field_name");
    public static final ParseField TRAINING_PERCENT = new ParseField("training_percent");

    private static final ConstructingObjectParser<Regression, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Regression, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Regression, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Regression, Void> parser = new ConstructingObjectParser<>(NAME.getPreferredName(), lenient,
            a -> new Regression((String) a[0], (Double) a[1], (Double) a[2], (Double) a[3], (Integer) a[4], (Double) a[5], (String) a[6],
                (Double) a[7]));
        parser.declareString(ConstructingObjectParser.constructorArg(), DEPENDENT_VARIABLE);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), LAMBDA);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), GAMMA);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), ETA);
        parser.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAXIMUM_NUMBER_TREES);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), FEATURE_BAG_FRACTION);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), PREDICTION_FIELD_NAME);
        parser.declareDouble(ConstructingObjectParser.optionalConstructorArg(), TRAINING_PERCENT);
        return parser;
    }

    public static Regression fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final String dependentVariable;
    private final Double lambda;
    private final Double gamma;
    private final Double eta;
    private final Integer maximumNumberTrees;
    private final Double featureBagFraction;
    private final String predictionFieldName;
    private final double trainingPercent;

    public Regression(String dependentVariable, @Nullable Double lambda, @Nullable Double gamma, @Nullable Double eta,
                      @Nullable Integer maximumNumberTrees, @Nullable Double featureBagFraction, @Nullable String predictionFieldName,
                      @Nullable Double trainingPercent) {
        this.dependentVariable = Objects.requireNonNull(dependentVariable);

        if (lambda != null && lambda < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", LAMBDA.getPreferredName());
        }
        this.lambda = lambda;

        if (gamma != null && gamma < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", GAMMA.getPreferredName());
        }
        this.gamma = gamma;

        if (eta != null && (eta < 0.001 || eta > 1)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [0.001, 1]", ETA.getPreferredName());
        }
        this.eta = eta;

        if (maximumNumberTrees != null && (maximumNumberTrees <= 0 || maximumNumberTrees > 2000)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, 2000]", MAXIMUM_NUMBER_TREES.getPreferredName());
        }
        this.maximumNumberTrees = maximumNumberTrees;

        if (featureBagFraction != null && (featureBagFraction <= 0 || featureBagFraction > 1.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in (0, 1]", FEATURE_BAG_FRACTION.getPreferredName());
        }
        this.featureBagFraction = featureBagFraction;

        this.predictionFieldName = predictionFieldName;

        if (trainingPercent != null && (trainingPercent < 1.0 || trainingPercent > 100.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [1, 100]", TRAINING_PERCENT.getPreferredName());
        }
        this.trainingPercent = trainingPercent == null ? 100.0 : trainingPercent;
    }

    public Regression(String dependentVariable) {
        this(dependentVariable, null, null, null, null, null, null, null);
    }

    public Regression(StreamInput in) throws IOException {
        dependentVariable = in.readString();
        lambda = in.readOptionalDouble();
        gamma = in.readOptionalDouble();
        eta = in.readOptionalDouble();
        maximumNumberTrees = in.readOptionalVInt();
        featureBagFraction = in.readOptionalDouble();
        predictionFieldName = in.readOptionalString();
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
        out.writeOptionalDouble(lambda);
        out.writeOptionalDouble(gamma);
        out.writeOptionalDouble(eta);
        out.writeOptionalVInt(maximumNumberTrees);
        out.writeOptionalDouble(featureBagFraction);
        out.writeOptionalString(predictionFieldName);
        out.writeDouble(trainingPercent);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        if (lambda != null) {
            builder.field(LAMBDA.getPreferredName(), lambda);
        }
        if (gamma != null) {
            builder.field(GAMMA.getPreferredName(), gamma);
        }
        if (eta != null) {
            builder.field(ETA.getPreferredName(), eta);
        }
        if (maximumNumberTrees != null) {
            builder.field(MAXIMUM_NUMBER_TREES.getPreferredName(), maximumNumberTrees);
        }
        if (featureBagFraction != null) {
            builder.field(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
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
        if (lambda != null) {
            params.put(LAMBDA.getPreferredName(), lambda);
        }
        if (gamma != null) {
            params.put(GAMMA.getPreferredName(), gamma);
        }
        if (eta != null) {
            params.put(ETA.getPreferredName(), eta);
        }
        if (maximumNumberTrees != null) {
            params.put(MAXIMUM_NUMBER_TREES.getPreferredName(), maximumNumberTrees);
        }
        if (featureBagFraction != null) {
            params.put(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
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
        return Collections.singletonList(new RequiredField(dependentVariable, Types.numerical()));
    }

    @Override
    public boolean supportsMissingValues() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependentVariable, lambda, gamma, eta, maximumNumberTrees, featureBagFraction, predictionFieldName,
            trainingPercent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Regression that = (Regression) o;
        return Objects.equals(dependentVariable, that.dependentVariable)
            && Objects.equals(lambda, that.lambda)
            && Objects.equals(gamma, that.gamma)
            && Objects.equals(eta, that.eta)
            && Objects.equals(maximumNumberTrees, that.maximumNumberTrees)
            && Objects.equals(featureBagFraction, that.featureBagFraction)
            && Objects.equals(predictionFieldName, that.predictionFieldName)
            && trainingPercent == that.trainingPercent;
    }
}
