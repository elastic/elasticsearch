/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class Regression implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("regression");

    public static final ParseField DEPENDENT_VARIABLE = new ParseField("dependent_variable");
    public static final ParseField PREDICTION_FIELD_NAME = new ParseField("prediction_field_name");
    public static final ParseField TRAINING_PERCENT = new ParseField("training_percent");
    public static final ParseField RANDOMIZE_SEED = new ParseField("randomize_seed");
    public static final ParseField LOSS_FUNCTION = new ParseField("loss_function");
    public static final ParseField LOSS_FUNCTION_PARAMETER = new ParseField("loss_function_parameter");

    private static final String STATE_DOC_ID_SUFFIX = "_regression_state#1";

    private static final ConstructingObjectParser<Regression, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Regression, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Regression, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Regression, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new Regression(
                (String) a[0],
                new BoostedTreeParams((Double) a[1], (Double) a[2], (Double) a[3], (Integer) a[4], (Double) a[5], (Integer) a[6]),
                (String) a[7],
                (Double) a[8],
                (Long) a[9],
                (LossFunction) a[10],
                (Double) a[11]));
        parser.declareString(constructorArg(), DEPENDENT_VARIABLE);
        BoostedTreeParams.declareFields(parser);
        parser.declareString(optionalConstructorArg(), PREDICTION_FIELD_NAME);
        parser.declareDouble(optionalConstructorArg(), TRAINING_PERCENT);
        parser.declareLong(optionalConstructorArg(), RANDOMIZE_SEED);
        parser.declareString(optionalConstructorArg(), LossFunction::fromString, LOSS_FUNCTION);
        parser.declareDouble(optionalConstructorArg(), LOSS_FUNCTION_PARAMETER);
        return parser;
    }

    public static Regression fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private static final List<String> PROGRESS_PHASES = Collections.unmodifiableList(
        Arrays.asList(
            "feature_selection",
            "coarse_parameter_search",
            "fine_tuning_parameters",
            "final_training"
        )
    );

    private final String dependentVariable;
    private final BoostedTreeParams boostedTreeParams;
    private final String predictionFieldName;
    private final double trainingPercent;
    private final long randomizeSeed;
    private final LossFunction lossFunction;
    private final Double lossFunctionParameter;

    public Regression(String dependentVariable,
                      BoostedTreeParams boostedTreeParams,
                      @Nullable String predictionFieldName,
                      @Nullable Double trainingPercent,
                      @Nullable Long randomizeSeed,
                      @Nullable LossFunction lossFunction,
                      @Nullable Double lossFunctionParameter) {
        if (trainingPercent != null && (trainingPercent < 1.0 || trainingPercent > 100.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [1, 100]", TRAINING_PERCENT.getPreferredName());
        }
        this.dependentVariable = ExceptionsHelper.requireNonNull(dependentVariable, DEPENDENT_VARIABLE);
        this.boostedTreeParams = ExceptionsHelper.requireNonNull(boostedTreeParams, BoostedTreeParams.NAME);
        this.predictionFieldName = predictionFieldName == null ? dependentVariable + "_prediction" : predictionFieldName;
        this.trainingPercent = trainingPercent == null ? 100.0 : trainingPercent;
        this.randomizeSeed = randomizeSeed == null ? Randomness.get().nextLong() : randomizeSeed;
        // Prior to introducing the loss function setting only MSE was implemented
        this.lossFunction = lossFunction == null ? LossFunction.MSE : lossFunction;
        if (lossFunctionParameter != null && lossFunctionParameter <= 0.0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive double", LOSS_FUNCTION_PARAMETER.getPreferredName());
        }
        this.lossFunctionParameter = lossFunctionParameter;
    }

    public Regression(String dependentVariable) {
        this(dependentVariable, BoostedTreeParams.builder().build(), null, null, null, null, null);
    }

    public Regression(StreamInput in) throws IOException {
        dependentVariable = in.readString();
        boostedTreeParams = new BoostedTreeParams(in);
        predictionFieldName = in.readOptionalString();
        trainingPercent = in.readDouble();
        randomizeSeed = in.readOptionalLong();
        lossFunction = in.readEnum(LossFunction.class);
        lossFunctionParameter = in.readOptionalDouble();
    }

    public String getDependentVariable() {
        return dependentVariable;
    }

    public BoostedTreeParams getBoostedTreeParams() {
        return boostedTreeParams;
    }

    public String getPredictionFieldName() {
        return predictionFieldName;
    }

    public double getTrainingPercent() {
        return trainingPercent;
    }

    public long getRandomizeSeed() {
        return randomizeSeed;
    }

    public LossFunction getLossFunction() {
        return lossFunction;
    }

    public Double getLossFunctionParameter() {
        return lossFunctionParameter;
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
        out.writeDouble(trainingPercent);
        out.writeOptionalLong(randomizeSeed);
        out.writeEnum(lossFunction);
        out.writeOptionalDouble(lossFunctionParameter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Version version = Version.fromString(params.param("version", Version.CURRENT.toString()));

        builder.startObject();
        builder.field(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        boostedTreeParams.toXContent(builder, params);
        if (predictionFieldName != null) {
            builder.field(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        builder.field(TRAINING_PERCENT.getPreferredName(), trainingPercent);
        if (version.onOrAfter(Version.V_7_6_0)) {
            builder.field(RANDOMIZE_SEED.getPreferredName(), randomizeSeed);
        }
        builder.field(LOSS_FUNCTION.getPreferredName(), lossFunction);
        if (lossFunctionParameter != null) {
            builder.field(LOSS_FUNCTION_PARAMETER.getPreferredName(), lossFunctionParameter);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Map<String, Object> getParams(FieldInfo fieldInfo) {
        Map<String, Object> params = new HashMap<>();
        params.put(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        params.putAll(boostedTreeParams.getParams());
        if (predictionFieldName != null) {
            params.put(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        params.put(TRAINING_PERCENT.getPreferredName(), trainingPercent);
        params.put(LOSS_FUNCTION.getPreferredName(), lossFunction.toString());
        if (lossFunctionParameter != null) {
            params.put(LOSS_FUNCTION_PARAMETER.getPreferredName(), lossFunctionParameter);
        }
        return params;
    }

    @Override
    public boolean supportsCategoricalFields() {
        return true;
    }

    @Override
    public Set<String> getAllowedCategoricalTypes(String fieldName) {
        return Types.categorical();
    }

    @Override
    public List<RequiredField> getRequiredFields() {
        return Collections.singletonList(new RequiredField(dependentVariable, Types.numerical()));
    }

    @Override
    public List<FieldCardinalityConstraint> getFieldCardinalityConstraints() {
        return Collections.emptyList();
    }

    @Override
    public Map<String, Object> getExplicitlyMappedFields(Map<String, Object> mappingsProperties, String resultsFieldName) {
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(resultsFieldName + ".feature_importance", MapUtils.featureImportanceMapping());
        // Prediction field should be always mapped as "double" rather than "float" in order to increase precision in case of
        // high (over 10M) values of dependent variable.
        additionalProperties.put(resultsFieldName + "." + predictionFieldName,
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));
        return additionalProperties;
    }

    @Override
    public boolean supportsMissingValues() {
        return true;
    }

    @Override
    public boolean persistsState() {
        return true;
    }

    @Override
    public String getStateDocId(String jobId) {
        return jobId + STATE_DOC_ID_SUFFIX;
    }

    @Override
    public List<String> getProgressPhases() {
        return PROGRESS_PHASES;
    }

    @Override
    public InferenceConfig inferenceConfig(FieldInfo fieldInfo) {
        return RegressionConfig.builder()
            .setResultsField(predictionFieldName)
            .setNumTopFeatureImportanceValues(getBoostedTreeParams().getNumTopFeatureImportanceValues())
            .build();
    }

    @Override
    public boolean supportsInference() {
        return true;
    }

    public static String extractJobIdFromStateDoc(String stateDocId) {
        int suffixIndex = stateDocId.lastIndexOf(STATE_DOC_ID_SUFFIX);
        return suffixIndex <= 0 ? null : stateDocId.substring(0, suffixIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Regression that = (Regression) o;
        return Objects.equals(dependentVariable, that.dependentVariable)
            && Objects.equals(boostedTreeParams, that.boostedTreeParams)
            && Objects.equals(predictionFieldName, that.predictionFieldName)
            && trainingPercent == that.trainingPercent
            && randomizeSeed == that.randomizeSeed
            && lossFunction == that.lossFunction
            && Objects.equals(lossFunctionParameter, that.lossFunctionParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependentVariable, boostedTreeParams, predictionFieldName, trainingPercent, randomizeSeed, lossFunction,
            lossFunctionParameter);
    }

    public enum LossFunction {
        MSE, MSLE, HUBER;

        private static LossFunction fromString(String value) {
            return LossFunction.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
