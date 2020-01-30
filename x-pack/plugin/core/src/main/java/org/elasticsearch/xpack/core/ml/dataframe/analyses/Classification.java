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
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;

public class Classification implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("classification");

    public static final ParseField DEPENDENT_VARIABLE = new ParseField("dependent_variable");
    public static final ParseField PREDICTION_FIELD_NAME = new ParseField("prediction_field_name");
    public static final ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");
    public static final ParseField TRAINING_PERCENT = new ParseField("training_percent");
    public static final ParseField RANDOMIZE_SEED = new ParseField("randomize_seed");

    private static final String STATE_DOC_ID_SUFFIX = "_classification_state#1";

    private static final ConstructingObjectParser<Classification, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Classification, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Classification, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Classification, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new Classification(
                (String) a[0],
                new BoostedTreeParams((Double) a[1], (Double) a[2], (Double) a[3], (Integer) a[4], (Double) a[5], (Integer) a[6]),
                (String) a[7],
                (Integer) a[8],
                (Double) a[9],
                (Long) a[10]));
        parser.declareString(constructorArg(), DEPENDENT_VARIABLE);
        BoostedTreeParams.declareFields(parser);
        parser.declareString(optionalConstructorArg(), PREDICTION_FIELD_NAME);
        parser.declareInt(optionalConstructorArg(), NUM_TOP_CLASSES);
        parser.declareDouble(optionalConstructorArg(), TRAINING_PERCENT);
        parser.declareLong(optionalConstructorArg(), RANDOMIZE_SEED);
        return parser;
    }

    public static Classification fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private static final Set<String> ALLOWED_DEPENDENT_VARIABLE_TYPES =
        Stream.of(Types.categorical(), Types.discreteNumerical(), Types.bool())
            .flatMap(Set::stream)
            .collect(Collectors.toUnmodifiableSet());
    /**
     * Name of the parameter passed down to C++.
     * This parameter is used to decide which JSON data type from {string, int, bool} to use when writing the prediction.
     */
    private static final String PREDICTION_FIELD_TYPE = "prediction_field_type";

    /**
     * As long as we only support binary classification it makes sense to always report both classes with their probabilities.
     * This way the user can see if the prediction was made with confidence they need.
     */
    private static final int DEFAULT_NUM_TOP_CLASSES = 2;

    private final String dependentVariable;
    private final BoostedTreeParams boostedTreeParams;
    private final String predictionFieldName;
    private final int numTopClasses;
    private final double trainingPercent;
    private final long randomizeSeed;

    public Classification(String dependentVariable,
                          BoostedTreeParams boostedTreeParams,
                          @Nullable String predictionFieldName,
                          @Nullable Integer numTopClasses,
                          @Nullable Double trainingPercent,
                          @Nullable Long randomizeSeed) {
        if (numTopClasses != null && (numTopClasses < 0 || numTopClasses > 1000)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [0, 1000]", NUM_TOP_CLASSES.getPreferredName());
        }
        if (trainingPercent != null && (trainingPercent < 1.0 || trainingPercent > 100.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [1, 100]", TRAINING_PERCENT.getPreferredName());
        }
        this.dependentVariable = ExceptionsHelper.requireNonNull(dependentVariable, DEPENDENT_VARIABLE);
        this.boostedTreeParams = ExceptionsHelper.requireNonNull(boostedTreeParams, BoostedTreeParams.NAME);
        this.predictionFieldName = predictionFieldName == null ? dependentVariable + "_prediction" : predictionFieldName;
        this.numTopClasses = numTopClasses == null ? DEFAULT_NUM_TOP_CLASSES : numTopClasses;
        this.trainingPercent = trainingPercent == null ? 100.0 : trainingPercent;
        this.randomizeSeed = randomizeSeed == null ? Randomness.get().nextLong() : randomizeSeed;
    }

    public Classification(String dependentVariable) {
        this(dependentVariable, BoostedTreeParams.builder().build(), null, null, null, null);
    }

    public Classification(StreamInput in) throws IOException {
        dependentVariable = in.readString();
        boostedTreeParams = new BoostedTreeParams(in);
        predictionFieldName = in.readOptionalString();
        numTopClasses = in.readOptionalVInt();
        trainingPercent = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            randomizeSeed = in.readOptionalLong();
        } else {
            randomizeSeed = Randomness.get().nextLong();
        }
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

    public int getNumTopClasses() {
        return numTopClasses;
    }

    public double getTrainingPercent() {
        return trainingPercent;
    }

    @Nullable
    public Long getRandomizeSeed() {
        return randomizeSeed;
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
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeOptionalLong(randomizeSeed);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Version version = Version.fromString(params.param("version", Version.CURRENT.toString()));

        builder.startObject();
        builder.field(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        boostedTreeParams.toXContent(builder, params);
        builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (predictionFieldName != null) {
            builder.field(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        builder.field(TRAINING_PERCENT.getPreferredName(), trainingPercent);
        if (version.onOrAfter(Version.V_7_6_0)) {
            builder.field(RANDOMIZE_SEED.getPreferredName(), randomizeSeed);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public Map<String, Object> getParams(Map<String, Set<String>> extractedFields) {
        Map<String, Object> params = new HashMap<>();
        params.put(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        params.putAll(boostedTreeParams.getParams());
        params.put(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (predictionFieldName != null) {
            params.put(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        String predictionFieldType = getPredictionFieldType(extractedFields.get(dependentVariable));
        if (predictionFieldType != null) {
            params.put(PREDICTION_FIELD_TYPE, predictionFieldType);
        }
        return params;
    }

    private static String getPredictionFieldType(Set<String> dependentVariableTypes) {
        if (dependentVariableTypes == null) {
            return null;
        }
        if (Types.categorical().containsAll(dependentVariableTypes)) {
            return "string";
        }
        if (Types.bool().containsAll(dependentVariableTypes)) {
            return "bool";
        }
        if (Types.discreteNumerical().containsAll(dependentVariableTypes)) {
            // C++ process uses int64_t type, so it is safe for the dependent variable to use long numbers.
            return "int";
        }
        return null;
    }

    @Override
    public boolean supportsCategoricalFields() {
        return true;
    }

    @Override
    public Set<String> getAllowedCategoricalTypes(String fieldName) {
        if (dependentVariable.equals(fieldName)) {
            return ALLOWED_DEPENDENT_VARIABLE_TYPES;
        }
        return Types.categorical();
    }

    @Override
    public List<RequiredField> getRequiredFields() {
        return Collections.singletonList(new RequiredField(dependentVariable, ALLOWED_DEPENDENT_VARIABLE_TYPES));
    }

    @Override
    public List<FieldCardinalityConstraint> getFieldCardinalityConstraints() {
        // This restriction is due to the fact that currently the C++ backend only supports binomial classification.
        return Collections.singletonList(FieldCardinalityConstraint.between(dependentVariable, 2, 2));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getExplicitlyMappedFields(Map<String, Object> mappingsProperties, String resultsFieldName) {
        Object dependentVariableMapping = extractMapping(dependentVariable, mappingsProperties);
        if ((dependentVariableMapping instanceof Map) == false) {
            return Collections.emptyMap();
        }
        Map<String, Object> dependentVariableMappingAsMap = (Map) dependentVariableMapping;
        // If the source field is an alias, fetch the concrete field that the alias points to.
        if (FieldAliasMapper.CONTENT_TYPE.equals(dependentVariableMappingAsMap.get("type"))) {
            String path = (String) dependentVariableMappingAsMap.get(FieldAliasMapper.Names.PATH);
            dependentVariableMapping = extractMapping(path, mappingsProperties);
        }
        // We may have updated the value of {@code dependentVariableMapping} in the "if" block above.
        // Hence, we need to check the "instanceof" condition again.
        if ((dependentVariableMapping instanceof Map) == false) {
            return Collections.emptyMap();
        }
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(resultsFieldName + "." + predictionFieldName, dependentVariableMapping);
        additionalProperties.put(resultsFieldName + ".top_classes.class_name", dependentVariableMapping);
        return additionalProperties;
    }

    private static Object extractMapping(String path, Map<String, Object> mappingsProperties) {
        return extractValue(String.join(".properties.", path.split("\\.")), mappingsProperties);
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

    public static String extractJobIdFromStateDoc(String stateDocId) {
        int suffixIndex = stateDocId.lastIndexOf(STATE_DOC_ID_SUFFIX);
        return suffixIndex <= 0 ? null : stateDocId.substring(0, suffixIndex);
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
            && trainingPercent == that.trainingPercent
            && randomizeSeed == that.randomizeSeed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dependentVariable, boostedTreeParams, predictionFieldName, numTopClasses, trainingPercent, randomizeSeed);
    }
}
