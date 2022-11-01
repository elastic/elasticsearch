/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.Version;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class Classification implements DataFrameAnalysis {

    public static final ParseField NAME = new ParseField("classification");

    public static final ParseField DEPENDENT_VARIABLE = new ParseField("dependent_variable");
    public static final ParseField PREDICTION_FIELD_NAME = new ParseField("prediction_field_name");
    public static final ParseField CLASS_ASSIGNMENT_OBJECTIVE = new ParseField("class_assignment_objective");
    public static final ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");
    public static final ParseField TRAINING_PERCENT = new ParseField("training_percent");
    public static final ParseField RANDOMIZE_SEED = new ParseField("randomize_seed");
    public static final ParseField FEATURE_PROCESSORS = new ParseField("feature_processors");
    public static final ParseField EARLY_STOPPING_ENABLED = new ParseField("early_stopping_enabled");

    private static final String STATE_DOC_ID_INFIX = "_classification_state#";

    private static final String NUM_CLASSES = "num_classes";

    private static final ConstructingObjectParser<Classification, Void> LENIENT_PARSER = createParser(true);
    private static final ConstructingObjectParser<Classification, Void> STRICT_PARSER = createParser(false);

    /**
     * The max number of classes classification supports
     */
    public static final int MAX_DEPENDENT_VARIABLE_CARDINALITY = 100;

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Classification, Void> createParser(boolean lenient) {
        ConstructingObjectParser<Classification, Void> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            a -> new Classification(
                (String) a[0],
                new BoostedTreeParams(
                    (Double) a[1],
                    (Double) a[2],
                    (Double) a[3],
                    (Integer) a[4],
                    (Double) a[5],
                    (Integer) a[6],
                    (Double) a[7],
                    (Double) a[8],
                    (Double) a[9],
                    (Double) a[10],
                    (Double) a[11],
                    (Integer) a[12]
                ),
                (String) a[13],
                (ClassAssignmentObjective) a[14],
                (Integer) a[15],
                (Double) a[16],
                (Long) a[17],
                (List<PreProcessor>) a[18],
                (Boolean) a[19]
            )
        );
        parser.declareString(constructorArg(), DEPENDENT_VARIABLE);
        BoostedTreeParams.declareFields(parser);
        parser.declareString(optionalConstructorArg(), PREDICTION_FIELD_NAME);
        parser.declareString(optionalConstructorArg(), ClassAssignmentObjective::fromString, CLASS_ASSIGNMENT_OBJECTIVE);
        parser.declareInt(optionalConstructorArg(), NUM_TOP_CLASSES);
        parser.declareDouble(optionalConstructorArg(), TRAINING_PERCENT);
        parser.declareLong(optionalConstructorArg(), RANDOMIZE_SEED);
        parser.declareNamedObjects(
            optionalConstructorArg(),
            (p, c, n) -> lenient
                ? p.namedObject(LenientlyParsedPreProcessor.class, n, new PreProcessor.PreProcessorParseContext(true))
                : p.namedObject(StrictlyParsedPreProcessor.class, n, new PreProcessor.PreProcessorParseContext(true)),
            (classification) -> {/*TODO should we throw if this is not set?*/},
            FEATURE_PROCESSORS
        );
        parser.declareBoolean(optionalConstructorArg(), EARLY_STOPPING_ENABLED);
        return parser;
    }

    public static Classification fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return ignoreUnknownFields ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private static final Set<String> ALLOWED_DEPENDENT_VARIABLE_TYPES = Stream.of(
        Types.categorical(),
        Types.discreteNumerical(),
        Types.bool()
    ).flatMap(Set::stream).collect(Collectors.toUnmodifiableSet());
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

    private static final List<String> PROGRESS_PHASES = Collections.unmodifiableList(
        Arrays.asList("feature_selection", "coarse_parameter_search", "fine_tuning_parameters", "final_training")
    );

    static final Map<String, Object> FEATURE_IMPORTANCE_MAPPING;
    static {
        Map<String, Object> classesProperties = new HashMap<>();
        classesProperties.put("class_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        classesProperties.put("importance", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));

        Map<String, Object> classesMapping = new HashMap<>();
        classesMapping.put("dynamic", false);
        classesMapping.put("type", NestedObjectMapper.CONTENT_TYPE);
        classesMapping.put("properties", classesProperties);

        Map<String, Object> properties = new HashMap<>();
        properties.put("feature_name", Collections.singletonMap("type", KeywordFieldMapper.CONTENT_TYPE));
        properties.put("classes", classesMapping);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("dynamic", false);
        mapping.put("type", NestedObjectMapper.CONTENT_TYPE);
        mapping.put("properties", properties);

        FEATURE_IMPORTANCE_MAPPING = Collections.unmodifiableMap(mapping);
    }

    private final String dependentVariable;
    private final BoostedTreeParams boostedTreeParams;
    private final String predictionFieldName;
    private final ClassAssignmentObjective classAssignmentObjective;
    private final int numTopClasses;
    private final double trainingPercent;
    private final long randomizeSeed;
    private final List<PreProcessor> featureProcessors;
    private final boolean earlyStoppingEnabled;

    public Classification(
        String dependentVariable,
        BoostedTreeParams boostedTreeParams,
        @Nullable String predictionFieldName,
        @Nullable ClassAssignmentObjective classAssignmentObjective,
        @Nullable Integer numTopClasses,
        @Nullable Double trainingPercent,
        @Nullable Long randomizeSeed,
        @Nullable List<PreProcessor> featureProcessors,
        @Nullable Boolean earlyStoppingEnabled
    ) {
        if (numTopClasses != null && (numTopClasses < -1 || numTopClasses > 1000)) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be an integer in [0, 1000] or a special value -1",
                NUM_TOP_CLASSES.getPreferredName()
            );
        }
        if (trainingPercent != null && (trainingPercent <= 0.0 || trainingPercent > 100.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a positive double in (0, 100]", TRAINING_PERCENT.getPreferredName());
        }
        this.dependentVariable = ExceptionsHelper.requireNonNull(dependentVariable, DEPENDENT_VARIABLE);
        this.boostedTreeParams = ExceptionsHelper.requireNonNull(boostedTreeParams, BoostedTreeParams.NAME);
        this.predictionFieldName = predictionFieldName == null ? dependentVariable + "_prediction" : predictionFieldName;
        this.classAssignmentObjective = classAssignmentObjective == null
            ? ClassAssignmentObjective.MAXIMIZE_MINIMUM_RECALL
            : classAssignmentObjective;
        this.numTopClasses = numTopClasses == null ? DEFAULT_NUM_TOP_CLASSES : numTopClasses;
        this.trainingPercent = trainingPercent == null ? 100.0 : trainingPercent;
        this.randomizeSeed = randomizeSeed == null ? Randomness.get().nextLong() : randomizeSeed;
        this.featureProcessors = featureProcessors == null ? Collections.emptyList() : Collections.unmodifiableList(featureProcessors);
        // Early stopping is true by default
        this.earlyStoppingEnabled = earlyStoppingEnabled == null ? true : earlyStoppingEnabled;
    }

    public Classification(String dependentVariable) {
        this(dependentVariable, BoostedTreeParams.builder().build(), null, null, null, null, null, null, null);
    }

    public Classification(StreamInput in) throws IOException {
        dependentVariable = in.readString();
        boostedTreeParams = new BoostedTreeParams(in);
        predictionFieldName = in.readOptionalString();
        classAssignmentObjective = in.readEnum(ClassAssignmentObjective.class);
        numTopClasses = in.readOptionalVInt();
        trainingPercent = in.readDouble();
        randomizeSeed = in.readOptionalLong();
        featureProcessors = Collections.unmodifiableList(in.readNamedWriteableList(PreProcessor.class));
        earlyStoppingEnabled = in.readBoolean();
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

    public ClassAssignmentObjective getClassAssignmentObjective() {
        return classAssignmentObjective;
    }

    public int getNumTopClasses() {
        return numTopClasses;
    }

    @Override
    public double getTrainingPercent() {
        return trainingPercent;
    }

    public long getRandomizeSeed() {
        return randomizeSeed;
    }

    public List<PreProcessor> getFeatureProcessors() {
        return featureProcessors;
    }

    public Boolean getEarlyStoppingEnabled() {
        return earlyStoppingEnabled;
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
        out.writeEnum(classAssignmentObjective);
        out.writeOptionalVInt(numTopClasses);
        out.writeDouble(trainingPercent);
        out.writeOptionalLong(randomizeSeed);
        out.writeNamedWriteableList(featureProcessors);
        out.writeBoolean(earlyStoppingEnabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Version version = Version.fromString(params.param("version", Version.CURRENT.toString()));

        builder.startObject();
        builder.field(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        boostedTreeParams.toXContent(builder, params);
        builder.field(CLASS_ASSIGNMENT_OBJECTIVE.getPreferredName(), classAssignmentObjective);
        builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (predictionFieldName != null) {
            builder.field(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        builder.field(TRAINING_PERCENT.getPreferredName(), trainingPercent);
        if (version.onOrAfter(Version.V_7_6_0)) {
            builder.field(RANDOMIZE_SEED.getPreferredName(), randomizeSeed);
        }
        if (featureProcessors.isEmpty() == false) {
            NamedXContentObjectHelper.writeNamedObjects(builder, params, true, FEATURE_PROCESSORS.getPreferredName(), featureProcessors);
        }
        builder.field(EARLY_STOPPING_ENABLED.getPreferredName(), earlyStoppingEnabled);
        builder.endObject();
        return builder;
    }

    @Override
    public Map<String, Object> getParams(FieldInfo fieldInfo) {
        Map<String, Object> params = new HashMap<>();
        params.put(DEPENDENT_VARIABLE.getPreferredName(), dependentVariable);
        params.putAll(boostedTreeParams.getParams());
        params.put(CLASS_ASSIGNMENT_OBJECTIVE.getPreferredName(), classAssignmentObjective);
        params.put(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        if (predictionFieldName != null) {
            params.put(PREDICTION_FIELD_NAME.getPreferredName(), predictionFieldName);
        }
        String predictionFieldType = getPredictionFieldTypeParamString(getPredictionFieldType(fieldInfo.getTypes(dependentVariable)));
        if (predictionFieldType != null) {
            params.put(PREDICTION_FIELD_TYPE, predictionFieldType);
        }
        params.put(NUM_CLASSES, fieldInfo.getCardinality(dependentVariable));
        params.put(TRAINING_PERCENT.getPreferredName(), trainingPercent);
        if (featureProcessors.isEmpty() == false) {
            params.put(
                FEATURE_PROCESSORS.getPreferredName(),
                featureProcessors.stream().map(p -> Collections.singletonMap(p.getName(), p)).collect(Collectors.toList())
            );
        }
        params.put(EARLY_STOPPING_ENABLED.getPreferredName(), earlyStoppingEnabled);
        params.put(RANDOMIZE_SEED.getPreferredName(), randomizeSeed);
        return params;
    }

    private static String getPredictionFieldTypeParamString(PredictionFieldType predictionFieldType) {
        if (predictionFieldType == null) {
            return null;
        }
        return switch (predictionFieldType) {
            case NUMBER ->
                // C++ process uses int64_t type, so it is safe for the dependent variable to use long numbers.
                "int";
            case STRING -> "string";
            case BOOLEAN -> "bool";
        };
    }

    public static PredictionFieldType getPredictionFieldType(Set<String> dependentVariableTypes) {
        if (dependentVariableTypes == null) {
            return null;
        }
        if (Types.categorical().containsAll(dependentVariableTypes)) {
            return PredictionFieldType.STRING;
        }
        if (Types.bool().containsAll(dependentVariableTypes)) {
            return PredictionFieldType.BOOLEAN;
        }
        if (Types.discreteNumerical().containsAll(dependentVariableTypes)) {
            return PredictionFieldType.NUMBER;
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
        return Collections.singletonList(FieldCardinalityConstraint.between(dependentVariable, 2, MAX_DEPENDENT_VARIABLE_CARDINALITY));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getResultMappings(String resultsFieldName, FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(resultsFieldName + ".is_training", Collections.singletonMap("type", BooleanFieldMapper.CONTENT_TYPE));
        additionalProperties.put(
            resultsFieldName + ".prediction_probability",
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName())
        );
        additionalProperties.put(
            resultsFieldName + ".prediction_score",
            Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName())
        );
        additionalProperties.put(resultsFieldName + ".feature_importance", FEATURE_IMPORTANCE_MAPPING);

        Map<String, FieldCapabilities> dependentVariableFieldCaps = fieldCapabilitiesResponse.getField(dependentVariable);
        if (dependentVariableFieldCaps == null || dependentVariableFieldCaps.isEmpty()) {
            throw ExceptionsHelper.badRequestException("no mappings could be found for required field [{}]", DEPENDENT_VARIABLE);
        }
        Object dependentVariableMappingType = dependentVariableFieldCaps.values().iterator().next().getType();
        additionalProperties.put(
            resultsFieldName + "." + predictionFieldName,
            Collections.singletonMap("type", dependentVariableMappingType)
        );

        Map<String, Object> topClassesProperties = new HashMap<>();
        topClassesProperties.put("class_name", Collections.singletonMap("type", dependentVariableMappingType));
        topClassesProperties.put("class_probability", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));
        topClassesProperties.put("class_score", Collections.singletonMap("type", NumberFieldMapper.NumberType.DOUBLE.typeName()));

        Map<String, Object> topClassesMapping = new HashMap<>();
        topClassesMapping.put("type", NestedObjectMapper.CONTENT_TYPE);
        topClassesMapping.put("properties", topClassesProperties);

        additionalProperties.put(resultsFieldName + ".top_classes", topClassesMapping);
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
    public String getStateDocIdPrefix(String jobId) {
        return jobId + STATE_DOC_ID_INFIX;
    }

    @Override
    public List<String> getProgressPhases() {
        return PROGRESS_PHASES;
    }

    @Override
    public InferenceConfig inferenceConfig(FieldInfo fieldInfo) {
        PredictionFieldType predictionFieldType = getPredictionFieldType(fieldInfo.getTypes(dependentVariable));
        return ClassificationConfig.builder()
            .setResultsField(predictionFieldName)
            .setNumTopClasses(numTopClasses)
            .setNumTopFeatureImportanceValues(getBoostedTreeParams().getNumTopFeatureImportanceValues())
            .setPredictionFieldType(predictionFieldType)
            .build();
    }

    @Override
    public boolean supportsInference() {
        return true;
    }

    public static String extractJobIdFromStateDoc(String stateDocId) {
        int suffixIndex = stateDocId.lastIndexOf(STATE_DOC_ID_INFIX);
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
            && Objects.equals(classAssignmentObjective, that.classAssignmentObjective)
            && Objects.equals(numTopClasses, that.numTopClasses)
            && Objects.equals(featureProcessors, that.featureProcessors)
            && Objects.equals(earlyStoppingEnabled, that.earlyStoppingEnabled)
            && trainingPercent == that.trainingPercent
            && randomizeSeed == that.randomizeSeed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            dependentVariable,
            boostedTreeParams,
            predictionFieldName,
            classAssignmentObjective,
            numTopClasses,
            trainingPercent,
            randomizeSeed,
            featureProcessors,
            earlyStoppingEnabled
        );
    }

    public enum ClassAssignmentObjective {
        MAXIMIZE_ACCURACY,
        MAXIMIZE_MINIMUM_RECALL;

        public static ClassAssignmentObjective fromString(String value) {
            return ClassAssignmentObjective.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
