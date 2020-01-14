/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RawInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NullInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.classificationLabel;

public class Ensemble implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Ensemble.class);
    // TODO should we have regression/classification sub-classes that accept the builder?
    public static final ParseField NAME = new ParseField("ensemble");
    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField TRAINED_MODELS = new ParseField("trained_models");
    public static final ParseField AGGREGATE_OUTPUT  = new ParseField("aggregate_output");
    public static final ParseField TARGET_TYPE = new ParseField("target_type");
    public static final ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");
    public static final ParseField CLASSIFICATION_WEIGHTS = new ParseField("classification_weights");

    private static final ObjectParser<Ensemble.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<Ensemble.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Ensemble.Builder, Void> createParser(boolean lenient) {
        ObjectParser<Ensemble.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            Ensemble.Builder::builderForParser);
        parser.declareStringArray(Ensemble.Builder::setFeatureNames, FEATURE_NAMES);
        parser.declareNamedObjects(Ensemble.Builder::setTrainedModels,
            (p, c, n) ->
                lenient ? p.namedObject(LenientlyParsedTrainedModel.class, n, null) :
                    p.namedObject(StrictlyParsedTrainedModel.class, n, null),
            (ensembleBuilder) -> ensembleBuilder.setModelsAreOrdered(true),
            TRAINED_MODELS);
        parser.declareNamedObjects(Ensemble.Builder::setOutputAggregatorFromParser,
            (p, c, n) ->
                lenient ? p.namedObject(LenientlyParsedOutputAggregator.class, n, null) :
                    p.namedObject(StrictlyParsedOutputAggregator.class, n, null),
            (ensembleBuilder) -> {/*Noop as it could be an array or object, it just has to be a one*/},
            AGGREGATE_OUTPUT);
        parser.declareString(Ensemble.Builder::setTargetType, TARGET_TYPE);
        parser.declareStringArray(Ensemble.Builder::setClassificationLabels, CLASSIFICATION_LABELS);
        parser.declareDoubleArray(Ensemble.Builder::setClassificationWeights, CLASSIFICATION_WEIGHTS);
        return parser;
    }

    public static Ensemble fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static Ensemble fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private final List<String> featureNames;
    private final List<TrainedModel> models;
    private final OutputAggregator outputAggregator;
    private final TargetType targetType;
    private final List<String> classificationLabels;
    private final double[] classificationWeights;

    Ensemble(List<String> featureNames,
             List<TrainedModel> models,
             OutputAggregator outputAggregator,
             TargetType targetType,
             @Nullable List<String> classificationLabels,
             @Nullable double[] classificationWeights) {
        this.featureNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(featureNames, FEATURE_NAMES));
        this.models = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(models, TRAINED_MODELS));
        this.outputAggregator = ExceptionsHelper.requireNonNull(outputAggregator, AGGREGATE_OUTPUT);
        this.targetType = ExceptionsHelper.requireNonNull(targetType, TARGET_TYPE);
        this.classificationLabels = classificationLabels == null ? null : Collections.unmodifiableList(classificationLabels);
        this.classificationWeights = classificationWeights == null ?
            null :
            Arrays.copyOf(classificationWeights, classificationWeights.length);
    }

    public Ensemble(StreamInput in) throws IOException {
        this.featureNames = Collections.unmodifiableList(in.readStringList());
        this.models = Collections.unmodifiableList(in.readNamedWriteableList(TrainedModel.class));
        this.outputAggregator = in.readNamedWriteable(OutputAggregator.class);
        this.targetType = TargetType.fromStream(in);
        if (in.readBoolean()) {
            this.classificationLabels = in.readStringList();
        } else {
            this.classificationLabels = null;
        }
        if (in.readBoolean()) {
            this.classificationWeights = in.readDoubleArray();
        } else {
            this.classificationWeights = null;
        }
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config) {
        if (config.isTargetTypeSupported(targetType) == false) {
            throw ExceptionsHelper.badRequestException(
                "Cannot infer using configuration for [{}] when model target_type is [{}]", config.getName(), targetType.toString());
        }
        List<Double> inferenceResults = this.models.stream().map(model -> {
            InferenceResults results = model.infer(fields, NullInferenceConfig.INSTANCE);
            assert results instanceof SingleValueInferenceResults;
            return ((SingleValueInferenceResults)results).value();
        }).collect(Collectors.toList());
        List<Double> processed = outputAggregator.processValues(inferenceResults);
        return buildResults(processed, config);
    }

    @Override
    public TargetType targetType() {
        return targetType;
    }

    private InferenceResults buildResults(List<Double> processedInferences, InferenceConfig config) {
        // Indicates that the config is useless and the caller just wants the raw value
        if (config instanceof NullInferenceConfig) {
            return new RawInferenceResults(outputAggregator.aggregate(processedInferences));
        }
        switch(targetType) {
            case REGRESSION:
                return new RegressionInferenceResults(outputAggregator.aggregate(processedInferences), config);
            case CLASSIFICATION:
                ClassificationConfig classificationConfig = (ClassificationConfig) config;
                assert classificationWeights == null || processedInferences.size() == classificationWeights.length;
                // Adjust the probabilities according to the thresholds
                Tuple<Integer, List<ClassificationInferenceResults.TopClassEntry>> topClasses = InferenceHelpers.topClasses(
                    processedInferences,
                    classificationLabels,
                    classificationWeights,
                    classificationConfig.getNumTopClasses());
                return new ClassificationInferenceResults((double)topClasses.v1(),
                    classificationLabel(topClasses.v1(), classificationLabels),
                    topClasses.v2(),
                    config);
            default:
                throw new UnsupportedOperationException("unsupported target_type [" + targetType + "] for inference on ensemble model");
        }
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(featureNames);
        out.writeNamedWriteableList(models);
        out.writeNamedWriteable(outputAggregator);
        targetType.writeTo(out);
        out.writeBoolean(classificationLabels != null);
        if (classificationLabels != null) {
            out.writeStringCollection(classificationLabels);
        }
        out.writeBoolean(classificationWeights != null);
        if (classificationWeights != null) {
            out.writeDoubleArray(classificationWeights);
        }
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAMES.getPreferredName(), featureNames);
        NamedXContentObjectHelper.writeNamedObjects(builder, params, true, TRAINED_MODELS.getPreferredName(), models);
        NamedXContentObjectHelper.writeNamedObjects(builder,
            params,
            false,
            AGGREGATE_OUTPUT.getPreferredName(),
            Collections.singletonList(outputAggregator));
        builder.field(TARGET_TYPE.getPreferredName(), targetType.toString());
        if (classificationLabels != null) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        if (classificationWeights != null) {
            builder.field(CLASSIFICATION_WEIGHTS.getPreferredName(), classificationWeights);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ensemble that = (Ensemble) o;
        return Objects.equals(featureNames, that.featureNames)
            && Objects.equals(models, that.models)
            && Objects.equals(targetType, that.targetType)
            && Objects.equals(classificationLabels, that.classificationLabels)
            && Objects.equals(outputAggregator, that.outputAggregator)
            && Arrays.equals(classificationWeights, that.classificationWeights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureNames,
            models,
            outputAggregator,
            targetType,
            classificationLabels,
            Arrays.hashCode(classificationWeights));
    }

    @Override
    public void validate() {
        if (outputAggregator.compatibleWith(targetType) == false) {
            throw ExceptionsHelper.badRequestException(
                "aggregate_output [{}] is not compatible with target_type [{}]",
                this.targetType,
                outputAggregator.getName()
            );
        }
        if (outputAggregator.expectedValueSize() != null &&
            outputAggregator.expectedValueSize() != models.size()) {
            throw ExceptionsHelper.badRequestException(
                "[{}] expects value array of size [{}] but number of models is [{}]",
                AGGREGATE_OUTPUT.getPreferredName(),
                outputAggregator.expectedValueSize(),
                models.size());
        }
        if ((this.classificationLabels != null || this.classificationWeights != null) && (this.targetType != TargetType.CLASSIFICATION)) {
            throw ExceptionsHelper.badRequestException(
                "[target_type] should be [classification] if [classification_labels] or [classification_weights] are provided");
        }
        if (classificationWeights != null &&
            classificationLabels != null &&
            classificationWeights.length != classificationLabels.size()) {
            throw ExceptionsHelper.badRequestException(
                "[classification_weights] and [classification_labels] should be the same length if both are provided"
            );
        }
        this.models.forEach(TrainedModel::validate);
    }

    @Override
    public long estimatedNumOperations() {
        OptionalDouble avg = models.stream().mapToLong(TrainedModel::estimatedNumOperations).average();
        assert avg.isPresent() : "unexpected null when calculating number of operations";
        // Average operations for each model and the operations required for processing and aggregating with the outputAggregator
        return (long)Math.ceil(avg.getAsDouble()) + 2 * (models.size() - 1);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOfCollection(featureNames);
        size += RamUsageEstimator.sizeOfCollection(classificationLabels);
        size += RamUsageEstimator.sizeOfCollection(models);
        if (classificationWeights != null) {
            size += RamUsageEstimator.sizeOf(classificationWeights);
        }
        size += outputAggregator.ramBytesUsed();
        return size;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> accountables = new ArrayList<>(models.size() + 1);
        for (TrainedModel model : models) {
            accountables.add(Accountables.namedAccountable(model.getName(), model));
        }
        accountables.add(Accountables.namedAccountable(outputAggregator.getName(), outputAggregator));
        return Collections.unmodifiableCollection(accountables);
    }

    public static class Builder {
        private List<String> featureNames;
        private List<TrainedModel> trainedModels;
        private OutputAggregator outputAggregator = new WeightedSum();
        private TargetType targetType = TargetType.REGRESSION;
        private List<String> classificationLabels;
        private double[] classificationWeights;
        private boolean modelsAreOrdered;

        private Builder (boolean modelsAreOrdered) {
            this.modelsAreOrdered = modelsAreOrdered;
        }

        private static Builder builderForParser() {
            return new Builder(false);
        }

        public Builder() {
            this(true);
        }

        public Builder setFeatureNames(List<String> featureNames) {
            this.featureNames = featureNames;
            return this;
        }

        public Builder setTrainedModels(List<TrainedModel> trainedModels) {
            this.trainedModels = trainedModels;
            return this;
        }

        public Builder setOutputAggregator(OutputAggregator outputAggregator) {
            this.outputAggregator = ExceptionsHelper.requireNonNull(outputAggregator, AGGREGATE_OUTPUT);
            return this;
        }

        public Builder setTargetType(TargetType targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder setClassificationLabels(List<String> classificationLabels) {
            this.classificationLabels = classificationLabels;
            return this;
        }

        public Builder setClassificationWeights(List<Double> classificationWeights) {
            this.classificationWeights = classificationWeights.stream().mapToDouble(Double::doubleValue).toArray();
            return this;
        }

        private void setOutputAggregatorFromParser(List<OutputAggregator> outputAggregators) {
            if (outputAggregators.size() != 1) {
                throw ExceptionsHelper.badRequestException("[{}] must have exactly one aggregator defined.",
                    AGGREGATE_OUTPUT.getPreferredName());
            }
            this.setOutputAggregator(outputAggregators.get(0));
        }

        private void setTargetType(String targetType) {
            this.targetType = TargetType.fromString(targetType);
        }

        private void setModelsAreOrdered(boolean value) {
            this.modelsAreOrdered = value;
        }

        public Ensemble build() {
            // This is essentially a serialization error but the underlying xcontent parsing does not allow us to inject this requirement
            // So, we verify the models were parsed in an ordered fashion here instead.
            if (modelsAreOrdered == false && trainedModels != null && trainedModels.size() > 1) {
                throw ExceptionsHelper.badRequestException("[trained_models] needs to be an array of objects");
            }
            return new Ensemble(featureNames, trainedModels, outputAggregator, targetType, classificationLabels, classificationWeights);
        }
    }
}
