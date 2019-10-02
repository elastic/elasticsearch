/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Ensemble implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel {

    // TODO should we have regression/classification sub-classes that accept the builder?
    public static final ParseField NAME = new ParseField("ensemble");
    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField TRAINED_MODELS = new ParseField("trained_models");
    public static final ParseField AGGREGATE_OUTPUT  = new ParseField("aggregate_output");
    public static final ParseField TARGET_TYPE = new ParseField("target_type");
    public static final ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");

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

    Ensemble(List<String> featureNames,
             List<TrainedModel> models,
             OutputAggregator outputAggregator,
             TargetType targetType,
             @Nullable List<String> classificationLabels) {
        this.featureNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(featureNames, FEATURE_NAMES));
        this.models = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(models, TRAINED_MODELS));
        this.outputAggregator = ExceptionsHelper.requireNonNull(outputAggregator, AGGREGATE_OUTPUT);
        this.targetType = ExceptionsHelper.requireNonNull(targetType, TARGET_TYPE);
        this.classificationLabels = classificationLabels == null ? null : Collections.unmodifiableList(classificationLabels);
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
    }

    @Override
    public List<String> getFeatureNames() {
        return featureNames;
    }

    @Override
    public double infer(Map<String, Object> fields) {
        List<Double> features = featureNames.stream().map(f -> (Double) fields.get(f)).collect(Collectors.toList());
        return infer(features);
    }

    @Override
    public double infer(List<Double> fields) {
        List<Double> processedInferences = inferAndProcess(fields);
        return outputAggregator.aggregate(processedInferences);
    }

    @Override
    public TargetType targetType() {
        return targetType;
    }

    @Override
    public List<Double> classificationProbability(Map<String, Object> fields) {
        if ((targetType == TargetType.CLASSIFICATION) == false) {
            throw new UnsupportedOperationException(
                "Cannot determine classification probability with target_type [" + targetType.toString() + "]");
        }
        List<Double> features = featureNames.stream().map(f -> (Double) fields.get(f)).collect(Collectors.toList());
        return classificationProbability(features);
    }

    @Override
    public List<Double> classificationProbability(List<Double> fields) {
        if ((targetType == TargetType.CLASSIFICATION) == false) {
            throw new UnsupportedOperationException(
                "Cannot determine classification probability with target_type [" + targetType.toString() + "]");
        }
        return inferAndProcess(fields);
    }

    @Override
    public List<String> classificationLabels() {
        return classificationLabels;
    }

    private List<Double> inferAndProcess(List<Double> fields) {
        List<Double> modelInferences = models.stream().map(m -> m.infer(fields)).collect(Collectors.toList());
        return outputAggregator.processValues(modelInferences);
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
            && Objects.equals(outputAggregator, that.outputAggregator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureNames, models, outputAggregator, targetType, classificationLabels);
    }

    @Override
    public void validate() {
        if (this.featureNames != null) {
            if (this.models.stream()
                .anyMatch(trainedModel -> trainedModel.getFeatureNames().equals(this.featureNames) == false)) {
                throw ExceptionsHelper.badRequestException(
                    "[{}] must be the same and in the same order for each of the {}",
                    FEATURE_NAMES.getPreferredName(),
                    TRAINED_MODELS.getPreferredName());
            }
        }
        if (outputAggregator.expectedValueSize() != null &&
            outputAggregator.expectedValueSize() != models.size()) {
            throw ExceptionsHelper.badRequestException(
                "[{}] expects value array of size [{}] but number of models is [{}]",
                AGGREGATE_OUTPUT.getPreferredName(),
                outputAggregator.expectedValueSize(),
                models.size());
        }
        if ((this.targetType == TargetType.CLASSIFICATION) != (this.classificationLabels != null)) {
            throw ExceptionsHelper.badRequestException(
                "[target_type] should be [classification] if [classification_labels] is provided, and vice versa");
        }
        this.models.forEach(TrainedModel::validate);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> featureNames;
        private List<TrainedModel> trainedModels;
        private OutputAggregator outputAggregator = new WeightedSum();
        private TargetType targetType = TargetType.REGRESSION;
        private List<String> classificationLabels;
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
            return new Ensemble(featureNames, trainedModels, outputAggregator, targetType, classificationLabels);
        }
    }
}
