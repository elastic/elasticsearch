/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RawInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NullInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.LenientlyParsedOutputAggregator;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.OutputAggregator;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.classificationLabel;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.decodeFeatureImportances;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.sumDoubleArrays;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.transformFeatureImportanceClassification;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.transformFeatureImportanceRegression;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble.AGGREGATE_OUTPUT;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble.CLASSIFICATION_LABELS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble.CLASSIFICATION_WEIGHTS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble.TRAINED_MODELS;

public class EnsembleInferenceModel implements InferenceModel, BoundedInferenceModel {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(EnsembleInferenceModel.class);
    private static final Logger LOGGER = LogManager.getLogger(EnsembleInferenceModel.class);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<EnsembleInferenceModel, Void> PARSER = new ConstructingObjectParser<>(
        "ensemble_inference_model",
        true,
        a -> new EnsembleInferenceModel(
            (List<InferenceModel>) a[0],
            (OutputAggregator) a[1],
            TargetType.fromString((String) a[2]),
            (List<String>) a[3],
            (List<Double>) a[4]
        )
    );
    static {
        PARSER.declareNamedObjects(
            constructorArg(),
            (p, c, n) -> p.namedObject(InferenceModel.class, n, null),
            (ensembleBuilder) -> {},
            TRAINED_MODELS
        );
        PARSER.declareNamedObject(
            constructorArg(),
            (p, c, n) -> p.namedObject(LenientlyParsedOutputAggregator.class, n, null),
            AGGREGATE_OUTPUT
        );
        PARSER.declareString(constructorArg(), TargetType.TARGET_TYPE);
        PARSER.declareStringArray(optionalConstructorArg(), CLASSIFICATION_LABELS);
        PARSER.declareDoubleArray(optionalConstructorArg(), CLASSIFICATION_WEIGHTS);
    }

    public static EnsembleInferenceModel fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private String[] featureNames = new String[0];
    private final List<InferenceModel> models;
    private final OutputAggregator outputAggregator;
    private final TargetType targetType;
    private final List<String> classificationLabels;
    private final double[] classificationWeights;
    private volatile boolean preparedForInference = false;
    private final Supplier<double[]> predictedValuesBoundariesSupplier;

    private EnsembleInferenceModel(
        List<InferenceModel> models,
        OutputAggregator outputAggregator,
        TargetType targetType,
        @Nullable List<String> classificationLabels,
        List<Double> classificationWeights
    ) {
        this.models = ExceptionsHelper.requireNonNull(models, TRAINED_MODELS);
        this.outputAggregator = ExceptionsHelper.requireNonNull(outputAggregator, AGGREGATE_OUTPUT);
        this.targetType = ExceptionsHelper.requireNonNull(targetType, TargetType.TARGET_TYPE);
        this.classificationLabels = classificationLabels;
        this.classificationWeights = classificationWeights == null
            ? null
            : classificationWeights.stream().mapToDouble(Double::doubleValue).toArray();
        this.predictedValuesBoundariesSupplier = CachedSupplier.wrap(this::initModelBoundaries);
    }

    @Override
    public String[] getFeatureNames() {
        return featureNames;
    }

    @Override
    public TargetType targetType() {
        return targetType;
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config, Map<String, String> featureDecoderMap) {
        return innerInfer(InferenceModel.extractFeatures(featureNames, fields), config, featureDecoderMap);
    }

    @Override
    public InferenceResults infer(double[] features, InferenceConfig config) {
        return innerInfer(features, config, Collections.emptyMap());
    }

    private InferenceResults innerInfer(double[] features, InferenceConfig config, Map<String, String> featureDecoderMap) {
        if (config.isTargetTypeSupported(targetType) == false) {
            throw ExceptionsHelper.badRequestException(
                "Cannot infer using configuration for [{}] when model target_type is [{}]",
                config.getName(),
                targetType.toString()
            );
        }
        if (preparedForInference == false) {
            throw ExceptionsHelper.serverError("model is not prepared for inference");
        }
        LOGGER.debug(
            () -> "Inference called with feature names ["
                + Strings.arrayToCommaDelimitedString(featureNames)
                + "] values "
                + Arrays.toString(features)
        );
        double[][] inferenceResults = new double[this.models.size()][];
        double[][] featureInfluence = new double[features.length][];
        int i = 0;
        NullInferenceConfig subModelInferenceConfig = new NullInferenceConfig(config.requestingImportance());
        for (InferenceModel model : models) {
            InferenceResults result = model.infer(features, subModelInferenceConfig);
            assert result instanceof RawInferenceResults;
            RawInferenceResults inferenceResult = (RawInferenceResults) result;
            inferenceResults[i++] = inferenceResult.getValue();
            if (config.requestingImportance()) {
                addFeatureImportance(featureInfluence, inferenceResult);
            }
        }
        double[] processed = outputAggregator.processValues(inferenceResults);
        return buildResults(processed, featureInfluence, featureDecoderMap, config);
    }

    // For testing
    double[][] featureImportance(double[] features) {
        double[][] featureInfluence = new double[features.length][];
        NullInferenceConfig subModelInferenceConfig = new NullInferenceConfig(true);
        for (InferenceModel model : models) {
            InferenceResults result = model.infer(features, subModelInferenceConfig);
            assert result instanceof RawInferenceResults;
            RawInferenceResults inferenceResult = (RawInferenceResults) result;
            addFeatureImportance(featureInfluence, inferenceResult);
        }
        return featureInfluence;
    }

    private static void addFeatureImportance(double[][] featureInfluence, RawInferenceResults inferenceResult) {
        double[][] modelFeatureImportance = inferenceResult.getFeatureImportance();
        assert modelFeatureImportance.length == featureInfluence.length;
        for (int j = 0; j < modelFeatureImportance.length; j++) {
            if (featureInfluence[j] == null) {
                featureInfluence[j] = new double[modelFeatureImportance[j].length];
            }
            featureInfluence[j] = sumDoubleArrays(featureInfluence[j], modelFeatureImportance[j]);
        }
    }

    private InferenceResults buildResults(
        double[] processedInferences,
        double[][] featureImportance,
        Map<String, String> featureDecoderMap,
        InferenceConfig config
    ) {
        // Indicates that the config is useless and the caller just wants the raw value
        if (config instanceof NullInferenceConfig) {
            return new RawInferenceResults(new double[] { outputAggregator.aggregate(processedInferences) }, featureImportance);
        }
        Map<String, double[]> decodedFeatureImportance = config.requestingImportance()
            ? decodeFeatureImportances(
                featureDecoderMap,
                IntStream.range(0, featureImportance.length)
                    .boxed()
                    .collect(Collectors.toMap(i -> featureNames[i], i -> featureImportance[i]))
            )
            : Collections.emptyMap();
        switch (targetType) {
            case REGRESSION:
                return new RegressionInferenceResults(
                    outputAggregator.aggregate(processedInferences),
                    config,
                    transformFeatureImportanceRegression(decodedFeatureImportance)
                );
            case CLASSIFICATION:
                ClassificationConfig classificationConfig = (ClassificationConfig) config;
                assert classificationWeights == null || processedInferences.length == classificationWeights.length;
                // Adjust the probabilities according to the thresholds
                Tuple<InferenceHelpers.TopClassificationValue, List<TopClassEntry>> topClasses = InferenceHelpers.topClasses(
                    processedInferences,
                    classificationLabels,
                    classificationWeights,
                    classificationConfig.getNumTopClasses(),
                    classificationConfig.getPredictionFieldType()
                );
                final InferenceHelpers.TopClassificationValue value = topClasses.v1();
                return new ClassificationInferenceResults(
                    value.getValue(),
                    classificationLabel(topClasses.v1().getValue(), classificationLabels),
                    topClasses.v2(),
                    transformFeatureImportanceClassification(
                        decodedFeatureImportance,
                        classificationLabels,
                        classificationConfig.getPredictionFieldType()
                    ),
                    config,
                    value.getProbability(),
                    value.getScore()
                );
            default:
                throw new UnsupportedOperationException("unsupported target_type [" + targetType + "] for inference on ensemble model");
        }
    }

    @Override
    public boolean supportsFeatureImportance() {
        return models.stream().allMatch(InferenceModel::supportsFeatureImportance);
    }

    @Override
    public String getName() {
        return "ensemble";
    }

    @Override
    public void rewriteFeatureIndices(final Map<String, Integer> newFeatureIndexMapping) {
        LOGGER.debug(() -> format("rewriting features %s", newFeatureIndexMapping));
        if (preparedForInference) {
            return;
        }
        preparedForInference = true;
        Map<String, Integer> featureIndexMapping = new HashMap<>();
        if (newFeatureIndexMapping == null || newFeatureIndexMapping.isEmpty()) {
            Set<String> referencedFeatures = subModelFeatures();
            LOGGER.debug(() -> format("detected submodel feature names %s", referencedFeatures));
            int newFeatureIndex = 0;
            featureIndexMapping = new HashMap<>();
            this.featureNames = new String[referencedFeatures.size()];
            for (String featureName : referencedFeatures) {
                featureIndexMapping.put(featureName, newFeatureIndex);
                this.featureNames[newFeatureIndex++] = featureName;
            }
        } else {
            this.featureNames = new String[0];
        }
        for (InferenceModel model : models) {
            model.rewriteFeatureIndices(featureIndexMapping);
        }
    }

    private Set<String> subModelFeatures() {
        Set<String> referencedFeatures = new LinkedHashSet<>();
        for (InferenceModel model : models) {
            if (model instanceof EnsembleInferenceModel ensembleInferenceModel) {
                referencedFeatures.addAll(ensembleInferenceModel.subModelFeatures());
            } else {
                for (String featureName : model.getFeatureNames()) {
                    referencedFeatures.add(featureName);
                }
            }
        }
        return referencedFeatures;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(featureNames);
        size += RamUsageEstimator.sizeOfCollection(classificationLabels);
        size += RamUsageEstimator.sizeOfCollection(models);
        if (classificationWeights != null) {
            size += RamUsageEstimator.sizeOf(classificationWeights);
        }
        size += outputAggregator.ramBytesUsed();
        return size;
    }

    public List<InferenceModel> getModels() {
        return models;
    }

    public OutputAggregator getOutputAggregator() {
        return outputAggregator;
    }

    public TargetType getTargetType() {
        return targetType;
    }

    public double[] getClassificationWeights() {
        return classificationWeights;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("EnsembleInferenceModel{");

        builder.append("featureNames=")
            .append(Arrays.toString(featureNames))
            .append(", models=")
            .append(models)
            .append(", outputAggregator=")
            .append(outputAggregator)
            .append(", targetType=")
            .append(targetType);

        if (targetType == TargetType.CLASSIFICATION) {
            builder.append(", classificationLabels=")
                .append(classificationLabels)
                .append(", classificationWeights=")
                .append(Arrays.toString(classificationWeights));
        } else if (targetType == TargetType.REGRESSION) {
            builder.append(", minPredictedValue=")
                .append(getMinPredictedValue())
                .append(", maxPredictedValue=")
                .append(getMaxPredictedValue());
        }

        builder.append(", preparedForInference=").append(preparedForInference);

        return builder.append('}').toString();
    }

    @Override
    public double getMinPredictedValue() {
        return this.predictedValuesBoundariesSupplier.get()[0];
    }

    @Override
    public double getMaxPredictedValue() {
        return this.predictedValuesBoundariesSupplier.get()[1];
    }

    private double[] initModelBoundaries() {
        double[] modelsMinBoundaries = new double[models.size()];
        double[] modelsMaxBoundaries = new double[models.size()];
        int i = 0;
        for (InferenceModel model : models) {
            if (model instanceof BoundedInferenceModel boundedInferenceModel) {
                modelsMinBoundaries[i] = boundedInferenceModel.getMinPredictedValue();
                modelsMaxBoundaries[i++] = boundedInferenceModel.getMaxPredictedValue();
            } else {
                throw new IllegalStateException("All submodels have to be bounded");
            }
        }

        return new double[] { outputAggregator.aggregate(modelsMinBoundaries), outputAggregator.aggregate(modelsMaxBoundaries) };
    }
}
