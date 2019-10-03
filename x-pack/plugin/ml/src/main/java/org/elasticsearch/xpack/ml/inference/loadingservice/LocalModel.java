/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.action.ClassificationInferenceResults;
import org.elasticsearch.xpack.ml.inference.action.InferenceResults;
import org.elasticsearch.xpack.ml.inference.action.RegressionInferenceResults;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LocalModel implements Model {

    private final TrainedModelDefinition trainedModelDefinition;
    private final String modelId;
    public LocalModel(String modelId, TrainedModelDefinition trainedModelDefinition) {
        this.trainedModelDefinition = trainedModelDefinition;
        this.modelId = modelId;
    }

    @Override
    public String getResultsType() {
        switch (trainedModelDefinition.getTrainedModel().targetType()) {
            case CLASSIFICATION:
                return ClassificationInferenceResults.RESULT_TYPE;
            case REGRESSION:
                return RegressionInferenceResults.RESULT_TYPE;
            default:
                throw ExceptionsHelper.badRequestException("Model [{}] has unsupported target type [{}]",
                    modelId,
                    trainedModelDefinition.getTrainedModel().targetType());
        }
    }

    @Override
    public void infer(Map<String, Object> fields, ActionListener<InferenceResults<?>> listener) {
        trainedModelDefinition.getPreProcessors().forEach(preProcessor -> preProcessor.process(fields));
        double value = trainedModelDefinition.getTrainedModel().infer(fields);
        InferenceResults<?> inferenceResults;
        if (trainedModelDefinition.getTrainedModel().targetType() == TargetType.CLASSIFICATION) {
            String classificationLabel = null;
            if (trainedModelDefinition.getTrainedModel().classificationLabels() != null) {
                assert value == Math.rint(value);
                int classIndex = Double.valueOf(value).intValue();
                if (classIndex < 0 || classIndex >= trainedModelDefinition.getTrainedModel().classificationLabels().size()) {
                    listener.onFailure(new ElasticsearchStatusException(
                        "model returned classification [{}] which is invalid given labels {}",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        classIndex,
                        trainedModelDefinition.getTrainedModel().classificationLabels()));
                    return;
                }
                classificationLabel = trainedModelDefinition.getTrainedModel().classificationLabels().get(classIndex);
            }
            inferenceResults = new ClassificationInferenceResults(value, classificationLabel, null);
        } else {
            inferenceResults = new RegressionInferenceResults(value);
        }
        listener.onResponse(inferenceResults);
    }

    @Override
    public void classificationProbability(Map<String, Object> fields, int topN, ActionListener<InferenceResults<?>> listener) {
        if (topN == 0) {
            infer(fields, listener);
            return;
        }
        if (trainedModelDefinition.getTrainedModel().targetType() != TargetType.CLASSIFICATION) {
            listener.onFailure(ExceptionsHelper
                .badRequestException("top result probabilities is only available for classification models"));
            return;
        }
        trainedModelDefinition.getPreProcessors().forEach(preProcessor -> preProcessor.process(fields));
        List<Double> probabilities = trainedModelDefinition.getTrainedModel().classificationProbability(fields);
        int[] sortedIndices = IntStream.range(0, probabilities.size())
            .boxed()
            .sorted(Comparator.comparing(probabilities::get).reversed())
            .mapToInt(i -> i)
            .toArray();
        if (trainedModelDefinition.getTrainedModel().classificationLabels() != null) {
            if (probabilities.size() != trainedModelDefinition.getTrainedModel().classificationLabels().size()) {
                listener.onFailure(ExceptionsHelper
                    .badRequestException(
                        "model returned classification probabilities of size [{}] which is not equal to classification labels size [{}]",
                        probabilities.size(),
                        trainedModelDefinition.getTrainedModel().classificationLabels()));
                return;
            }
        }
        List<String> labels = trainedModelDefinition.getTrainedModel().classificationLabels() == null ?
            // If we don't have the labels we should return the top classification values anyways, they will just be numeric
            IntStream.range(0, probabilities.size()).boxed().map(String::valueOf).collect(Collectors.toList()) :
            trainedModelDefinition.getTrainedModel().classificationLabels();

        int count = topN < 0 ? probabilities.size() : topN;
        List<ClassificationInferenceResults.TopClassEntry> topClassEntries = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            int idx = sortedIndices[i];
            topClassEntries.add(new ClassificationInferenceResults.TopClassEntry(labels.get(idx), probabilities.get(idx)));
        }

        listener.onResponse(new ClassificationInferenceResults(((Number)sortedIndices[0]).doubleValue(),
            trainedModelDefinition.getTrainedModel().classificationLabels() == null ?
                null :
                trainedModelDefinition.getTrainedModel().classificationLabels().get(sortedIndices[0]),
            topClassEntries));
    }
}
