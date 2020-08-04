/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.ml.inference.results.FeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class InferenceHelpers {

    private InferenceHelpers() { }

    /**
     * @return Tuple of the highest scored index and the top classes
     */
    public static Tuple<TopClassificationValue, List<TopClassEntry>> topClasses(double[] probabilities,
                                                                                List<String> classificationLabels,
                                                                                @Nullable double[] classificationWeights,
                                                                                int numToInclude,
                                                                                PredictionFieldType predictionFieldType) {

        if (classificationLabels != null && probabilities.length != classificationLabels.size()) {
            throw ExceptionsHelper
                .serverError(
                    "model returned classification probabilities of size [{}] which is not equal to classification labels size [{}]",
                    null,
                    probabilities.length,
                    classificationLabels.size());
        }

        double[] scores = classificationWeights == null ?
            probabilities :
            IntStream.range(0, probabilities.length)
                .mapToDouble(i -> probabilities[i] * classificationWeights[i])
                .toArray();

        int[] sortedIndices = IntStream.range(0, scores.length)
            .boxed()
            .sorted(Comparator.comparing(i -> scores[(Integer)i]).reversed())
            .mapToInt(i -> i)
            .toArray();

        final TopClassificationValue topClassificationValue = new TopClassificationValue(sortedIndices[0],
            probabilities[sortedIndices[0]],
            scores[sortedIndices[0]]);
        if (numToInclude == 0) {
            return Tuple.tuple(topClassificationValue, Collections.emptyList());
        }

        List<String> labels = classificationLabels == null ?
            // If we don't have the labels we should return the top classification values anyways, they will just be numeric
            IntStream.range(0, probabilities.length).boxed().map(String::valueOf).collect(Collectors.toList()) :
            classificationLabels;

        int count = numToInclude < 0 ? probabilities.length : Math.min(numToInclude, probabilities.length);
        List<TopClassEntry> topClassEntries = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            int idx = sortedIndices[i];
            topClassEntries.add(new TopClassEntry(
                predictionFieldType.transformPredictedValue((double)idx, labels.get(idx)),
                probabilities[idx],
                scores[idx]));
        }

        return Tuple.tuple(topClassificationValue, topClassEntries);
    }

    public static String classificationLabel(Integer inferenceValue, @Nullable List<String> classificationLabels) {
        if (classificationLabels == null) {
            return String.valueOf(inferenceValue);
        }
        if (inferenceValue < 0 || inferenceValue >= classificationLabels.size()) {
            throw ExceptionsHelper.serverError(
                "model returned classification value of [{}] which is not a valid index in classification labels [{}]",
                null,
                inferenceValue,
                classificationLabels);
        }
        return classificationLabels.get(inferenceValue);
    }

    public static Double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number)value).doubleValue();
        }
        if (value instanceof String) {
            return stringToDouble((String) value);
        }
        return null;
    }

    private static Double stringToDouble(String value) {
        if (value.isEmpty()) {
            return null;
        }
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException nfe) {
            assert false : "value is not properly formatted double [" + value + "]";
            return null;
        }
    }

    public static Map<String, double[]> decodeFeatureImportances(Map<String, String> processedFeatureToOriginalFeatureMap,
                                                                 Map<String, double[]> featureImportances) {
        if (processedFeatureToOriginalFeatureMap == null || processedFeatureToOriginalFeatureMap.isEmpty()) {
            return featureImportances;
        }

        Map<String, double[]> originalFeatureImportance = new HashMap<>();
        featureImportances.forEach((feature, importance) -> {
            String featureName = processedFeatureToOriginalFeatureMap.getOrDefault(feature, feature);
            originalFeatureImportance.compute(featureName, (f, v1) -> v1 == null ? importance : sumDoubleArrays(importance, v1));
        });
        return originalFeatureImportance;
    }

    public static List<FeatureImportance> transformFeatureImportance(Map<String, double[]> featureImportance,
                                                                     @Nullable List<String> classificationLabels) {
        List<FeatureImportance> importances = new ArrayList<>(featureImportance.size());
        featureImportance.forEach((k, v) -> {
            // This indicates regression, or logistic regression
            // If the length > 1, we assume multi-class classification.
            if (v.length == 1) {
                importances.add(FeatureImportance.forRegression(k, v[0]));
            } else {
                Map<String, Double> classImportance = new LinkedHashMap<>(v.length, 1.0f);
                // If the classificationLabels exist, their length must match leaf_value length
                assert classificationLabels == null || classificationLabels.size() == v.length;
                for (int i = 0; i < v.length; i++) {
                    classImportance.put(classificationLabels == null ? String.valueOf(i) : classificationLabels.get(i), v[i]);
                }
                importances.add(FeatureImportance.forClassification(k, classImportance));
            }
        });
        return importances;
    }

    public static double[] sumDoubleArrays(double[] sumTo, double[] inc) {
        assert sumTo != null && inc != null && sumTo.length == inc.length;
        for (int i = 0; i < inc.length; i++) {
            sumTo[i] += inc[i];
        }
        return sumTo;
    }

    public static class TopClassificationValue {
        private final int value;
        private final double probability;
        private final double score;

        TopClassificationValue(int value, double probability, double score) {
            this.value = value;
            this.probability = probability;
            this.score = score;
        }

        public int getValue() {
            return value;
        }

        public double getProbability() {
            return probability;
        }

        public double getScore() {
            return score;
        }
    }
}
