/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionFeatureImportance;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public final class InferenceHelpers {

    private InferenceHelpers() {}

    /**
     * @return Tuple of the highest scored index and the top classes
     */
    public static Tuple<TopClassificationValue, List<TopClassEntry>> topClasses(
        double[] probabilities,
        List<String> classificationLabels,
        @Nullable double[] classificationWeights,
        int numToInclude,
        PredictionFieldType predictionFieldType
    ) {

        if (classificationLabels != null && probabilities.length != classificationLabels.size()) {
            throw ExceptionsHelper.serverError(
                "model returned classification probabilities of size [{}] which is not equal to classification labels size [{}]",
                null,
                probabilities.length,
                classificationLabels.size()
            );
        }

        double[] scores = classificationWeights == null
            ? probabilities
            : IntStream.range(0, probabilities.length).mapToDouble(i -> probabilities[i] * classificationWeights[i]).toArray();

        int[] sortedIndices = IntStream.range(0, scores.length)
            .boxed()
            .sorted(Comparator.comparing(i -> scores[(Integer) i]).reversed())
            .mapToInt(i -> i)
            .toArray();

        final TopClassificationValue topClassificationValue = new TopClassificationValue(
            sortedIndices[0],
            probabilities[sortedIndices[0]],
            scores[sortedIndices[0]]
        );
        if (numToInclude == 0) {
            return Tuple.tuple(topClassificationValue, Collections.emptyList());
        }

        List<String> labels = classificationLabels == null ?
        // If we don't have the labels we should return the top classification values anyways, they will just be numeric
            IntStream.range(0, probabilities.length).mapToObj(String::valueOf).toList() : classificationLabels;

        int count = numToInclude < 0 ? probabilities.length : Math.min(numToInclude, probabilities.length);
        List<TopClassEntry> topClassEntries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int idx = sortedIndices[i];
            topClassEntries.add(
                new TopClassEntry(
                    predictionFieldType.transformPredictedValue((double) idx, labels.get(idx)),
                    probabilities[idx],
                    scores[idx]
                )
            );
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
                classificationLabels
            );
        }
        return classificationLabels.get(inferenceValue);
    }

    public static Double toDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String str) {
            return stringToDouble(str);
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

    public static Map<String, double[]> decodeFeatureImportances(
        Map<String, String> processedFeatureToOriginalFeatureMap,
        Map<String, double[]> featureImportances
    ) {
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

    public static List<RegressionFeatureImportance> transformFeatureImportanceRegression(Map<String, double[]> featureImportance) {
        List<RegressionFeatureImportance> importances = new ArrayList<>(featureImportance.size());
        featureImportance.forEach((k, v) -> importances.add(new RegressionFeatureImportance(k, v[0])));
        return importances;
    }

    public static List<ClassificationFeatureImportance> transformFeatureImportanceClassification(
        Map<String, double[]> featureImportance,
        @Nullable List<String> classificationLabels,
        @Nullable PredictionFieldType predictionFieldType
    ) {
        List<ClassificationFeatureImportance> importances = new ArrayList<>(featureImportance.size());
        final PredictionFieldType fieldType = predictionFieldType == null ? PredictionFieldType.STRING : predictionFieldType;
        featureImportance.forEach((k, v) -> {
            // This indicates logistic regression (binary classification)
            // If the length > 1, we assume multi-class classification.
            if (v.length == 1) {
                String zeroLabel = classificationLabels == null ? null : classificationLabels.get(0);
                String oneLabel = classificationLabels == null ? null : classificationLabels.get(1);
                // For feature importance, it is built off of the value in the leaves.
                // These leaves indicate which direction the feature pulls the value
                // The original importance is an indication of how it pushes or pulls the value towards or from `1`
                // To get the importance for the `0` class, we simply invert it.
                importances.add(
                    new ClassificationFeatureImportance(
                        k,
                        Arrays.asList(
                            new ClassificationFeatureImportance.ClassImportance(fieldType.transformPredictedValue(0.0, zeroLabel), -v[0]),
                            new ClassificationFeatureImportance.ClassImportance(fieldType.transformPredictedValue(1.0, oneLabel), v[0])
                        )
                    )
                );
            } else {
                List<ClassificationFeatureImportance.ClassImportance> classImportance = new ArrayList<>(v.length);
                // If the classificationLabels exist, their length must match leaf_value length
                assert classificationLabels == null || classificationLabels.size() == v.length;
                for (int i = 0; i < v.length; i++) {
                    String label = classificationLabels == null ? null : classificationLabels.get(i);
                    classImportance.add(
                        new ClassificationFeatureImportance.ClassImportance(fieldType.transformPredictedValue((double) i, label), v[i])
                    );
                }
                importances.add(new ClassificationFeatureImportance(k, classImportance));
            }
        });
        return importances;
    }

    public static double[] sumDoubleArrays(double[] sumTo, double[] inc) {
        return sumDoubleArrays(sumTo, inc, 1);
    }

    public static double[] sumDoubleArrays(double[] sumTo, double[] inc, int weight) {
        assert sumTo != null && inc != null && sumTo.length == inc.length;
        for (int i = 0; i < inc.length; i++) {
            sumTo[i] += (inc[i] * weight);
        }
        return sumTo;
    }

    public static void divMut(double[] xs, int v) {
        if (xs.length == 0) {
            return;
        }
        if (v == 0) {
            throw new IllegalArgumentException("unable to divide by [" + v + "] as it results in undefined behavior");
        }
        for (int i = 0; i < xs.length; i++) {
            xs[i] /= v;
        }
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
