/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class InferenceHelpers {

    private InferenceHelpers() { }

    /**
     * @return Tuple of the highest scored index and the top classes
     */
    public static Tuple<Integer, List<ClassificationInferenceResults.TopClassEntry>> topClasses(List<Double> probabilities,
                                                                                                List<String> classificationLabels,
                                                                                                @Nullable double[] classificationWeights,
                                                                                                int numToInclude) {

        if (classificationLabels != null && probabilities.size() != classificationLabels.size()) {
            throw ExceptionsHelper
                .serverError(
                    "model returned classification probabilities of size [{}] which is not equal to classification labels size [{}]",
                    null,
                    probabilities.size(),
                    classificationLabels.size());
        }

        List<Double> scores = classificationWeights == null ?
            probabilities :
            IntStream.range(0, probabilities.size())
                .mapToDouble(i -> probabilities.get(i) * classificationWeights[i])
                .boxed()
                .collect(Collectors.toList());

        int[] sortedIndices = IntStream.range(0, probabilities.size())
            .boxed()
            .sorted(Comparator.comparing(scores::get).reversed())
            .mapToInt(i -> i)
            .toArray();

        if (numToInclude == 0) {
            return Tuple.tuple(sortedIndices[0], Collections.emptyList());
        }

        List<String> labels = classificationLabels == null ?
            // If we don't have the labels we should return the top classification values anyways, they will just be numeric
            IntStream.range(0, probabilities.size()).boxed().map(String::valueOf).collect(Collectors.toList()) :
            classificationLabels;

        int count = numToInclude < 0 ? probabilities.size() : Math.min(numToInclude, probabilities.size());
        List<ClassificationInferenceResults.TopClassEntry> topClassEntries = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            int idx = sortedIndices[i];
            topClassEntries.add(new ClassificationInferenceResults.TopClassEntry(labels.get(idx), probabilities.get(idx), scores.get(idx)));
        }

        return Tuple.tuple(sortedIndices[0], topClassEntries);
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
            try {
                return Double.valueOf((String)value);
            } catch (NumberFormatException nfe) {
                assert false : "value is not properly formatted double [" + value + "]";
                return null;
            }
        }
        return null;
    }
}
