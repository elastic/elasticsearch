/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Nullable;
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

    public static List<ClassificationInferenceResults.TopClassEntry> topClasses(List<Double> probabilities,
                                                                                List<String> classificationLabels,
                                                                                int numToInclude) {
        if (numToInclude == 0) {
            return Collections.emptyList();
        }
        int[] sortedIndices = IntStream.range(0, probabilities.size())
            .boxed()
            .sorted(Comparator.comparing(probabilities::get).reversed())
            .mapToInt(i -> i)
            .toArray();

        if (classificationLabels != null && probabilities.size() != classificationLabels.size()) {
            throw ExceptionsHelper
                .serverError(
                    "model returned classification probabilities of size [{}] which is not equal to classification labels size [{}]",
                    null,
                    probabilities.size(),
                    classificationLabels);
        }

        List<String> labels = classificationLabels == null ?
            // If we don't have the labels we should return the top classification values anyways, they will just be numeric
            IntStream.range(0, probabilities.size()).boxed().map(String::valueOf).collect(Collectors.toList()) :
            classificationLabels;

        int count = numToInclude < 0 ? probabilities.size() : Math.min(numToInclude, probabilities.size());
        List<ClassificationInferenceResults.TopClassEntry> topClassEntries = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            int idx = sortedIndices[i];
            topClassEntries.add(new ClassificationInferenceResults.TopClassEntry(labels.get(idx), probabilities.get(idx)));
        }

        return topClassEntries;
    }

    public static String classificationLabel(double inferenceValue, @Nullable List<String> classificationLabels) {
        assert inferenceValue == Math.rint(inferenceValue);
        if (classificationLabels == null) {
            return String.valueOf(inferenceValue);
        }
        int label = Double.valueOf(inferenceValue).intValue();
        if (label < 0 || label >= classificationLabels.size()) {
            throw ExceptionsHelper.serverError(
                "model returned classification value of [{}] which is not a valid index in classification labels [{}]",
                null,
                label,
                classificationLabels);
        }
        return classificationLabels.get(label);
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
