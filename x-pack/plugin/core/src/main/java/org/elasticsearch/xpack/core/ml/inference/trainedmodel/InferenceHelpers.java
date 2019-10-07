package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

        List<String> labels = classificationLabels == null ?
            // If we don't have the labels we should return the top classification values anyways, they will just be numeric
            IntStream.range(0, probabilities.size()).boxed().map(String::valueOf).collect(Collectors.toList()) :
            classificationLabels;

        if (probabilities.size() != labels.size()) {
            throw ExceptionsHelper
                .badRequestException(
                    "model returned classification probabilities of size [{}] which is not equal to classification labels size [{}]",
                    probabilities.size(),
                    classificationLabels);
        }


        int count = numToInclude < 0 ? probabilities.size() : numToInclude;
        List<ClassificationInferenceResults.TopClassEntry> topClassEntries = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            int idx = sortedIndices[i];
            topClassEntries.add(new ClassificationInferenceResults.TopClassEntry(labels.get(idx), probabilities.get(idx)));
        }

        return topClassEntries;
    }
}
