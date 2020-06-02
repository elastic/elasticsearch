/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public final class OptimizableModelTestsUtils {

    private OptimizableModelTestsUtils() {}

    public static void testModelOptimization(BiFunction<TargetType, List<String>, TrainedModel> modelProvider, int numberOfRuns) {
        for (int i = 0; i < numberOfRuns; ++i) {
            TargetType targetType = TargetType.REGRESSION;
            int numberOfFeatures = randomIntBetween(2, 10);
            List<String> featureNames = Stream.generate(
                () -> randomAlphaOfLength(10)).limit(numberOfFeatures).collect(Collectors.toList());
            TrainedModel model = modelProvider.apply(targetType, featureNames);

            InferenceConfig config = RegressionConfig.EMPTY_PARAMS;
            Map<String, Object> fields = randomStringDoubleMap(featureNames);
            InferenceResults unoptimizedResults = model.infer(fields, config, Collections.emptyMap());
            model.optimizeForInference(true, Collections.emptyMap());
            InferenceResults optimizedResults = model.infer(fields, config, Collections.emptyMap());
            assertThat(unoptimizedResults, equalTo(optimizedResults));
        }
    }

    public static void testModelOptimizationsNotTopLevel(BiFunction<TargetType, List<String>, TrainedModel> modelProvider,
                                                         int numberOfRuns) {
        for (int i = 0; i < numberOfRuns; ++i) {
            TargetType targetType = TargetType.REGRESSION;
            int numberOfFeatures = randomIntBetween(2, 10);
            List<String> featureNames = Stream.generate(
                () -> randomAlphaOfLength(10)).limit(numberOfFeatures).collect(Collectors.toList());
            TrainedModel model = modelProvider.apply(targetType, featureNames);

            InferenceConfig config = RegressionConfig.EMPTY_PARAMS;

            Map<String, Object> fields = randomStringDoubleMap(featureNames);
            int fieldIndex = 0;
            Map<String, Integer> fieldIndices = new HashMap<>();
            double[] fieldValues = new double[fields.size()];
            for (Map.Entry<String, Object> field : fields.entrySet()) {
                fieldIndices.put(field.getKey(), fieldIndex);
                fieldValues[fieldIndex++] = InferenceHelpers.toDouble(field.getValue());
            }
            InferenceResults unoptimizedResults = model.infer(fields, config, Collections.emptyMap());
            model.optimizeForInference(false, fieldIndices);
            InferenceResults optimizedResults = model.infer(fieldValues, config);
            assertThat(unoptimizedResults, equalTo(optimizedResults));
        }
    }

    public static Map<String, Object> randomStringDoubleMap(List<String> keys) {
        return keys.stream().collect(Collectors.toMap(Function.identity(), (v) -> randomDouble()));
    }
}
