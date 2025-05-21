/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceModelTestUtils.deserializeFromTrainedModel;
import static org.hamcrest.Matchers.equalTo;

public class BoundedWindowInferenceModelTests extends ESTestCase {

    private static final List<String> featureNames = Arrays.asList("foo", "bar");

    public void testBoundsSetting() throws IOException {
        BoundedWindowInferenceModel testModel = getModel(-2.0, 5.2, 10.5);
        assertThat(testModel.getMinPredictedValue(), equalTo(-2.0));
        assertThat(testModel.getMaxPredictedValue(), equalTo(10.5));
    }

    public void testInferenceScoresWithoutAdjustment() throws IOException {
        BoundedWindowInferenceModel testModel = getModel(1.0, 5.2, 10.5);

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        Double lowResultValue = ((SingleValueInferenceResults) testModel.infer(
            featureMap,
            RegressionConfig.EMPTY_PARAMS,
            Collections.emptyMap()
        )).value();
        assertThat(lowResultValue, equalTo(1.0));

        featureVector = Arrays.asList(12.0, 0.0);
        featureMap = zipObjMap(featureNames, featureVector);
        Double highResultValue = ((SingleValueInferenceResults) testModel.infer(
            featureMap,
            RegressionConfig.EMPTY_PARAMS,
            Collections.emptyMap()
        )).value();
        assertThat(highResultValue, equalTo(10.5));

        double[] featureArray = new double[2];
        featureArray[0] = 12.0;
        featureArray[1] = 0.0;
        Double highResultValueFromFeatures = ((SingleValueInferenceResults) testModel.infer(featureArray, RegressionConfig.EMPTY_PARAMS))
            .value();
        assertThat(highResultValueFromFeatures, equalTo(10.5));
    }

    public void testInferenceScoresWithAdjustment() throws IOException {
        BoundedWindowInferenceModel testModel = getModel(-5.0, 1.2, 6.5);

        List<Double> featureVector = Arrays.asList(-10.0, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        Double lowResultValue = ((SingleValueInferenceResults) testModel.infer(
            featureMap,
            RegressionConfig.EMPTY_PARAMS,
            Collections.emptyMap()
        )).value();
        assertThat(lowResultValue, equalTo(0.0));

        featureVector = Arrays.asList(12.0, 0.0);
        featureMap = zipObjMap(featureNames, featureVector);
        Double highResultValue = ((SingleValueInferenceResults) testModel.infer(
            featureMap,
            RegressionConfig.EMPTY_PARAMS,
            Collections.emptyMap()
        )).value();
        assertThat(highResultValue, equalTo(11.5));

        double[] featureArray = new double[2];
        featureArray[0] = 12.0;
        featureArray[1] = 0.0;
        Double highResultValueFromFeatures = ((SingleValueInferenceResults) testModel.infer(featureArray, RegressionConfig.EMPTY_PARAMS))
            .value();
        assertThat(highResultValueFromFeatures, equalTo(11.5));
    }

    private BoundedWindowInferenceModel getModel(double lowerBoundValue, double midValue, double upperBoundValue) throws IOException {
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), upperBoundValue);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), lowerBoundValue);
        builder.addLeaf(leftChildNode.getRightChild(), midValue);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = builder.setFeatureNames(featureNames).build();
        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        return new BoundedWindowInferenceModel(tree);
    }

    private static Map<String, Object> zipObjMap(List<String> keys, List<Double> values) {
        return IntStream.range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }

}
