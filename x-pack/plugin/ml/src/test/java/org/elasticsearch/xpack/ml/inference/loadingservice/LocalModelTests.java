/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class LocalModelTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testClassificationInfer() throws Exception {
        TrainedModelDefinition definition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildClassification(false))
            .build();

        Model model = new LocalModel(definition);
        Map<String, Object> fields = new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }};

        PlainActionFuture<Object> future = new PlainActionFuture<>();
        model.infer(fields, future);
        assertThat(future.get(), equalTo(0.0));

        future = new PlainActionFuture<>();
        model.confidence(fields, 0, future);
        assertThat(future.get(), equalTo(Collections.emptyMap()));

        future = new PlainActionFuture<>();
        model.confidence(fields, 1, future);
        assertThat(((Map<String, Double>)future.get()).get("0"), closeTo(0.5498339973124778, 0.0000001));

        // Test with labels
        definition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildClassification(true))
            .build();
        model = new LocalModel(definition);
        future = new PlainActionFuture<>();
        model.infer(fields, future);
        assertThat(future.get(), equalTo("not_to_be"));

        future = new PlainActionFuture<>();
        model.confidence(fields, 0, future);
        assertThat(future.get(), equalTo(Collections.emptyMap()));

        future = new PlainActionFuture<>();
        model.confidence(fields, 1, future);
        assertThat(((Map<String, Double>)future.get()).get("not_to_be"), closeTo(0.5498339973124778, 0.0000001));

        future = new PlainActionFuture<>();
        model.confidence(fields, 2, future);
        assertThat((Map<String, Double>)future.get(), aMapWithSize(2));

        future = new PlainActionFuture<>();
        model.confidence(fields, -1, future);
        assertThat((Map<String, Double>)future.get(), aMapWithSize(2));
    }

    public void testRegression() throws Exception {
        TrainedModelDefinition trainedModelDefinition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildRegression())
            .build();
        Model model = new LocalModel(trainedModelDefinition);

        Map<String, Object> fields = new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }};

        PlainActionFuture<Object> future = new PlainActionFuture<>();
        model.infer(fields, future);
        assertThat(future.get(), equalTo(1.3));

        PlainActionFuture<Object> failedFuture = new PlainActionFuture<>();
        model.confidence(fields, -1, failedFuture);
        ExecutionException ex = expectThrows(ExecutionException.class, failedFuture::get);
        assertThat(ex.getCause().getMessage(), equalTo("top result probabilities is only available for classification models"));
    }

    private static Map<String, String> oneHotMap() {
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        return oneHotEncoding;
    }

    private static TrainedModel buildClassification(boolean includeLabels) {
        List<String> featureNames = Arrays.asList("foo", "bar", "animal_cat", "animal_dog");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.8)
                .setSplitFeature(1)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0)).build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(3)
                .setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(0.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .build();
        return Ensemble.builder()
            .setClassificationLabels(includeLabels ? Arrays.asList("not_to_be", "to_be") : null)
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(Arrays.asList(0.7, 0.5, 1.0)))
            .build();
    }

    private static TrainedModel buildRegression() {
        List<String> featureNames = Arrays.asList("foo", "bar", "animal_cat", "animal_dog");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(0)
                .setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.3))
            .addNode(TreeNode.builder(2)
                .setThreshold(0.0)
                .setSplitFeature(3)
                .setLeftChild(3)
                .setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.1))
            .addNode(TreeNode.builder(4).setLeafValue(0.2)).build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(2)
                .setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0)
                .setLeftChild(1)
                .setRightChild(2)
                .setSplitFeature(1)
                .setThreshold(0.2))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        return Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedSum(Arrays.asList(0.5, 0.5, 0.5)))
            .build();
    }

}
