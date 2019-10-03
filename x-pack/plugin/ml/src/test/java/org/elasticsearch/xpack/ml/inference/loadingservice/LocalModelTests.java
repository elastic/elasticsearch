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
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class LocalModelTests extends ESTestCase {

    public void testClassificationInfer() throws Exception {
        String modelId = "classification_model";
        TrainedModelDefinition definition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildClassification(false))
            .build();

        Model model = new LocalModel(modelId, definition);
        Map<String, Object> fields = new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }};

        PlainActionFuture<InferenceResults<?>> future = new PlainActionFuture<>();
        model.infer(fields, future);
        InferenceResults<?> result = future.get();
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), is("0.0"));

        future = new PlainActionFuture<>();
        model.classificationProbability(fields, 0, future);
        result = future.get();
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), is("0.0"));

        future = new PlainActionFuture<>();
        model.classificationProbability(fields, 1, future);
        ClassificationInferenceResults classificationResult = (ClassificationInferenceResults)future.get();
        assertThat(classificationResult.getTopClasses().get(0).getProbability(), closeTo(0.5498339973124778, 0.0000001));
        assertThat(classificationResult.getTopClasses().get(0).getLabel(), equalTo("0"));

        // Test with labels
        definition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildClassification(true))
            .build();
        model = new LocalModel(modelId, definition);
        future = new PlainActionFuture<>();
        model.infer(fields, future);
        result = future.get();
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), equalTo("not_to_be"));

        future = new PlainActionFuture<>();
        model.classificationProbability(fields, 0, future);
        result = future.get();
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), equalTo("not_to_be"));

        future = new PlainActionFuture<>();
        model.classificationProbability(fields, 1, future);
        result = future.get();
        classificationResult = (ClassificationInferenceResults)result;
        assertThat(classificationResult.getTopClasses().get(0).getProbability(), closeTo(0.5498339973124778, 0.0000001));
        assertThat(classificationResult.getTopClasses().get(0).getLabel(), equalTo("not_to_be"));

        future = new PlainActionFuture<>();
        model.classificationProbability(fields, 2, future);
        result = future.get();
        classificationResult = (ClassificationInferenceResults)result;
        assertThat(classificationResult.getTopClasses(), hasSize(2));

        future = new PlainActionFuture<>();
        model.classificationProbability(fields, -1, future);
        result = future.get();
        classificationResult = (ClassificationInferenceResults)result;
        assertThat(classificationResult.getTopClasses(), hasSize(2));
    }

    public void testRegression() throws Exception {
        TrainedModelDefinition trainedModelDefinition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildRegression())
            .build();
        Model model = new LocalModel("regression_model", trainedModelDefinition);

        Map<String, Object> fields = new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }};

        PlainActionFuture<InferenceResults<?>> future = new PlainActionFuture<>();
        model.infer(fields, future);
        InferenceResults<?> results = future.get();
        assertThat(results.value(), equalTo(1.3));

        PlainActionFuture<InferenceResults<?>> failedFuture = new PlainActionFuture<>();
        model.classificationProbability(fields, -1, failedFuture);
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
