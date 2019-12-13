/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

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
        List<String> inputFields = Arrays.asList("foo", "bar", "categorical");
        TrainedModelDefinition definition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildClassification(false))
            .build();

        Model model = new LocalModel(modelId, definition, new TrainedModelInput(inputFields));
        Map<String, Object> fields = new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }};

        SingleValueInferenceResults result = getSingleValue(model, fields, new ClassificationConfig(0));
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), is("0.0"));

        ClassificationInferenceResults classificationResult =
            (ClassificationInferenceResults)getSingleValue(model, fields, new ClassificationConfig(1));
        assertThat(classificationResult.getTopClasses().get(0).getProbability(), closeTo(0.5498339973124778, 0.0000001));
        assertThat(classificationResult.getTopClasses().get(0).getClassification(), equalTo("0"));

        // Test with labels
        definition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildClassification(true))
            .build();
        model = new LocalModel(modelId, definition, new TrainedModelInput(inputFields));
        result = getSingleValue(model, fields, new ClassificationConfig(0));
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), equalTo("not_to_be"));

        classificationResult = (ClassificationInferenceResults)getSingleValue(model, fields, new ClassificationConfig(1));
        assertThat(classificationResult.getTopClasses().get(0).getProbability(), closeTo(0.5498339973124778, 0.0000001));
        assertThat(classificationResult.getTopClasses().get(0).getClassification(), equalTo("not_to_be"));

        classificationResult = (ClassificationInferenceResults)getSingleValue(model, fields, new ClassificationConfig(2));
        assertThat(classificationResult.getTopClasses(), hasSize(2));

        classificationResult = (ClassificationInferenceResults)getSingleValue(model, fields, new ClassificationConfig(-1));
        assertThat(classificationResult.getTopClasses(), hasSize(2));
    }

    public void testRegression() throws Exception {
        List<String> inputFields = Arrays.asList("foo", "bar", "categorical");
        TrainedModelDefinition trainedModelDefinition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildRegression())
            .build();
        Model model = new LocalModel("regression_model", trainedModelDefinition, new TrainedModelInput(inputFields));

        Map<String, Object> fields = new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }};

        SingleValueInferenceResults results = getSingleValue(model, fields, RegressionConfig.EMPTY_PARAMS);
        assertThat(results.value(), equalTo(1.3));

        PlainActionFuture<InferenceResults> failedFuture = new PlainActionFuture<>();
        model.infer(fields, new ClassificationConfig(2), failedFuture);
        ExecutionException ex = expectThrows(ExecutionException.class, failedFuture::get);
        assertThat(ex.getCause().getMessage(),
            equalTo("Cannot infer using configuration for [classification] when model target_type is [regression]"));
    }

    public void testAllFieldsMissing() throws Exception {
        List<String> inputFields = Arrays.asList("foo", "bar", "categorical");
        TrainedModelDefinition trainedModelDefinition = new TrainedModelDefinition.Builder()
            .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotMap())))
            .setTrainedModel(buildRegression())
            .build();
        Model model = new LocalModel("regression_model", trainedModelDefinition, new TrainedModelInput(inputFields));

        Map<String, Object> fields = new HashMap<>() {{
            put("something", 1.0);
            put("other", 0.5);
            put("baz", "dog");
        }};

        WarningInferenceResults results = (WarningInferenceResults)getInferenceResult(model, fields, RegressionConfig.EMPTY_PARAMS);
        assertThat(results.getWarning(),
            equalTo(Messages.getMessage(Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING, "regression_model")));
    }

    private static SingleValueInferenceResults getSingleValue(Model model,
                                                              Map<String, Object> fields,
                                                              InferenceConfig config) throws Exception {
        return (SingleValueInferenceResults)getInferenceResult(model, fields, config);
    }

    private static InferenceResults getInferenceResult(Model model, Map<String, Object> fields, InferenceConfig config) throws Exception {
        PlainActionFuture<InferenceResults> future = new PlainActionFuture<>();
        model.infer(fields, config, future);
        return future.get();
    }

    private static Map<String, String> oneHotMap() {
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        return oneHotEncoding;
    }

    public static TrainedModel buildClassification(boolean includeLabels) {
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
            .setOutputAggregator(new WeightedMode(new double[]{0.7, 0.5, 1.0}))
            .build();
    }

    public static TrainedModel buildRegression() {
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
            .setOutputAggregator(new WeightedSum(new double[]{0.5, 0.5, 0.5}))
            .build();
    }

}
