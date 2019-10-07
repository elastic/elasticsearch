/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceParams;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ModelInferenceActionIT extends MlSingleNodeTestCase {

    private TrainedModelProvider trainedModelProvider;

    @Before
    public void createComponents() throws Exception {
        trainedModelProvider = new TrainedModelProvider(client(), xContentRegistry());
        waitForMlTemplates();
    }

    public void testInferModels() throws Exception {
        String modelId1 = "test-load-models-regression";
        String modelId2 = "test-load-models-classification";
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        TrainedModelConfig config1 = buildTrainedModelConfigBuilder(modelId2, 0)
            .setDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotEncoding)))
                .setInput(new TrainedModelDefinition.Input(Arrays.asList("field1", "field2")))
                .setTrainedModel(buildClassification()))
            .build(Version.CURRENT);
        TrainedModelConfig config2 = buildTrainedModelConfigBuilder(modelId1, 0)
            .setDefinition(new TrainedModelDefinition.Builder()
                .setPreProcessors(Arrays.asList(new OneHotEncoding("categorical", oneHotEncoding)))
                .setInput(new TrainedModelDefinition.Input(Arrays.asList("field1", "field2")))
                .setTrainedModel(buildRegression()))
            .build(Version.CURRENT);
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config1, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config2, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));


        List<Map<String, Object>> toInfer = new ArrayList<>();
        toInfer.add(new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.5);
            put("categorical", "dog");
        }});
        toInfer.add(new HashMap<>() {{
            put("foo", 0.9);
            put("bar", 1.5);
            put("categorical", "cat");
        }});

        List<Map<String, Object>> toInfer2 = new ArrayList<>();
        toInfer2.add(new HashMap<>() {{
            put("foo", 0.0);
            put("bar", 0.01);
            put("categorical", "dog");
        }});
        toInfer2.add(new HashMap<>() {{
            put("foo", 1.0);
            put("bar", 0.0);
            put("categorical", "cat");
        }});

        // Test regression
        InferModelAction.Request request = new InferModelAction.Request(modelId1, 0, toInfer, null);
        InferModelAction.Response response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults)i).value()).collect(Collectors.toList()),
            contains(1.3, 1.25));

        request = new InferModelAction.Request(modelId1, 0, toInfer2, null);
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults)i).value()).collect(Collectors.toList()),
            contains(1.65, 1.55));


        // Test classification
        request = new InferModelAction.Request(modelId2, 0, toInfer, null);
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(response.getInferenceResults()
                .stream()
                .map(i -> ((SingleValueInferenceResults)i).valueAsString())
                .collect(Collectors.toList()),
            contains("not_to_be", "to_be"));

        // Get top classes
        request = new InferModelAction.Request(modelId2, 0, toInfer, new InferenceParams(2));
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        ClassificationInferenceResults classificationInferenceResults =
            (ClassificationInferenceResults)response.getInferenceResults().get(0);

        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("not_to_be"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("to_be"));
        assertThat(classificationInferenceResults.getTopClasses().get(0).getProbability(),
            greaterThan(classificationInferenceResults.getTopClasses().get(1).getProbability()));

        classificationInferenceResults = (ClassificationInferenceResults)response.getInferenceResults().get(1);
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("to_be"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("not_to_be"));
        // they should always be in order of Most probable to least
        assertThat(classificationInferenceResults.getTopClasses().get(0).getProbability(),
            greaterThan(classificationInferenceResults.getTopClasses().get(1).getProbability()));

        // Test that top classes restrict the number returned
        request = new InferModelAction.Request(modelId2, 0, toInfer2, new InferenceParams(1));
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        classificationInferenceResults = (ClassificationInferenceResults)response.getInferenceResults().get(0);
        assertThat(classificationInferenceResults.getTopClasses(), hasSize(1));
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("to_be"));
    }

    public void testInferMissingModel() {
        String model = "test-infer-missing-model";
        InferModelAction.Request request = new InferModelAction.Request(model, 0, Collections.emptyList(), null);
        try {
            client().execute(InferModelAction.INSTANCE, request).actionGet();
        } catch (ElasticsearchException ex) {
            assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model, 0)));
        }
    }

    private static TrainedModel buildClassification() {
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
            .setClassificationLabels(Arrays.asList("not_to_be", "to_be"))
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

    private static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId, long modelVersion) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelId(modelId)
            .setModelType("binary_decision_tree")
            .setModelVersion(modelVersion);
    }

    @Override
    public NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

}
