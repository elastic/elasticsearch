/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.license.License;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.inference.loadingservice.LocalModelTests.buildClassification;
import static org.elasticsearch.xpack.ml.inference.loadingservice.LocalModelTests.buildRegression;
import static org.hamcrest.CoreMatchers.instanceOf;
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
        TrainedModelConfig config1 = buildTrainedModelConfigBuilder(modelId2).setInput(
            new TrainedModelInput(Arrays.asList("field.foo", "field.bar", "other.categorical"))
        )
            .setParsedDefinition(
                new TrainedModelDefinition.Builder().setPreProcessors(
                    Arrays.asList(new OneHotEncoding("other.categorical", oneHotEncoding, false))
                ).setTrainedModel(buildClassification(true))
            )
            .setVersion(Version.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setCreateTime(Instant.now())
            .setEstimatedOperations(0)
            .setModelSize(0)
            .build();
        TrainedModelConfig config2 = buildTrainedModelConfigBuilder(modelId1).setInput(
            new TrainedModelInput(Arrays.asList("field.foo", "field.bar", "other.categorical"))
        )
            .setParsedDefinition(
                new TrainedModelDefinition.Builder().setPreProcessors(
                    Arrays.asList(new OneHotEncoding("other.categorical", oneHotEncoding, false))
                ).setTrainedModel(buildRegression())
            )
            .setVersion(Version.CURRENT)
            .setEstimatedOperations(0)
            .setModelSize(0)
            .setCreateTime(Instant.now())
            .build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config1, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config2, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        List<Map<String, Object>> toInfer = new ArrayList<>();
        toInfer.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 1.0);
                        put("bar", 0.5);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "dog");
                    }
                });
            }
        });
        toInfer.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 0.9);
                        put("bar", 1.5);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "cat");
                    }
                });
            }
        });

        List<Map<String, Object>> toInfer2 = new ArrayList<>();
        toInfer2.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 0.0);
                        put("bar", 0.01);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "dog");
                    }
                });
            }
        });
        toInfer2.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 1.0);
                        put("bar", 0.0);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "cat");
                    }
                });
            }
        });

        // Test regression
        InferModelAction.Request request = InferModelAction.Request.forIngestDocs(
            modelId1,
            toInfer,
            RegressionConfigUpdate.EMPTY_PARAMS,
            true
        );
        InferModelAction.Response response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(
            response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults) i).value()).collect(Collectors.toList()),
            contains(1.3, 1.25)
        );

        request = InferModelAction.Request.forIngestDocs(modelId1, toInfer2, RegressionConfigUpdate.EMPTY_PARAMS, true);
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(
            response.getInferenceResults().stream().map(i -> ((SingleValueInferenceResults) i).value()).collect(Collectors.toList()),
            contains(1.65, 1.55)
        );

        // Test classification
        request = InferModelAction.Request.forIngestDocs(modelId2, toInfer, ClassificationConfigUpdate.EMPTY_PARAMS, true);
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(
            response.getInferenceResults()
                .stream()
                .map(i -> ((SingleValueInferenceResults) i).valueAsString())
                .collect(Collectors.toList()),
            contains("no", "yes")
        );

        // Get top classes
        request = InferModelAction.Request.forIngestDocs(
            modelId2,
            toInfer,
            new ClassificationConfigUpdate(2, null, null, null, null),
            true
        );
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        ClassificationInferenceResults classificationInferenceResults = (ClassificationInferenceResults) response.getInferenceResults()
            .get(0);

        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("no"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("yes"));
        assertThat(
            classificationInferenceResults.getTopClasses().get(0).getProbability(),
            greaterThan(classificationInferenceResults.getTopClasses().get(1).getProbability())
        );

        classificationInferenceResults = (ClassificationInferenceResults) response.getInferenceResults().get(1);
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("yes"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("no"));
        // they should always be in order of Most probable to least
        assertThat(
            classificationInferenceResults.getTopClasses().get(0).getProbability(),
            greaterThan(classificationInferenceResults.getTopClasses().get(1).getProbability())
        );

        // Test that top classes restrict the number returned
        request = InferModelAction.Request.forIngestDocs(
            modelId2,
            toInfer2,
            new ClassificationConfigUpdate(1, null, null, null, null),
            true
        );
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        classificationInferenceResults = (ClassificationInferenceResults) response.getInferenceResults().get(0);
        assertThat(classificationInferenceResults.getTopClasses(), hasSize(1));
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("yes"));
    }

    public void testInferModelMultiClassModel() throws Exception {
        String modelId = "test-load-models-classification-multi";
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).setInput(
            new TrainedModelInput(Arrays.asList("field.foo", "field.bar", "other.categorical"))
        )
            .setParsedDefinition(
                new TrainedModelDefinition.Builder().setPreProcessors(
                    Arrays.asList(new OneHotEncoding("other.categorical", oneHotEncoding, false))
                ).setTrainedModel(buildMultiClassClassification())
            )
            .setVersion(Version.CURRENT)
            .setLicenseLevel(License.OperationMode.PLATINUM.description())
            .setCreateTime(Instant.now())
            .setEstimatedOperations(0)
            .setModelSize(0)
            .build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        List<Map<String, Object>> toInfer = new ArrayList<>();
        toInfer.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 1.0);
                        put("bar", 0.5);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "dog");
                    }
                });
            }
        });
        toInfer.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 0.9);
                        put("bar", 1.5);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "cat");
                    }
                });
            }
        });

        List<Map<String, Object>> toInfer2 = new ArrayList<>();
        toInfer2.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 0.0);
                        put("bar", 0.01);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "dog");
                    }
                });
            }
        });
        toInfer2.add(new HashMap<>() {
            {
                put("field", new HashMap<>() {
                    {
                        put("foo", 1.0);
                        put("bar", 0.0);
                    }
                });
                put("other", new HashMap<>() {
                    {
                        put("categorical", "cat");
                    }
                });
            }
        });

        // Test regression
        InferModelAction.Request request = InferModelAction.Request.forIngestDocs(
            modelId,
            toInfer,
            ClassificationConfigUpdate.EMPTY_PARAMS,
            true
        );
        InferModelAction.Response response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(
            response.getInferenceResults()
                .stream()
                .map(i -> ((SingleValueInferenceResults) i).valueAsString())
                .collect(Collectors.toList()),
            contains("option_0", "option_2")
        );

        request = InferModelAction.Request.forIngestDocs(modelId, toInfer2, ClassificationConfigUpdate.EMPTY_PARAMS, true);
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();
        assertThat(
            response.getInferenceResults()
                .stream()
                .map(i -> ((SingleValueInferenceResults) i).valueAsString())
                .collect(Collectors.toList()),
            contains("option_2", "option_0")
        );

        // Get top classes
        request = InferModelAction.Request.forIngestDocs(modelId, toInfer, new ClassificationConfigUpdate(3, null, null, null, null), true);
        response = client().execute(InferModelAction.INSTANCE, request).actionGet();

        ClassificationInferenceResults classificationInferenceResults = (ClassificationInferenceResults) response.getInferenceResults()
            .get(0);

        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("option_0"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("option_2"));
        assertThat(classificationInferenceResults.getTopClasses().get(2).getClassification(), equalTo("option_1"));

        classificationInferenceResults = (ClassificationInferenceResults) response.getInferenceResults().get(1);
        assertThat(classificationInferenceResults.getTopClasses().get(0).getClassification(), equalTo("option_2"));
        assertThat(classificationInferenceResults.getTopClasses().get(1).getClassification(), equalTo("option_0"));
        assertThat(classificationInferenceResults.getTopClasses().get(2).getClassification(), equalTo("option_1"));
    }

    public void testInferMissingModel() {
        String model = "test-infer-missing-model";
        InferModelAction.Request request = InferModelAction.Request.forIngestDocs(
            model,
            Collections.emptyList(),
            RegressionConfigUpdate.EMPTY_PARAMS,
            true
        );
        try {
            client().execute(InferModelAction.INSTANCE, request).actionGet();
        } catch (ElasticsearchException ex) {
            assertThat(ex.getMessage(), equalTo(Messages.getMessage(Messages.INFERENCE_NOT_FOUND, model)));
        }
    }

    public void testInferMissingFields() throws Exception {
        String modelId = "test-load-models-regression-missing-fields";
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).setInput(
            new TrainedModelInput(Arrays.asList("field1", "field2"))
        )
            .setParsedDefinition(
                new TrainedModelDefinition.Builder().setPreProcessors(
                    Arrays.asList(new OneHotEncoding("categorical", oneHotEncoding, false))
                ).setTrainedModel(buildRegression())
            )
            .setVersion(Version.CURRENT)
            .setEstimatedOperations(0)
            .setModelSize(0)
            .setCreateTime(Instant.now())
            .build();
        AtomicReference<Boolean> putConfigHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(listener -> trainedModelProvider.storeTrainedModel(config, listener), putConfigHolder, exceptionHolder);
        assertThat(putConfigHolder.get(), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));

        List<Map<String, Object>> toInferMissingField = new ArrayList<>();
        toInferMissingField.add(new HashMap<>() {
            {
                put("foo", 1.0);
                put("bar", 0.5);
            }
        });

        InferModelAction.Request request = InferModelAction.Request.forIngestDocs(
            modelId,
            toInferMissingField,
            RegressionConfigUpdate.EMPTY_PARAMS,
            true
        );
        try {
            InferenceResults result = client().execute(InferModelAction.INSTANCE, request).actionGet().getInferenceResults().get(0);
            assertThat(result, is(instanceOf(WarningInferenceResults.class)));
            assertThat(
                ((WarningInferenceResults) result).getWarning(),
                equalTo(Messages.getMessage(Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING, modelId))
            );
        } catch (ElasticsearchException ex) {
            fail("Should not have thrown. Ex: " + ex.getMessage());
        }
    }

    static TrainedModelConfig.Builder buildTrainedModelConfigBuilder(String modelId) {
        return TrainedModelConfig.builder()
            .setCreatedBy("ml_test")
            .setParsedDefinition(TrainedModelDefinitionTests.createRandomBuilder())
            .setDescription("trained model config for test")
            .setModelType(TrainedModelType.TREE_ENSEMBLE)
            .setModelId(modelId);
    }

    public static TrainedModel buildMultiClassClassification() {
        List<String> featureNames = Arrays.asList("field.foo", "field.bar", "animal_cat", "animal_dog");

        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(Arrays.asList(1.0, 0.0, 2.0)))
            .addNode(TreeNode.builder(2).setThreshold(0.8).setSplitFeature(1).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(Arrays.asList(0.0, 1.0, 0.0)))
            .addNode(TreeNode.builder(4).setLeafValue(Arrays.asList(0.0, 0.0, 1.0)))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(3).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(Arrays.asList(2.0, 0.0, 0.0)))
            .addNode(TreeNode.builder(2).setLeafValue(Arrays.asList(0.0, 2.0, 0.0)))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(Arrays.asList(0.0, 0.0, 1.0)))
            .addNode(TreeNode.builder(2).setLeafValue(Arrays.asList(0.0, 1.0, 0.0)))
            .build();
        return Ensemble.builder()
            .setClassificationLabels(Arrays.asList("option_0", "option_1", "option_2"))
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[] { 0.7, 0.5, 1.0 }, 3))
            .build();
    }

}
