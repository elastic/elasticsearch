/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference.loadingservice;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.license.License;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PredictionFieldType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.inference.TrainedModelStatsService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.writeResult;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.EnsembleInferenceModelTests.serializeFromTrainedModel;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class LocalModelTests extends ESTestCase {

    public void testClassificationInfer() throws Exception {
        TrainedModelStatsService modelStatsService = mock(TrainedModelStatsService.class);
        doAnswer((args) -> null).when(modelStatsService).queueStats(any(InferenceStats.class), anyBoolean());
        String modelId = "classification_model";
        List<String> inputFields = Arrays.asList("field.foo", "field.bar", "categorical");
        InferenceDefinition definition = InferenceDefinition.builder()
            .setPreProcessors(Collections.singletonList(new OneHotEncoding("categorical", oneHotMap(), false)))
            .setTrainedModel(buildClassificationInference(false))
            .build();

        LocalModel model = new LocalModel(
            modelId,
            "test-node",
            definition,
            new TrainedModelInput(inputFields),
            Collections.singletonMap("field.foo", "field.foo.keyword"),
            ClassificationConfig.EMPTY_PARAMS,
            randomFrom(License.OperationMode.values()),
            TrainedModelType.TREE_ENSEMBLE,
            modelStatsService,
            mock(CircuitBreaker.class)
        );
        Map<String, Object> fields = new HashMap<>() {
            {
                put("field.foo", 1.0);
                put("field", Collections.singletonMap("bar", 0.5));
                put("categorical", "dog");
            }
        };

        SingleValueInferenceResults result = getSingleValue(model, fields, ClassificationConfigUpdate.EMPTY_PARAMS);
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), is("0"));
        assertThat(model.getLatestStatsAndReset().getInferenceCount(), equalTo(1L));

        ClassificationInferenceResults classificationResult = (ClassificationInferenceResults) getSingleValue(
            model,
            fields,
            new ClassificationConfigUpdate(1, null, null, null, null)
        );
        assertThat(classificationResult.getTopClasses().get(0).getProbability(), closeTo(0.5498339973124778, 0.0000001));
        assertThat(classificationResult.getTopClasses().get(0).getClassification(), equalTo("0"));
        assertThat(model.getLatestStatsAndReset().getInferenceCount(), equalTo(1L));

        // Test with labels
        definition = InferenceDefinition.builder()
            .setPreProcessors(Collections.singletonList(new OneHotEncoding("categorical", oneHotMap(), false)))
            .setTrainedModel(buildClassificationInference(true))
            .build();
        model = new LocalModel(
            modelId,
            "test-node",
            definition,
            new TrainedModelInput(inputFields),
            Collections.singletonMap("field.foo", "field.foo.keyword"),
            ClassificationConfig.EMPTY_PARAMS,
            License.OperationMode.PLATINUM,
            TrainedModelType.TREE_ENSEMBLE,
            modelStatsService,
            mock(CircuitBreaker.class)
        );
        result = getSingleValue(model, fields, ClassificationConfigUpdate.EMPTY_PARAMS);
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), equalTo("no"));

        classificationResult = (ClassificationInferenceResults) getSingleValue(
            model,
            fields,
            new ClassificationConfigUpdate(1, null, null, null, null)
        );
        assertThat(classificationResult.getTopClasses().get(0).getProbability(), closeTo(0.5498339973124778, 0.0000001));
        assertThat(classificationResult.getTopClasses().get(0).getClassification(), equalTo("no"));
        assertThat(model.getLatestStatsAndReset().getInferenceCount(), equalTo(2L));

        classificationResult = (ClassificationInferenceResults) getSingleValue(
            model,
            fields,
            new ClassificationConfigUpdate(2, null, null, null, null)
        );
        assertThat(classificationResult.getTopClasses(), hasSize(2));
        assertThat(model.getLatestStatsAndReset().getInferenceCount(), equalTo(1L));

        classificationResult = (ClassificationInferenceResults) getSingleValue(
            model,
            fields,
            new ClassificationConfigUpdate(-1, null, null, null, null)
        );
        assertThat(classificationResult.getTopClasses(), hasSize(2));
        assertThat(model.getLatestStatsAndReset().getInferenceCount(), equalTo(1L));
    }

    @SuppressWarnings("unchecked")
    public void testClassificationInferWithDifferentPredictionFieldTypes() throws Exception {
        TrainedModelStatsService modelStatsService = mock(TrainedModelStatsService.class);
        doAnswer((args) -> null).when(modelStatsService).queueStats(any(InferenceStats.class), anyBoolean());
        String modelId = "classification_model";
        List<String> inputFields = Arrays.asList("field.foo.keyword", "field.bar", "categorical");
        InferenceDefinition definition = InferenceDefinition.builder()
            .setPreProcessors(Collections.singletonList(new OneHotEncoding("categorical", oneHotMap(), false)))
            .setTrainedModel(buildClassificationInference(true))
            .build();

        LocalModel model = new LocalModel(
            modelId,
            "test-node",
            definition,
            new TrainedModelInput(inputFields),
            Collections.singletonMap("field.foo", "field.foo.keyword"),
            ClassificationConfig.EMPTY_PARAMS,
            License.OperationMode.PLATINUM,
            TrainedModelType.TREE_ENSEMBLE,
            modelStatsService,
            mock(CircuitBreaker.class)
        );
        Map<String, Object> fields = new HashMap<>() {
            {
                put("field.foo", 1.0);
                put("field.bar", 0.5);
                put("categorical", "dog");
            }
        };

        InferenceResults result = getInferenceResult(
            model,
            fields,
            new ClassificationConfigUpdate(2, null, null, null, PredictionFieldType.STRING)
        );

        IngestDocument document = TestIngestDocument.emptyIngestDocument();
        writeResult(result, document, "result_field", modelId);
        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("no"));
        List<?> list = document.getFieldValue("result_field.top_classes", List.class);
        assertThat(list.size(), equalTo(2));
        assertThat(((Map<String, Object>) list.get(0)).get("class_name"), equalTo("no"));
        assertThat(((Map<String, Object>) list.get(1)).get("class_name"), equalTo("yes"));

        result = getInferenceResult(model, fields, new ClassificationConfigUpdate(2, null, null, null, PredictionFieldType.NUMBER));

        document = TestIngestDocument.emptyIngestDocument();
        writeResult(result, document, "result_field", modelId);
        assertThat(document.getFieldValue("result_field.predicted_value", Double.class), equalTo(0.0));
        list = document.getFieldValue("result_field.top_classes", List.class);
        assertThat(list.size(), equalTo(2));
        assertThat(((Map<String, Object>) list.get(0)).get("class_name"), equalTo(0.0));
        assertThat(((Map<String, Object>) list.get(1)).get("class_name"), equalTo(1.0));

        result = getInferenceResult(model, fields, new ClassificationConfigUpdate(2, null, null, null, PredictionFieldType.BOOLEAN));

        document = TestIngestDocument.emptyIngestDocument();
        writeResult(result, document, "result_field", modelId);
        assertThat(document.getFieldValue("result_field.predicted_value", Boolean.class), equalTo(false));
        list = document.getFieldValue("result_field.top_classes", List.class);
        assertThat(list.size(), equalTo(2));
        assertThat(((Map<String, Object>) list.get(0)).get("class_name"), equalTo(false));
        assertThat(((Map<String, Object>) list.get(1)).get("class_name"), equalTo(true));
    }

    public void testRegression() throws Exception {
        TrainedModelStatsService modelStatsService = mock(TrainedModelStatsService.class);
        doAnswer((args) -> null).when(modelStatsService).queueStats(any(InferenceStats.class), anyBoolean());
        List<String> inputFields = Arrays.asList("foo", "bar", "categorical");
        InferenceDefinition trainedModelDefinition = InferenceDefinition.builder()
            .setPreProcessors(Collections.singletonList(new OneHotEncoding("categorical", oneHotMap(), false)))
            .setTrainedModel(buildRegressionInference())
            .build();
        LocalModel model = new LocalModel(
            "regression_model",
            "test-node",
            trainedModelDefinition,
            new TrainedModelInput(inputFields),
            Collections.singletonMap("bar", "bar.keyword"),
            RegressionConfig.EMPTY_PARAMS,
            License.OperationMode.PLATINUM,
            TrainedModelType.TREE_ENSEMBLE,
            modelStatsService,
            mock(CircuitBreaker.class)
        );

        Map<String, Object> fields = new HashMap<>() {
            {
                put("foo", 1.0);
                put("bar.keyword", 0.5);
                put("categorical", "dog");
            }
        };

        SingleValueInferenceResults results = getSingleValue(model, fields, RegressionConfigUpdate.EMPTY_PARAMS);
        assertThat(results.value(), equalTo(1.3));
    }

    public void testAllFieldsMissing() throws Exception {
        TrainedModelStatsService modelStatsService = mock(TrainedModelStatsService.class);
        doAnswer((args) -> null).when(modelStatsService).queueStats(any(InferenceStats.class), anyBoolean());
        List<String> inputFields = Arrays.asList("foo", "bar", "categorical");
        InferenceDefinition trainedModelDefinition = InferenceDefinition.builder()
            .setPreProcessors(Collections.singletonList(new OneHotEncoding("categorical", oneHotMap(), false)))
            .setTrainedModel(buildRegressionInference())
            .build();
        LocalModel model = new LocalModel(
            "regression_model",
            "test-node",
            trainedModelDefinition,
            new TrainedModelInput(inputFields),
            null,
            RegressionConfig.EMPTY_PARAMS,
            License.OperationMode.PLATINUM,
            TrainedModelType.TREE_ENSEMBLE,
            modelStatsService,
            mock(CircuitBreaker.class)
        );

        Map<String, Object> fields = new HashMap<>() {
            {
                put("something", 1.0);
                put("other", 0.5);
                put("baz", "dog");
            }
        };

        WarningInferenceResults results = (WarningInferenceResults) getInferenceResult(model, fields, RegressionConfigUpdate.EMPTY_PARAMS);
        assertThat(results.getWarning(), equalTo(Messages.getMessage(Messages.INFERENCE_WARNING_ALL_FIELDS_MISSING, "regression_model")));
        assertThat(model.getLatestStatsAndReset().getMissingAllFieldsCount(), equalTo(1L));
    }

    public void testInferPersistsStatsAfterNumberOfCalls() throws Exception {
        TrainedModelStatsService modelStatsService = mock(TrainedModelStatsService.class);
        doAnswer((args) -> null).when(modelStatsService).queueStats(any(InferenceStats.class), anyBoolean());
        String modelId = "classification_model";
        List<String> inputFields = Arrays.asList("field.foo", "field.bar", "categorical");
        InferenceDefinition definition = InferenceDefinition.builder()
            .setPreProcessors(Collections.singletonList(new OneHotEncoding("categorical", oneHotMap(), false)))
            .setTrainedModel(buildClassificationInference(false))
            .build();

        LocalModel model = new LocalModel(
            modelId,
            "test-node",
            definition,
            new TrainedModelInput(inputFields),
            null,
            ClassificationConfig.EMPTY_PARAMS,
            License.OperationMode.PLATINUM,
            TrainedModelType.TREE_ENSEMBLE,
            modelStatsService,
            mock(CircuitBreaker.class)
        );
        Map<String, Object> fields = new HashMap<>() {
            {
                put("field.foo", 1.0);
                put("field.bar", 0.5);
                put("categorical", "dog");
            }
        };

        for (int i = 0; i < 100; i++) {
            getSingleValue(model, fields, ClassificationConfigUpdate.EMPTY_PARAMS);
        }
        SingleValueInferenceResults result = getSingleValue(model, fields, ClassificationConfigUpdate.EMPTY_PARAMS);
        assertThat(result.value(), equalTo(0.0));
        assertThat(result.valueAsString(), is("0"));
        // Should have reset after persistence, so only 2 docs have been seen since last persistence
        assertThat(model.getLatestStatsAndReset().getInferenceCount(), equalTo(2L));
        verify(modelStatsService, times(1)).queueStats(argThat(o -> o.getInferenceCount() == 99L), anyBoolean());
    }

    public void testMapFieldsIfNecessary() {
        Map<String, String> fieldMap = new TreeMap<>();
        fieldMap.put("a1", "a2");
        fieldMap.put("b1", "b2");
        fieldMap.put("missing1", "missing2");

        Map<String, Object> fields = new TreeMap<>();
        fields.put("a1", "a_value");
        fields.put("b1", "b_value");

        LocalModel.mapFieldsIfNecessary(fields, fieldMap);

        Map<String, Object> expectedMap = new TreeMap<>();
        expectedMap.put("a1", "a_value");
        expectedMap.put("b1", "b_value");
        expectedMap.put("a2", "a_value");
        expectedMap.put("b2", "b_value");

        assertThat(fields, equalTo(expectedMap));
    }

    public void testReferenceCounting() throws IOException {
        TrainedModelStatsService modelStatsService = mock(TrainedModelStatsService.class);
        String modelId = "ref-count-model";
        List<String> inputFields = Arrays.asList("field.foo", "field.bar");
        InferenceDefinition definition = InferenceDefinition.builder().setTrainedModel(buildClassificationInference(false)).build();

        {
            CircuitBreaker breaker = mock(CircuitBreaker.class);
            LocalModel model = new LocalModel(
                modelId,
                "test-node",
                definition,
                new TrainedModelInput(inputFields),
                null,
                ClassificationConfig.EMPTY_PARAMS,
                License.OperationMode.PLATINUM,
                TrainedModelType.TREE_ENSEMBLE,
                modelStatsService,
                breaker
            );

            model.release();
            verify(breaker, times(1)).addWithoutBreaking(eq(-definition.ramBytesUsed()));
            verifyNoMoreInteractions(breaker);
            assertEquals(0L, model.getReferenceCount());

            // reacquire
            model.acquire();
            verify(breaker, times(1)).addEstimateBytesAndMaybeBreak(eq(definition.ramBytesUsed()), eq(modelId));
            verifyNoMoreInteractions(breaker);
            assertEquals(1L, model.getReferenceCount());
        }

        {
            CircuitBreaker breaker = mock(CircuitBreaker.class);
            LocalModel model = new LocalModel(
                modelId,
                "test-node",
                definition,
                new TrainedModelInput(inputFields),
                null,
                ClassificationConfig.EMPTY_PARAMS,
                License.OperationMode.PLATINUM,
                TrainedModelType.TREE_ENSEMBLE,
                modelStatsService,
                breaker
            );

            model.acquire();
            model.acquire();
            model.release();
            model.release();
            model.release();
            verify(breaker, times(1)).addWithoutBreaking(eq(-definition.ramBytesUsed()));
            verifyNoMoreInteractions(breaker);
            assertEquals(0L, model.getReferenceCount());
        }
    }

    private static SingleValueInferenceResults getSingleValue(LocalModel model, Map<String, Object> fields, InferenceConfigUpdate config)
        throws Exception {
        return (SingleValueInferenceResults) getInferenceResult(model, fields, config);
    }

    private static InferenceResults getInferenceResult(LocalModel model, Map<String, Object> fields, InferenceConfigUpdate config)
        throws Exception {
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

    public static InferenceModel buildClassificationInference(boolean includeLables) throws IOException {
        return serializeFromTrainedModel((Ensemble) buildClassification(includeLables));
    }

    public static TrainedModel buildClassification(boolean includeLabels) {
        List<String> featureNames = Arrays.asList("field.foo", "field.bar", "animal_cat", "animal_dog");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setThreshold(0.8).setSplitFeature(1).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(3).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(0.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .build();
        return Ensemble.builder()
            .setClassificationLabels(includeLabels ? Arrays.asList("no", "yes") : null)
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[] { 0.7, 0.5, 1.0 }, 2))
            .build();
    }

    public static InferenceModel buildRegressionInference() throws IOException {
        return serializeFromTrainedModel((Ensemble) buildRegression());
    }

    public static TrainedModel buildRegression() {
        List<String> featureNames = Arrays.asList("field.foo", "field.bar", "animal_cat", "animal_dog");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.3))
            .addNode(TreeNode.builder(2).setThreshold(0.0).setSplitFeature(3).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.1))
            .addNode(TreeNode.builder(4).setLeafValue(0.2))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(2).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(1).setThreshold(0.2))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        return Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedSum(new double[] { 0.5, 0.5, 0.5 }))
            .build();
    }

}
