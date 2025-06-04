/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedMode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.WeightedSum;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.job.config.Operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceModelTestUtils.deserializeFromTrainedModel;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class EnsembleInferenceModelTests extends ESTestCase {

    private static final int NUMBER_OF_TEST_RUNS = 20;

    public static EnsembleInferenceModel serializeFromTrainedModel(Ensemble ensemble) throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        EnsembleInferenceModel model = deserializeFromTrainedModel(ensemble, registry, EnsembleInferenceModel::fromXContent);
        model.rewriteFeatureIndices(Collections.emptyMap());
        return model;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    public void testSerializationFromEnsemble() throws Exception {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            int numberOfFeatures = randomIntBetween(1, 10);
            Ensemble ensemble = EnsembleTests.createRandom(randomFrom(TargetType.values()), randomBoolean() ? 0 : numberOfFeatures);
            assertThat(serializeFromTrainedModel(ensemble), is(not(nullValue())));
        }
    }

    public void testInferenceWithoutPreparing() throws IOException {
        Ensemble ensemble = EnsembleTests.createRandom(TargetType.REGRESSION, 4);

        EnsembleInferenceModel model = deserializeFromTrainedModel(ensemble, xContentRegistry(), EnsembleInferenceModel::fromXContent);
        expectThrows(ElasticsearchException.class, () -> model.infer(Collections.emptyMap(), RegressionConfig.EMPTY_PARAMS, null));
    }

    public void testClassificationProbability() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
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
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(1).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .build();
        Ensemble ensembleObject = Ensemble.builder()
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[] { 0.7, 0.5, 1.0 }, 2))
            .setClassificationWeights(Arrays.asList(0.7, 0.3))
            .build();

        EnsembleInferenceModel ensemble = deserializeFromTrainedModel(
            ensembleObject,
            xContentRegistry(),
            EnsembleInferenceModel::fromXContent
        );
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        List<Double> expected = Arrays.asList(0.768524783, 0.231475216);
        List<Double> scores = Arrays.asList(0.230557435, 0.162032651);
        double eps = 0.000001;
        List<TopClassEntry> probabilities = ((ClassificationInferenceResults) ensemble.infer(
            featureMap,
            new ClassificationConfig(2),
            Collections.emptyMap()
        )).getTopClasses();
        for (int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        expected = Arrays.asList(0.310025518, 0.6899744811);
        scores = Arrays.asList(0.217017863, 0.2069923443);
        probabilities = ((ClassificationInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
            .getTopClasses();
        for (int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }

        featureVector = Arrays.asList(0.0, 1.0);
        featureMap = zipObjMap(featureNames, featureVector);
        expected = Arrays.asList(0.768524783, 0.231475216);
        scores = Arrays.asList(0.230557435, 0.162032651);
        probabilities = ((ClassificationInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
            .getTopClasses();
        for (int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }

        // This should handle missing values and take the default_left path
        featureMap = Maps.newMapWithExpectedSize(2);
        featureMap.put("foo", 0.3);
        featureMap.put("bar", null);
        expected = Arrays.asList(0.6899744811, 0.3100255188);
        scores = Arrays.asList(0.482982136, 0.0930076556);
        probabilities = ((ClassificationInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
            .getTopClasses();
        for (int i = 0; i < expected.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expected.get(i), eps));
            assertThat(probabilities.get(i).getScore(), closeTo(scores.get(i), eps));
        }
    }

    public void testClassificationInference() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setThreshold(0.8).setSplitFeature(1).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(1).setThreshold(1.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Ensemble ensembleObject = Ensemble.builder()
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[] { 0.7, 0.5, 1.0 }, 2))
            .build();

        EnsembleInferenceModel ensemble = deserializeFromTrainedModel(
            ensembleObject,
            xContentRegistry(),
            EnsembleInferenceModel::fromXContent
        );
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureVector = Arrays.asList(0.0, 1.0);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureMap = Maps.newMapWithExpectedSize(2);
        featureMap.put("foo", 0.3);
        featureMap.put("bar", null);
        assertThat(
            0.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );
    }

    public void testMultiClassClassificationInference() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(2.0))
            .addNode(TreeNode.builder(2).setThreshold(0.8).setSplitFeature(1).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.0))
            .addNode(TreeNode.builder(4).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(1).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(2.0))
            .addNode(TreeNode.builder(2).setLeafValue(1.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Tree tree3 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(1).setThreshold(2.0))
            .addNode(TreeNode.builder(1).setLeafValue(1.0))
            .addNode(TreeNode.builder(2).setLeafValue(0.0))
            .setTargetType(randomFrom(TargetType.CLASSIFICATION, TargetType.REGRESSION))
            .build();
        Ensemble ensembleObject = Ensemble.builder()
            .setTargetType(TargetType.CLASSIFICATION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2, tree3))
            .setOutputAggregator(new WeightedMode(new double[] { 0.7, 0.5, 1.0 }, 3))
            .build();

        EnsembleInferenceModel ensemble = deserializeFromTrainedModel(
            ensembleObject,
            xContentRegistry(),
            EnsembleInferenceModel::fromXContent
        );
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            2.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureVector = Arrays.asList(0.0, 1.0);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureMap = Maps.newMapWithExpectedSize(2);
        featureMap.put("foo", 0.6);
        featureMap.put("bar", null);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, new ClassificationConfig(0), Collections.emptyMap())).value(),
                0.00001
            )
        );
    }

    public void testRegressionInference() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.3))
            .addNode(TreeNode.builder(2).setThreshold(0.8).setSplitFeature(1).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.1))
            .addNode(TreeNode.builder(4).setLeafValue(0.2))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Ensemble ensembleObject = Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .setOutputAggregator(new WeightedSum(new double[] { 0.5, 0.5 }))
            .build();

        EnsembleInferenceModel ensemble = deserializeFromTrainedModel(
            ensembleObject,
            xContentRegistry(),
            EnsembleInferenceModel::fromXContent
        );
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        List<Double> featureVector = Arrays.asList(0.4, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            0.9,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            0.5,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        // Test with NO aggregator supplied, verifies default behavior of non-weighted sum
        ensembleObject = Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .build();
        ensemble = deserializeFromTrainedModel(ensembleObject, xContentRegistry(), EnsembleInferenceModel::fromXContent);
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        featureVector = Arrays.asList(0.4, 0.0);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.8,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureVector = Arrays.asList(2.0, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            1.0,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        featureMap = Maps.newMapWithExpectedSize(2);
        featureMap.put("foo", 0.3);
        featureMap.put("bar", null);
        assertThat(
            1.8,
            closeTo(
                ((SingleValueInferenceResults) ensemble.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );
    }

    public void testFeatureImportance() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.55)
                    .setNumberSamples(10L),
                TreeNode.builder(1)
                    .setSplitFeature(0)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setOperator(Operator.LT)
                    .setThreshold(0.41)
                    .setNumberSamples(6L),
                TreeNode.builder(2)
                    .setSplitFeature(1)
                    .setLeftChild(5)
                    .setRightChild(6)
                    .setOperator(Operator.LT)
                    .setThreshold(0.25)
                    .setNumberSamples(4L),
                TreeNode.builder(3).setLeafValue(1.18230136).setNumberSamples(5L),
                TreeNode.builder(4).setLeafValue(1.98006658).setNumberSamples(1L),
                TreeNode.builder(5).setLeafValue(3.25350885).setNumberSamples(3L),
                TreeNode.builder(6).setLeafValue(2.42384369).setNumberSamples(1L)
            )
            .build();

        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.45)
                    .setNumberSamples(10L),
                TreeNode.builder(1)
                    .setSplitFeature(0)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setOperator(Operator.LT)
                    .setThreshold(0.25)
                    .setNumberSamples(5L),
                TreeNode.builder(2)
                    .setSplitFeature(0)
                    .setLeftChild(5)
                    .setRightChild(6)
                    .setOperator(Operator.LT)
                    .setThreshold(0.59)
                    .setNumberSamples(5L),
                TreeNode.builder(3).setLeafValue(1.04476388).setNumberSamples(3L),
                TreeNode.builder(4).setLeafValue(1.52799228).setNumberSamples(2L),
                TreeNode.builder(5).setLeafValue(1.98006658).setNumberSamples(1L),
                TreeNode.builder(6).setLeafValue(2.950216).setNumberSamples(4L)
            )
            .build();

        Ensemble ensembleObject = Ensemble.builder()
            .setOutputAggregator(new WeightedSum((double[]) null))
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .setFeatureNames(featureNames)
            .build();

        EnsembleInferenceModel ensemble = deserializeFromTrainedModel(
            ensembleObject,
            xContentRegistry(),
            EnsembleInferenceModel::fromXContent
        );
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        double[][] featureImportance = ensemble.featureImportance(new double[] { 0.0, 0.9 });
        final double eps = 1.0E-8;
        assertThat(featureImportance[0][0], closeTo(-1.653200025, eps));
        assertThat(featureImportance[1][0], closeTo(-0.12444978, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.1, 0.8 });
        assertThat(featureImportance[0][0], closeTo(-1.653200025, eps));
        assertThat(featureImportance[1][0], closeTo(-0.12444978, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.2, 0.7 });
        assertThat(featureImportance[0][0], closeTo(-1.653200025, eps));
        assertThat(featureImportance[1][0], closeTo(-0.12444978, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.3, 0.6 });
        assertThat(featureImportance[0][0], closeTo(-1.16997162, eps));
        assertThat(featureImportance[1][0], closeTo(-0.12444978, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.4, 0.5 });
        assertThat(featureImportance[0][0], closeTo(-1.16997162, eps));
        assertThat(featureImportance[1][0], closeTo(-0.12444978, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.5, 0.4 });
        assertThat(featureImportance[0][0], closeTo(0.0798679, eps));
        assertThat(featureImportance[1][0], closeTo(-0.12444978, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.6, 0.3 });
        assertThat(featureImportance[0][0], closeTo(1.80491886, eps));
        assertThat(featureImportance[1][0], closeTo(-0.4355742, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.7, 0.2 });
        assertThat(featureImportance[0][0], closeTo(2.0538184, eps));
        assertThat(featureImportance[1][0], closeTo(0.1451914, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.8, 0.1 });
        assertThat(featureImportance[0][0], closeTo(2.0538184, eps));
        assertThat(featureImportance[1][0], closeTo(0.1451914, eps));

        featureImportance = ensemble.featureImportance(new double[] { 0.9, 0.0 });
        assertThat(featureImportance[0][0], closeTo(2.0538184, eps));
        assertThat(featureImportance[1][0], closeTo(0.1451914, eps));
    }

    public void testMinAndMaxBoundaries() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree1 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(0.3))
            .addNode(TreeNode.builder(2).setThreshold(0.8).setSplitFeature(1).setLeftChild(3).setRightChild(4))
            .addNode(TreeNode.builder(3).setLeafValue(0.1))
            .addNode(TreeNode.builder(4).setLeafValue(0.2))
            .build();
        Tree tree2 = Tree.builder()
            .setFeatureNames(featureNames)
            .setRoot(TreeNode.builder(0).setLeftChild(1).setRightChild(2).setSplitFeature(0).setThreshold(0.5))
            .addNode(TreeNode.builder(1).setLeafValue(1.5))
            .addNode(TreeNode.builder(2).setLeafValue(0.9))
            .build();
        Ensemble ensembleObject = Ensemble.builder()
            .setTargetType(TargetType.REGRESSION)
            .setFeatureNames(featureNames)
            .setTrainedModels(Arrays.asList(tree1, tree2))
            .setOutputAggregator(new WeightedSum(new double[] { 0.5, 0.5 }))
            .build();

        EnsembleInferenceModel ensemble = deserializeFromTrainedModel(
            ensembleObject,
            xContentRegistry(),
            EnsembleInferenceModel::fromXContent
        );
        ensemble.rewriteFeatureIndices(Collections.emptyMap());

        assertThat(ensemble.getMinPredictedValue(), equalTo(1.0));
        assertThat(ensemble.getMaxPredictedValue(), equalTo(1.8));
    }

    private static Map<String, Object> zipObjMap(List<String> keys, List<Double> values) {
        return IntStream.range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }
}
