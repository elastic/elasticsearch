/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
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

public class TreeInferenceModelTests extends ESTestCase {

    private static final int NUMBER_OF_TEST_RUNS = 20;

    public static TreeInferenceModel serializeFromTrainedModel(Tree tree) throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        TreeInferenceModel model = deserializeFromTrainedModel(tree, registry, TreeInferenceModel::fromXContent);
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

    public void testCtorWithNullTargetType() {
        TreeInferenceModel treeInferenceModel = new TreeInferenceModel(
            Collections.emptyList(),
            Collections.singletonList(new TreeInferenceModel.NodeBuilder().setLeafValue(new double[] { 1.0 }).setNumberSamples(100L)),
            null,
            Collections.emptyList()
        );
        assertThat(treeInferenceModel.targetType(), equalTo(TargetType.REGRESSION));
    }

    public void testSerializationFromEnsemble() throws Exception {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            Tree tree = TreeTests.createRandom(randomFrom(TargetType.values()));
            assertThat(serializeFromTrainedModel(tree), is(not(nullValue())));
        }
    }

    public void testInferenceWithoutPreparing() throws IOException {
        Tree tree = TreeTests.createRandom(randomFrom(TargetType.values()));

        TreeInferenceModel model = deserializeFromTrainedModel(tree, xContentRegistry(), TreeInferenceModel::fromXContent);
        expectThrows(ElasticsearchException.class, () -> model.infer(Collections.emptyMap(), RegressionConfig.EMPTY_PARAMS, null));
    }

    public void testInferWithStump() throws IOException {
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        builder.setRoot(TreeNode.builder(0).setLeafValue(Collections.singletonList(42.0)));
        builder.setFeatureNames(Collections.emptyList());

        Tree treeObject = builder.build();
        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());
        List<String> featureNames = Arrays.asList("foo", "bar");
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector); // does not really matter as this is a stump
        assertThat(
            42.0,
            closeTo(
                ((SingleValueInferenceResults) tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );
    }

    public void testInfer() throws IOException {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), 0.3);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 0.1);
        builder.addLeaf(leftChildNode.getRightChild(), 0.2);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = builder.setFeatureNames(featureNames).build();
        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());
        // This feature vector should hit the right child of the root node
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            0.3,
            closeTo(
                ((SingleValueInferenceResults) tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureVector = Arrays.asList(0.3, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            0.1,
            closeTo(
                ((SingleValueInferenceResults) tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        // This should hit the right child of the left child of the root node
        // i.e. it takes the path left, right
        featureVector = Arrays.asList(0.3, 0.9);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(
            0.2,
            closeTo(
                ((SingleValueInferenceResults) tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        // This should still work if the internal values are strings
        List<String> featureVectorStrings = Arrays.asList("0.3", "0.9");
        featureMap = zipObjMap(featureNames, featureVectorStrings);
        assertThat(
            0.2,
            closeTo(
                ((SingleValueInferenceResults) tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );

        // This should handle missing values and take the default_left path
        featureMap = Maps.newMapWithExpectedSize(2);
        featureMap.put("foo", 0.3);
        featureMap.put("bar", null);
        assertThat(
            0.1,
            closeTo(
                ((SingleValueInferenceResults) tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001
            )
        );
    }

    public void testTreeClassificationProbability() throws IOException {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.CLASSIFICATION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), 1.0);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 1.0);
        builder.addLeaf(leftChildNode.getRightChild(), 0.0);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = builder.setFeatureNames(featureNames).setClassificationLabels(Arrays.asList("cat", "dog")).build();
        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());
        final double eps = 0.000001;
        // This feature vector should hit the right child of the root node
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        List<Double> expectedProbs = Arrays.asList(1.0, 0.0);
        List<String> expectedFields = Arrays.asList("dog", "cat");
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        List<TopClassEntry> probabilities = ((ClassificationInferenceResults) tree.infer(
            featureMap,
            new ClassificationConfig(2),
            Collections.emptyMap()
        )).getTopClasses();
        for (int i = 0; i < expectedProbs.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expectedProbs.get(i), eps));
            assertThat(probabilities.get(i).getClassification(), equalTo(expectedFields.get(i)));
        }

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureVector = Arrays.asList(0.3, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        probabilities = ((ClassificationInferenceResults) tree.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
            .getTopClasses();
        for (int i = 0; i < expectedProbs.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expectedProbs.get(i), eps));
            assertThat(probabilities.get(i).getClassification(), equalTo(expectedFields.get(i)));
        }

        // This should handle missing values and take the default_left path
        featureMap = Maps.newMapWithExpectedSize(2);
        featureMap.put("foo", 0.3);
        featureMap.put("bar", null);
        probabilities = ((ClassificationInferenceResults) tree.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
            .getTopClasses();
        for (int i = 0; i < expectedProbs.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expectedProbs.get(i), eps));
            assertThat(probabilities.get(i).getClassification(), equalTo(expectedFields.get(i)));
        }
    }

    public void testFeatureImportance() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.5)
                    .setNumberSamples(4L),
                TreeNode.builder(1)
                    .setSplitFeature(1)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setOperator(Operator.LT)
                    .setThreshold(0.5)
                    .setNumberSamples(2L),
                TreeNode.builder(2)
                    .setSplitFeature(1)
                    .setLeftChild(5)
                    .setRightChild(6)
                    .setOperator(Operator.LT)
                    .setThreshold(0.5)
                    .setNumberSamples(2L),
                TreeNode.builder(3).setLeafValue(3.0).setNumberSamples(1L),
                TreeNode.builder(4).setLeafValue(8.0).setNumberSamples(1L),
                TreeNode.builder(5).setLeafValue(13.0).setNumberSamples(1L),
                TreeNode.builder(6).setLeafValue(18.0).setNumberSamples(1L)
            )
            .build();

        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        double[][] featureImportance = tree.featureImportance(new double[] { 0.25, 0.25 });
        final double eps = 1.0E-8;
        assertThat(featureImportance[0][0], closeTo(-5.0, eps));
        assertThat(featureImportance[1][0], closeTo(-2.5, eps));

        featureImportance = tree.featureImportance(new double[] { 0.25, 0.75 });
        assertThat(featureImportance[0][0], closeTo(-5.0, eps));
        assertThat(featureImportance[1][0], closeTo(2.5, eps));

        featureImportance = tree.featureImportance(new double[] { 0.75, 0.25 });
        assertThat(featureImportance[0][0], closeTo(5.0, eps));
        assertThat(featureImportance[1][0], closeTo(-2.5, eps));

        featureImportance = tree.featureImportance(new double[] { 0.75, 0.75 });
        assertThat(featureImportance[0][0], closeTo(5.0, eps));
        assertThat(featureImportance[1][0], closeTo(2.5, eps));
    }

    /**
     * Trained trees can contain a split where one child saw no training samples, typically below an ancestor that already
     * split on the same feature. The zero fraction for that child must not turn feature importance into NaN. Expected
     * values follow from SHAP local accuracy: importance sums to prediction minus the sample-weighted mean leaf value,
     * which is (2 * 10 + 2 * 20) / 4 = 15 here, since the zero-sample leaf carries no weight.
     */
    public void testFeatureImportanceWithZeroSampleLeaf() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.5)
                    .setNumberSamples(4L),
                TreeNode.builder(1)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setThreshold(0.2)
                    .setNumberSamples(2L),
                TreeNode.builder(2).setLeafValue(20.0).setNumberSamples(2L),
                TreeNode.builder(3).setLeafValue(7.0).setNumberSamples(0L),
                TreeNode.builder(4).setLeafValue(10.0).setNumberSamples(2L)
            )
            .build();

        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        final double eps = 1.0E-8;
        // Reaches the populated leaf (10) while the sibling leaf has zero samples
        double[][] featureImportance = tree.featureImportance(new double[] { 0.3, 0.0 });
        assertThat(featureImportance[0][0], closeTo(10.0 - 15.0, eps));
        assertThat(featureImportance[1][0], closeTo(0.0, eps));

        // Reaches the zero-sample leaf (7) itself
        featureImportance = tree.featureImportance(new double[] { 0.1, 0.0 });
        assertThat(featureImportance[0][0], closeTo(7.0 - 15.0, eps));
        assertThat(featureImportance[1][0], closeTo(0.0, eps));

        featureImportance = tree.featureImportance(new double[] { 0.7, 0.0 });
        assertThat(featureImportance[0][0], closeTo(20.0 - 15.0, eps));
        assertThat(featureImportance[1][0], closeTo(0.0, eps));
    }

    /**
     * An internal node can itself have zero training samples, so the fraction of its samples reaching each child is 0 / 0.
     * The fractions are then split evenly between the children, which keeps them summing to one, so SHAP local accuracy
     * still holds (importance sums to prediction minus the sample-weighted mean leaf value, 15 here).
     * <p>
     * Expected values are Shapley values of the path-dependent value function v(S), computed by hand:
     * v({}) = 15, v({foo}) = 20 or 10 (by foo), v({bar}) = 15 for bar = 0.3 and 0.5 * 20 + 0.5 * (0.5 * 7 + 0.5 * 9) = 14
     * for bar = 0.7, and v({foo, bar}) is the prediction. Values for documents passing through the zero-sample node depend
     * on the even split: with a left fraction p, foo's importance for (0.3, 0.7) would be -5.25 + p / 2. So do values for
     * documents that never reach the node but that bar alone routes into it: bar's importance for (0.7, 0.7) would be
     * -0.25 - p / 2.
     */
    public void testFeatureImportanceWithZeroSampleInnerNode() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(0.5)
                    .setNumberSamples(4L),
                TreeNode.builder(1)
                    .setSplitFeature(1)
                    .setOperator(Operator.LT)
                    .setLeftChild(3)
                    .setRightChild(4)
                    .setThreshold(0.5)
                    .setNumberSamples(2L),
                TreeNode.builder(2).setLeafValue(20.0).setNumberSamples(2L),
                TreeNode.builder(3).setLeafValue(10.0).setNumberSamples(2L),
                TreeNode.builder(4)
                    .setSplitFeature(0)
                    .setOperator(Operator.LT)
                    .setLeftChild(5)
                    .setRightChild(6)
                    .setThreshold(0.2)
                    .setNumberSamples(0L),
                TreeNode.builder(5).setLeafValue(7.0).setNumberSamples(0L),
                TreeNode.builder(6).setLeafValue(9.0).setNumberSamples(0L)
            )
            .build();

        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        final double eps = 1.0E-8;
        // Never reaches the zero-sample node
        double[][] featureImportance = tree.featureImportance(new double[] { 0.7, 0.3 });
        assertThat(featureImportance[0][0], closeTo(5.0, eps));
        assertThat(featureImportance[1][0], closeTo(0.0, eps));

        // Reaches the populated sibling of the zero-sample node
        featureImportance = tree.featureImportance(new double[] { 0.3, 0.3 });
        assertThat(featureImportance[0][0], closeTo(-5.0, eps));
        assertThat(featureImportance[1][0], closeTo(0.0, eps));

        // Passes through the zero-sample node to its right leaf (9)
        featureImportance = tree.featureImportance(new double[] { 0.3, 0.7 });
        assertThat(featureImportance[0][0], closeTo(-5.0, eps));
        assertThat(featureImportance[1][0], closeTo(-1.0, eps));

        // Passes through the zero-sample node to its left leaf (7)
        featureImportance = tree.featureImportance(new double[] { 0.1, 0.7 });
        assertThat(featureImportance[0][0], closeTo(-6.0, eps));
        assertThat(featureImportance[1][0], closeTo(-2.0, eps));

        // Never reaches the zero-sample node, but bar alone routes it there
        featureImportance = tree.featureImportance(new double[] { 0.7, 0.7 });
        assertThat(featureImportance[0][0], closeTo(5.5, eps));
        assertThat(featureImportance[1][0], closeTo(-0.5, eps));
    }

    /**
     * number_samples is optional, so a model can arrive without it and then every node, including the root, has zero samples.
     * Such a model has no sample weights to compute feature importance from. The zero-sample guard only applies below the
     * root, so this must keep producing NaN rather than plausible looking values from even splits at every node.
     */
    public void testFeatureImportanceWithoutNumberSamplesIsNotMasked() throws IOException {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = Tree.builder()
            .setFeatureNames(featureNames)
            .setNodes(
                TreeNode.builder(0).setSplitFeature(0).setOperator(Operator.LT).setLeftChild(1).setRightChild(2).setThreshold(0.5),
                TreeNode.builder(1).setSplitFeature(1).setOperator(Operator.LT).setLeftChild(3).setRightChild(4).setThreshold(0.5),
                TreeNode.builder(2).setLeafValue(20.0),
                TreeNode.builder(3).setLeafValue(10.0),
                TreeNode.builder(4).setLeafValue(30.0)
            )
            .build();

        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        double[][] featureImportance = tree.featureImportance(new double[] { 0.3, 0.7 });
        assertTrue(Double.isNaN(featureImportance[0][0]));
        assertTrue(Double.isNaN(featureImportance[1][0]));
    }

    public void testMinAndMaxBoundaries() throws IOException {
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), 0.3);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 0.1);
        builder.addLeaf(leftChildNode.getRightChild(), 0.2);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree treeObject = builder.setFeatureNames(featureNames).build();
        TreeInferenceModel tree = deserializeFromTrainedModel(treeObject, xContentRegistry(), TreeInferenceModel::fromXContent);
        tree.rewriteFeatureIndices(Collections.emptyMap());

        assertThat(tree.getMinPredictedValue(), equalTo(0.1));
        assertThat(tree.getMaxPredictedValue(), equalTo(0.3));
    }

    private static Map<String, Object> zipObjMap(List<String> keys, List<? extends Object> values) {
        return IntStream.range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }

}
