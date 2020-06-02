/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;


public class TreeTests extends AbstractSerializingTestCase<Tree> {

    private final double eps = 1.0E-8;
    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected Tree doParseInstance(XContentParser parser) throws IOException {
        return lenient ? Tree.fromXContentLenient(parser) : Tree.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("feature_names");
    }

    @Override
    protected Tree createTestInstance() {
        return createRandom();
    }

    public static Tree createRandom(TargetType targetType) {
        int numberOfFeatures = randomIntBetween(1, 10);
        List<String> featureNames = new ArrayList<>();
        for (int i = 0; i < numberOfFeatures; i++) {
            featureNames.add(randomAlphaOfLength(10));
        }
        return buildRandomTree(targetType, featureNames,  6);
    }

    public static Tree createRandom() {
        return createRandom(randomFrom(TargetType.values()));
    }

    public static Tree buildRandomTree(TargetType targetType, List<String> featureNames, int depth) {
        Tree.Builder builder = Tree.builder();
        int maxFeatureIndex = featureNames.size() - 1;
        builder.setFeatureNames(featureNames);

        TreeNode.Builder node = builder.addJunction(0, randomInt(maxFeatureIndex), true, randomDouble());
        List<Integer> childNodes = List.of(node.getLeftChild(), node.getRightChild());

        for (int i = 0; i < depth -1; i++) {

            List<Integer> nextNodes = new ArrayList<>();
            for (int nodeId : childNodes) {
                if (i == depth -2) {
                    builder.addLeaf(nodeId, randomDouble());
                } else {
                    TreeNode.Builder childNode =
                        builder.addJunction(nodeId, randomInt(maxFeatureIndex), true, randomDouble());
                    nextNodes.add(childNode.getLeftChild());
                    nextNodes.add(childNode.getRightChild());
                }
            }
            childNodes = nextNodes;
        }
        List<String> categoryLabels = null;
        if (randomBoolean() && targetType == TargetType.CLASSIFICATION) {
            categoryLabels = Arrays.asList(generateRandomStringArray(randomIntBetween(1, 10), randomIntBetween(1, 10), false, false));
        }

        return builder.setTargetType(targetType).setClassificationLabels(categoryLabels).build();
    }

    public static Tree buildRandomTree(List<String> featureNames, int depth) {
        return buildRandomTree(randomFrom(TargetType.values()), featureNames, depth);
    }

    @Override
    protected Writeable.Reader<Tree> instanceReader() {
        return Tree::new;
    }

    public void testInferWithStump() {
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        builder.setRoot(TreeNode.builder(0).setLeafValue(Collections.singletonList(42.0)));
        builder.setFeatureNames(Collections.emptyList());

        Tree tree = builder.build();
        List<String> featureNames = Arrays.asList("foo", "bar");
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector); // does not really matter as this is a stump
        assertThat(42.0,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));
    }

    public void testInfer() {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), 0.3);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 0.1);
        builder.addLeaf(leftChildNode.getRightChild(), 0.2);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree = builder.setFeatureNames(featureNames).build();

        // This feature vector should hit the right child of the root node
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        assertThat(0.3,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureVector = Arrays.asList(0.3, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(0.1,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));

        // This should hit the right child of the left child of the root node
        // i.e. it takes the path left, right
        featureVector = Arrays.asList(0.3, 0.9);
        featureMap = zipObjMap(featureNames, featureVector);
        assertThat(0.2,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));

        // This should still work if the internal values are strings
        List<String> featureVectorStrings = Arrays.asList("0.3", "0.9");
        featureMap = zipObjMap(featureNames, featureVectorStrings);
        assertThat(0.2,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));

        // This should handle missing values and take the default_left path
        featureMap = new HashMap<>(2) {{
            put("foo", 0.3);
            put("bar", null);
        }};
        assertThat(0.1,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));
    }

    public void testInferNestedFields() {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.REGRESSION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), 0.3);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 0.1);
        builder.addLeaf(leftChildNode.getRightChild(), 0.2);

        List<String> featureNames = Arrays.asList("foo.baz", "bar.biz");
        Tree tree = builder.setFeatureNames(featureNames).build();

        // This feature vector should hit the right child of the root node
        Map<String, Object> featureMap = new HashMap<>() {{
            put("foo", new HashMap<>(){{
                put("baz", 0.6);
            }});
            put("bar", new HashMap<>(){{
                put("biz", 0.0);
            }});
        }};
        assertThat(0.3,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureMap = new HashMap<>() {{
            put("foo", new HashMap<>(){{
                put("baz", 0.3);
            }});
            put("bar", new HashMap<>(){{
                put("biz", 0.7);
            }});
        }};
        assertThat(0.1,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));

        // This should hit the right child of the left child of the root node
        // i.e. it takes the path left, right
        featureMap = new HashMap<>() {{
            put("foo", new HashMap<>(){{
                put("baz", 0.3);
            }});
            put("bar", new HashMap<>(){{
                put("biz", 0.9);
            }});
        }};
        assertThat(0.2,
            closeTo(((SingleValueInferenceResults)tree.infer(featureMap, RegressionConfig.EMPTY_PARAMS, Collections.emptyMap())).value(),
                0.00001));
    }

    public void testTreeClassificationProbability() {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.Builder builder = Tree.builder().setTargetType(TargetType.CLASSIFICATION);
        TreeNode.Builder rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.getRightChild(), 1.0);
        TreeNode.Builder leftChildNode = builder.addJunction(rootNode.getLeftChild(), 1, true, 0.8);
        builder.addLeaf(leftChildNode.getLeftChild(), 1.0);
        builder.addLeaf(leftChildNode.getRightChild(), 0.0);

        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree = builder.setFeatureNames(featureNames).setClassificationLabels(Arrays.asList("cat", "dog")).build();

        double eps = 0.000001;
        // This feature vector should hit the right child of the root node
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        List<Double> expectedProbs = Arrays.asList(1.0, 0.0);
        List<String> expectedFields = Arrays.asList("dog", "cat");
        Map<String, Object> featureMap = zipObjMap(featureNames, featureVector);
        List<ClassificationInferenceResults.TopClassEntry> probabilities =
            ((ClassificationInferenceResults)tree.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expectedProbs.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expectedProbs.get(i), eps));
            assertThat(probabilities.get(i).getClassification(), equalTo(expectedFields.get(i)));
        }

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureVector = Arrays.asList(0.3, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        probabilities =
            ((ClassificationInferenceResults)tree.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expectedProbs.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expectedProbs.get(i), eps));
            assertThat(probabilities.get(i).getClassification(), equalTo(expectedFields.get(i)));
        }

        // This should handle missing values and take the default_left path
        featureMap = new HashMap<>(2) {{
            put("foo", 0.3);
            put("bar", null);
        }};
        probabilities =
            ((ClassificationInferenceResults)tree.infer(featureMap, new ClassificationConfig(2), Collections.emptyMap()))
                .getTopClasses();
        for(int i = 0; i < expectedProbs.size(); i++) {
            assertThat(probabilities.get(i).getProbability(), closeTo(expectedProbs.get(i), eps));
            assertThat(probabilities.get(i).getClassification(), equalTo(expectedFields.get(i)));
        }
    }

    public void testTreeWithNullRoot() {
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> Tree.builder()
                .setNodes(Collections.singletonList(null))
                .setFeatureNames(Arrays.asList("foo", "bar"))
                .build());
        assertThat(ex.getMessage(), equalTo("[tree] cannot contain null nodes"));
    }

    public void testTreeWithInvalidNode() {
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> Tree.builder()
                .setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setSplitFeature(1)
                .setThreshold(randomDouble()))
                .setFeatureNames(Arrays.asList("foo", "bar"))
                .build().validate());
        assertThat(ex.getMessage(), equalTo("[tree] contains missing nodes [1]"));
    }

    public void testTreeWithNullNode() {
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> Tree.builder()
                .setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setSplitFeature(1)
                .setThreshold(randomDouble()),
                null)
                .setFeatureNames(Arrays.asList("foo", "bar"))
                .build()
                .validate());
        assertThat(ex.getMessage(), equalTo("[tree] cannot contain null nodes"));
    }

    public void testTreeWithCycle() {
        ElasticsearchStatusException ex = expectThrows(ElasticsearchStatusException.class,
            () -> Tree.builder()
                .setNodes(TreeNode.builder(0)
                    .setLeftChild(1)
                    .setSplitFeature(1)
                    .setThreshold(randomDouble()),
                TreeNode.builder(0)
                    .setLeftChild(0)
                    .setSplitFeature(1)
                    .setThreshold(randomDouble()))
                .setFeatureNames(Arrays.asList("foo", "bar"))
                .build()
                .validate());
        assertThat(ex.getMessage(), equalTo("[tree] contains cycle at node 0"));
    }

    public void testTreeWithTargetTypeAndLabelsMismatch() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        String msg = "[target_type] should be [classification] if [classification_labels] are provided";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            Tree.builder()
                .setRoot(TreeNode.builder(0)
                        .setLeftChild(1)
                        .setSplitFeature(1)
                        .setThreshold(randomDouble()))
                .setFeatureNames(featureNames)
                .setClassificationLabels(Arrays.asList("label1", "label2"))
                .build()
                .validate();
        });
        assertThat(ex.getMessage(), equalTo(msg));
    }

    public void testOperationsEstimations() {
        Tree tree = buildRandomTree(Arrays.asList("foo", "bar", "baz"), 5);
        assertThat(tree.estimatedNumOperations(), equalTo(7L));
    }

    public void testFeatureImportance() {
        List<String> featureNames = Arrays.asList("foo", "bar");
        Tree tree = Tree.builder()
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
                TreeNode.builder(6).setLeafValue(18.0).setNumberSamples(1L)).build();

        Map<String, double[]> featureImportance = tree.featureImportance(zipObjMap(featureNames, Arrays.asList(0.25, 0.25)),
            Collections.emptyMap());
        assertThat(featureImportance.get("foo")[0], closeTo(-5.0, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(-2.5, eps));

        featureImportance = tree.featureImportance(zipObjMap(featureNames, Arrays.asList(0.25, 0.75)), Collections.emptyMap());
        assertThat(featureImportance.get("foo")[0], closeTo(-5.0, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(2.5, eps));

        featureImportance = tree.featureImportance(zipObjMap(featureNames, Arrays.asList(0.75, 0.25)), Collections.emptyMap());
        assertThat(featureImportance.get("foo")[0], closeTo(5.0, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(-2.5, eps));

        featureImportance = tree.featureImportance(zipObjMap(featureNames, Arrays.asList(0.75, 0.75)), Collections.emptyMap());
        assertThat(featureImportance.get("foo")[0], closeTo(5.0, eps));
        assertThat(featureImportance.get("bar")[0], closeTo(2.5, eps));
    }

    public void testMaxFeatureIndex() {

        int numFeatures = randomIntBetween(1, 15);
        // We need a tree where every feature is used, choose a depth big enough to
        // accommodate those non-leave nodes (leaf nodes don't have a feature index)
        int depth = (int) Math.ceil(Math.log(numFeatures +1) / Math.log(2)) + 1;
        List<String> featureNames = new ArrayList<>(numFeatures);
        for (int i=0; i<numFeatures; i++) {
            featureNames.add("feature" + i);
        }

        Tree.Builder builder = Tree.builder().setFeatureNames(featureNames);

        // build a tree using feature indices 0..numFeatures -1
        int featureIndex = 0;
        TreeNode.Builder node = builder.addJunction(0, featureIndex++, true, randomDouble());
        List<Integer> childNodes = List.of(node.getLeftChild(), node.getRightChild());

        for (int i = 0; i < depth -1; i++) {
            List<Integer> nextNodes = new ArrayList<>();
            for (int nodeId : childNodes) {
                if (i == depth -2) {
                    builder.addLeaf(nodeId, randomDouble());
                } else {
                    TreeNode.Builder childNode =
                            builder.addJunction(nodeId, featureIndex++ % numFeatures, true, randomDouble());
                    nextNodes.add(childNode.getLeftChild());
                    nextNodes.add(childNode.getRightChild());
                }
            }
            childNodes = nextNodes;
        }

        Tree tree = builder.build();

        assertEquals(numFeatures, tree.maxFeatureIndex() +1);
    }

    public void testMaxFeatureIndexSingleNodeTree() {
        Tree tree = Tree.builder()
                .setRoot(TreeNode.builder(0).setLeafValue(10.0))
                .setFeatureNames(Collections.emptyList())
                .build();

        assertEquals(-1, tree.maxFeatureIndex());
    }

    public void testValidateGivenMissingFeatures() {
        List<String> featureNames = Arrays.asList("foo", "bar", "baz");

        // build a tree referencing a feature at index 3 which is not in the featureNames list
        Tree.Builder builder = Tree.builder().setFeatureNames(featureNames);
        builder.addJunction(0, 0, true, randomDouble());
        builder.addJunction(1, 1, true, randomDouble());
        builder.addJunction(2, 3, true, randomDouble());
        builder.addLeaf(3, randomDouble());
        builder.addLeaf(4, randomDouble());
        builder.addLeaf(5, randomDouble());
        builder.addLeaf(6, randomDouble());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> builder.build().validate());
        assertThat(e.getDetailedMessage(), containsString("feature index [3] is out of bounds for the [feature_names] array"));
    }

    public void testValidateGivenTreeWithNoFeatures() {
        Tree.builder()
                .setRoot(TreeNode.builder(0).setLeafValue(10.0))
                .setFeatureNames(Collections.emptyList())
                .build()
                .validate();
    }

    private static Map<String, Object> zipObjMap(List<String> keys, List<? extends Object> values) {
        return IntStream.range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }
}
