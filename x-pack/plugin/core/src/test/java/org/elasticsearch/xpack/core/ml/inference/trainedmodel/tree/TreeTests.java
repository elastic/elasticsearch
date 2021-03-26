/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;


public class TreeTests extends AbstractSerializingTestCase<Tree> {

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

    public void testTreeWithEmptyFeaturesAndOneNode() {
        // Shouldn't throw
        Tree.builder()
            .setRoot(TreeNode.builder(0).setLeafValue(10.0))
            .setFeatureNames(Collections.emptyList())
            .build()
            .validate();
    }

    public void testTreeWithEmptyFeaturesAndThreeNodes() {
        String msg = "[feature_names] is empty and the tree has > 1 nodes; num nodes [3]. " +
            "The model Must have features if tree is not a stump";
        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> {
            Tree.builder()
                .setRoot(TreeNode.builder(0)
                    .setLeftChild(1)
                    .setRightChild(2)
                    .setThreshold(randomDouble()))
                .addNode(TreeNode.builder(1)
                    .setLeafValue(randomDouble()))
                .addNode(TreeNode.builder(2)
                    .setLeafValue(randomDouble()))
                .setFeatureNames(Collections.emptyList())
                .build()
                .validate();
        });
        assertThat(ex.getMessage(), equalTo(msg));
    }

    public void testOperationsEstimations() {
        Tree tree = buildRandomTree(Arrays.asList("foo", "bar", "baz"), 5);
        assertThat(tree.estimatedNumOperations(), equalTo(7L));
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


}
