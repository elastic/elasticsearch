/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    public static Tree createRandom() {
        return buildRandomTree(randomIntBetween(2, 15),  6);
    }

    public static Tree buildRandomTree(int numFeatures, int depth) {

        Tree.Builder builder = Tree.builder();
        List<String> featureNames = new ArrayList<>(numFeatures);
        for(int i = 0; i < numFeatures; i++) {
            featureNames.add(randomAlphaOfLength(10));
        }
        builder.setFeatureNames(featureNames);

        TreeNode.Builder node = builder.addJunction(0, randomInt(numFeatures), true, randomDouble());
        List<Integer> childNodes = List.of(node.getLeftChild(), node.getRightChild());

        for (int i = 0; i < depth -1; i++) {

            List<Integer> nextNodes = new ArrayList<>();
            for (int nodeId : childNodes) {
                if (i == depth -2) {
                    builder.addLeaf(nodeId, randomDouble());
                } else {
                    TreeNode.Builder childNode =
                        builder.addJunction(nodeId, randomInt(numFeatures), true, randomDouble());
                    nextNodes.add(childNode.getLeftChild());
                    nextNodes.add(childNode.getRightChild());
                }
            }
            childNodes = nextNodes;
        }

        return builder.build();
    }

    @Override
    protected Writeable.Reader<Tree> instanceReader() {
        return Tree::new;
    }

    public void testInfer() {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.Builder builder = Tree.builder();
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
        assertEquals(0.3, tree.infer(featureMap), 0.00001);

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureVector = Arrays.asList(0.3, 0.7);
        featureMap = zipObjMap(featureNames, featureVector);
        assertEquals(0.1, tree.infer(featureMap), 0.00001);

        // This should hit the right child of the left child of the root node
        // i.e. it takes the path left, right
        featureVector = Arrays.asList(0.3, 0.9);
        featureMap = zipObjMap(featureNames, featureVector);
        assertEquals(0.2, tree.infer(featureMap), 0.00001);
    }

    public void testTreeWithNullRoot() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> Tree.builder().setNodes(Collections.singletonList(null))
                .build());
        assertThat(ex.getMessage(), equalTo("[tree] must have non-null root node."));
    }

    public void testTreeWithInvalidNode() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> Tree.builder().setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setSplitFeature(1)
                .setThreshold(randomDouble()))
                .build());
        assertThat(ex.getMessage(), equalTo("[tree] contains null or missing nodes [1]"));
    }

    public void testTreeWithNullNode() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> Tree.builder().setNodes(TreeNode.builder(0)
                .setLeftChild(1)
                .setSplitFeature(1)
                .setThreshold(randomDouble()),
                null)
                .build());
        assertThat(ex.getMessage(), equalTo("[tree] contains null or missing nodes [1]"));
    }

    public void testTreeWithCycle() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
            () -> Tree.builder().setNodes(TreeNode.builder(0)
                    .setLeftChild(1)
                    .setSplitFeature(1)
                    .setThreshold(randomDouble()),
                TreeNode.builder(0)
                    .setLeftChild(0)
                    .setSplitFeature(1)
                    .setThreshold(randomDouble()))
                .build());
        assertThat(ex.getMessage(), equalTo("[tree] contains cycle at node 0"));
    }

    private static Map<String, Object> zipObjMap(List<String> keys, List<Double> values) {
        return IntStream.range(0, keys.size()).boxed().collect(Collectors.toMap(keys::get, values::get));
    }
}
