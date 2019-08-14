/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;

public class TreeTests extends ESTestCase {

    public static Tree buildRandomTree(int numFeatures, int depth) {

        Tree.TreeBuilder builder = Tree.TreeBuilder.newTreeBuilder();

        Tree.Node node = builder.addJunction(0, randomFeatureIndex(numFeatures), true, randomDecisionThreshold());
        List<Integer> childNodes = List.of(node.leftChild, node.rightChild);

        for (int i=0; i<depth -1; i++) {

            List<Integer> nextNodes = new ArrayList<>();
            for (int nodeId : childNodes) {

                if (i == depth -2) {
                    builder.addLeaf(nodeId, randomDecisionThreshold());
                } else {
                    Tree.Node childNode = builder.addJunction(nodeId, randomFeatureIndex(numFeatures), true, randomDecisionThreshold());
                    nextNodes.add(childNode.leftChild);
                    nextNodes.add(childNode.rightChild);
                }
            }

            childNodes = nextNodes;
        }

        return builder.build();
    }

    static int randomFeatureIndex(int max) {
        return randomIntBetween(0, max -1);
    }

    static double randomDecisionThreshold() {
        return randomDouble();
    }

    public void testPredict() {
        // Build a tree with 2 nodes and 3 leaves using 2 features
        // The leaves have unique values 0.1, 0.2, 0.3
        Tree.TreeBuilder builder = Tree.TreeBuilder.newTreeBuilder();
        Tree.Node rootNode = builder.addJunction(0, 0, true, 0.5);
        builder.addLeaf(rootNode.rightChild, 0.3);
        Tree.Node leftChildNode = builder.addJunction(rootNode.leftChild, 1, true, 0.8);
        builder.addLeaf(leftChildNode.leftChild, 0.1);
        builder.addLeaf(leftChildNode.rightChild, 0.2);

        Tree tree = builder.build();

        // This feature vector should hit the right child of the root node
        List<Double> featureVector = Arrays.asList(0.6, 0.0);
        assertEquals(0.3, tree.predict(featureVector), 0.00001);

        // This should hit the left child of the left child of the root node
        // i.e. it takes the path left, left
        featureVector = Arrays.asList(0.3, 0.7);
        assertEquals(0.1, tree.predict(featureVector), 0.00001);

        // This should hit the right child of the left child of the root node
        // i.e. it takes the path left, right
        featureVector = Arrays.asList(0.3, 0.8);
        assertEquals(0.2, tree.predict(featureVector), 0.00001);
    }

    public void testTrace() {
        int numFeatures = randomIntBetween(1, 6);
        int depth = 6;
        Tree tree = buildRandomTree(numFeatures, depth);

        List<Double> features = new ArrayList<>(numFeatures);
        for (int i=0; i<numFeatures; i++) {
            features.add(randomDecisionThreshold());
        }

        List<Tree.Node> trace = tree.trace(features);
        assertThat(trace, hasSize(depth));
        for (int i=0; i<trace.size() -2; i++) {
            assertFalse(trace.get(i).isLeaf());
        }
        assertTrue(trace.get(trace.size() -1).isLeaf());

        double prediction = tree.predict(features);
        assertEquals(trace.get(trace.size() -1).value(), prediction, 0.0001);

        // Because the tree is built up breadth first we can figure out o
        // a node's id from its child nodes. Then we can trace the route
        // taken an assert it's the branch decisions were correct

        int expectedNodeId = 0;
        for (Tree.Node visitedNode: trace) {
            if (visitedNode.isLeaf() == false) {
                // Imagine the nodes array is 1 based index. The root node
                // has index 1, its children 2 & 3. Because the tree is built
                // breadth first node 2 children will be at indexes 4 & 5 and
                // node 3 children are at 6 & 7.
                // So a nodes children are at nodeindex * 2 and nodeindex * 2 +1
                // and the parent is at nodeindex / 2.
                // The +/- 1's are adjusting for a 0 based index
                int nodeId = ((visitedNode.leftChild + 1) / 2) - 1;
                assertEquals(expectedNodeId, nodeId);

                // unfortunately this doesn't apply to leaf nodes
                // as their children are -1

                expectedNodeId = visitedNode.compare(features);
            } else {
                assertEquals(prediction, visitedNode.value(), 0.0001);
            }
        }

        assertThat(tree.missingNodes(), hasSize(0));
    }

    public void testCompare() {
        int leftChild = 1;
        int rightChild = 2;
        Tree.Node node = new Tree.Node(leftChild, rightChild, 0, true, 0.5);

        List<Double> features = List.of(0.1);
        assertEquals(leftChild, node.compare(features));

        features = List.of(0.9);
        assertEquals(rightChild, node.compare(features));
    }

    public void testCompare_nonDefaultOperator() {
        int leftChild = 1;
        int rightChild = 2;
        Tree.Node node = new Tree.Node(leftChild, rightChild, 0, true, 0.5, (value, threshold) -> value >= threshold);

        List<Double> features = List.of(0.1);
        assertEquals(rightChild, node.compare(features));
        features = List.of(0.5);
        assertEquals(leftChild, node.compare(features));
        features = List.of(0.9);
        assertEquals(leftChild, node.compare(features));

        node = new Tree.Node(leftChild, rightChild, 0, true, 0.5, (value, threshold) -> value <= threshold);

        features = List.of(0.1);
        assertEquals(leftChild, node.compare(features));
        features = List.of(0.5);
        assertEquals(leftChild, node.compare(features));
        features = List.of(0.9);
        assertEquals(rightChild, node.compare(features));
    }

    public void testCompare_missingFeature() {
        int leftChild = 1;
        int rightChild = 2;
        Tree.Node leftBiasNode = new Tree.Node(leftChild, rightChild, 0, true, 0.5);
        List<Double> features = new ArrayList<>();
        features.add(null);
        assertEquals(leftChild, leftBiasNode.compare(features));

        Tree.Node rightBiasNode = new Tree.Node(leftChild, rightChild, 0, false, 0.5);
        assertEquals(rightChild, rightBiasNode.compare(features));
    }

    public void testIsLeaf() {
        Tree.Node leaf = new Tree.Node(0.0);
        assertTrue(leaf.isLeaf());

        Tree.Node node = new Tree.Node(1, 2, 0, false, 0.0);
        assertFalse(node.isLeaf());
    }

    public void testMissingNodes() {
        Tree.TreeBuilder builder = Tree.TreeBuilder.newTreeBuilder();
        Tree.Node rootNode = builder.addJunction(0, 0, true, randomDecisionThreshold());

        Tree.Node node2 = builder.addJunction(rootNode.rightChild, 0, false, 0.1);
        builder.addLeaf(node2.leftChild, 0.1);

        List<Integer> missingNodeIndexes = builder.build().missingNodes();
        assertEquals(Arrays.asList(1, 4), missingNodeIndexes);
    }
}
