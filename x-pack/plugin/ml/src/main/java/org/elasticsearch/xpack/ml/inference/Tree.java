/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * A decision tree that can make predictions given a feature vector
 */
public class Tree {

    private final List<Node> nodes;

    Tree(List<Node> nodes) {
        this.nodes = Collections.unmodifiableList(nodes);
    }

    /**
     * Trace the route predicting on the feature vector takes.
     * @param features  The feature vector
     * @return The list of traversed nodes ordered from root to leaf
     */
    public List<Node> trace(List<Double> features) {
        return trace(features, 0, new ArrayList<>());
    }

    private List<Node> trace(List<Double> features, int nodeIndex, List<Node> visited) {
        Node node = nodes.get(nodeIndex);
        visited.add(node);
        if (node.isLeaf()) {
            return visited;
        }

        int nextNode = node.compare(features);
        return trace(features, nextNode, visited);
    }

    /**
     * Make a prediction based on the feature vector
     * @param features  The feature vector
     * @return The prediction
     */
    public Double predict(List<Double> features) {
        return predict(features, 0);
    }

    private Double predict(List<Double> features, int nodeIndex) {
        Node node = nodes.get(nodeIndex);
        if (node.isLeaf()) {
            return node.value();
        }

        int nextNode = node.compare(features);
        return predict(features, nextNode);
    }

    /**
     * Finds null nodes
     * @return List of indexes to null nodes
     */
    List<Integer> missingNodes() {
        List<Integer> nullNodeIndices = new ArrayList<>();
        for (int i=0; i<nodes.size(); i++) {
            if (nodes.get(i) == null) {
                nullNodeIndices.add(i);
            }
        }
        return nullNodeIndices;
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    static class Node {
        int leftChild;
        int rightChild;
        int featureIndex;
        boolean isDefaultLeft;
        double thresholdValue;
        BiPredicate<Double, Double> operator;

        Node(int leftChild, int rightChild, int featureIndex, boolean isDefaultLeft, double thresholdValue) {
            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.featureIndex = featureIndex;
            this.isDefaultLeft = isDefaultLeft;
            this.thresholdValue = thresholdValue;
            this.operator = (value, threshold) -> value < threshold;   // less than
        }

        Node(int leftChild, int rightChild, int featureIndex, boolean isDefaultLeft, double thresholdValue,
                     BiPredicate<Double, Double> operator) {
            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.featureIndex = featureIndex;
            this.isDefaultLeft = isDefaultLeft;
            this.thresholdValue = thresholdValue;
            this.operator = operator;
        }

        Node(double value) {
            this(-1, -1, -1, false, value);
        }

        boolean isLeaf() {
            return leftChild < 1;
        }

        int compare(List<Double> features) {
            Double feature = features.get(featureIndex);
            if (isMissing(feature)) {
                return isDefaultLeft ? leftChild : rightChild;
            }

            return operator.test(feature, thresholdValue) ? leftChild : rightChild;
        }

        boolean isMissing(Double feature) {
            return feature == null;
        }

        Double value() {
            return thresholdValue;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("{\n");
            builder.append("left: ").append(leftChild).append('\n');
            builder.append("right: ").append(rightChild).append('\n');
            builder.append("isDefaultLeft: ").append(isDefaultLeft).append('\n');
            builder.append("isLeaf: ").append(isLeaf()).append('\n');
            builder.append("featureIndex: ").append(featureIndex).append('\n');
            builder.append("value: ").append(thresholdValue).append('\n');
            builder.append("}\n");
            return builder.toString();
        }
    }


    public static class TreeBuilder {

        private final ArrayList<Node> nodes;
        private int numNodes;

        public static TreeBuilder newTreeBuilder() {
            return new TreeBuilder();
        }

        TreeBuilder() {
            nodes = new ArrayList<>();
            // allocate space in the root node and set to a leaf
            nodes.add(null);
            addLeaf(0, 0.0);
            numNodes = 1;
        }

        /**
         * Add a decision node. Space for the child nodes is allocated
         * @param nodeIndex         Where to place the node. This is either 0 (root) or an existing child node index
         * @param featureIndex      The feature index the decision is made on
         * @param isDefaultLeft     Default left branch if the feature is missing
         * @param decisionThreshold The decision threshold
         * @return The created node
         */
        public Node addJunction(int nodeIndex, int featureIndex, boolean isDefaultLeft, double decisionThreshold) {
            assert nodeIndex < nodes.size();

            int leftChild = numNodes++;
            int rightChild = numNodes++;
            nodes.ensureCapacity(numNodes);

            Node node = new Node(leftChild, rightChild, featureIndex, isDefaultLeft, decisionThreshold);
            nodes.set(nodeIndex, node);
            // allocate space for the child nodes
            nodes.add(null);
            nodes.add(null);
            assert nodes.size() == numNodes;

            return node;
        }

        /**
         * Sets the node at {@code nodeIndex} to a leaf node.
         * @param nodeIndex The index as allocated by a call to {@link #addJunction(int, int, boolean, double)}
         * @param value     The prediction value
         * @return this
         */
        public TreeBuilder addLeaf(int nodeIndex, double value) {
            assert nodeIndex < nodes.size();
            nodes.set(nodeIndex, new Node(value));
            return this;
        }

        public Tree build() {
            return new Tree(nodes);
        }
    }
}
