/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class Tree implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel {

    public static final ParseField NAME = new ParseField("tree");

    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField TREE_STRUCTURE = new ParseField("tree_structure");

    private static final ObjectParser<Tree.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<Tree.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Tree.Builder, Void> createParser(boolean lenient) {
        ObjectParser<Tree.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            Tree.Builder::new);
        parser.declareStringArray(Tree.Builder::setFeatureNames, FEATURE_NAMES);
        parser.declareObjectArray(Tree.Builder::setNodes, (p, c) -> TreeNode.fromXContent(p, lenient), TREE_STRUCTURE);
        return parser;
    }

    public static Tree fromXContentStrict(XContentParser parser) {
        return STRICT_PARSER.apply(parser, null).build();
    }

    public static Tree fromXContentLenient(XContentParser parser) {
        return LENIENT_PARSER.apply(parser, null).build();
    }

    private final List<String> featureNames;
    private final List<TreeNode> nodes;

    Tree(List<String> featureNames, List<TreeNode> nodes) {
        this.featureNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(featureNames, FEATURE_NAMES));
        this.nodes = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(nodes, TREE_STRUCTURE));
    }

    public Tree(StreamInput in) throws IOException {
        this.featureNames = Collections.unmodifiableList(in.readStringList());
        this.nodes = Collections.unmodifiableList(in.readList(TreeNode::new));
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public List<String> getFeatureNames() {
        return featureNames;
    }

    public List<TreeNode> getNodes() {
        return nodes;
    }

    @Override
    public double infer(Map<String, Object> fields) {
        List<Double> features = featureNames.stream().map(f -> (Double) fields.get(f)).collect(Collectors.toList());
        return infer(features);
    }

    private double infer(List<Double> features) {
        TreeNode node = nodes.get(0);
        while(node.isLeaf() == false) {
            node = nodes.get(node.compare(features));
        }
        return node.getLeafValue();
    }

    /**
     * Trace the route predicting on the feature vector takes.
     * @param features  The feature vector
     * @return The list of traversed nodes ordered from root to leaf
     */
    public List<TreeNode> trace(List<Double> features) {
        List<TreeNode> visited = new ArrayList<>();
        TreeNode node = nodes.get(0);
        visited.add(node);
        while(node.isLeaf() == false) {
            node = nodes.get(node.compare(features));
            visited.add(node);
        }
        return visited;
    }

    @Override
    public boolean isClassification() {
        return false;
    }

    @Override
    public List<Double> inferProbabilities(Map<String, Object> fields) {
        throw new UnsupportedOperationException("Cannot infer probabilities against a regression model.");
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(featureNames);
        out.writeCollection(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAMES.getPreferredName(), featureNames);
        builder.field(TREE_STRUCTURE.getPreferredName(), nodes);
        builder.endObject();
        return  builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tree that = (Tree) o;
        return Objects.equals(featureNames, that.featureNames)
            && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureNames, nodes);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<String> featureNames;
        private ArrayList<TreeNode.Builder> nodes;
        private int numNodes;

        public Builder() {
            nodes = new ArrayList<>();
            // allocate space in the root node and set to a leaf
            nodes.add(null);
            addLeaf(0, 0.0);
            numNodes = 1;
        }

        public Builder setFeatureNames(List<String> featureNames) {
            this.featureNames = featureNames;
            return this;
        }

        public Builder addNode(TreeNode.Builder node) {
            nodes.add(node);
            return this;
        }

        public Builder setNodes(List<TreeNode.Builder> nodes) {
            this.nodes = new ArrayList<>(nodes);
            return this;
        }

        public Builder setNodes(TreeNode.Builder... nodes) {
            return setNodes(Arrays.asList(nodes));
        }

        /**
         * Add a decision node. Space for the child nodes is allocated
         * @param nodeIndex         Where to place the node. This is either 0 (root) or an existing child node index
         * @param featureIndex      The feature index the decision is made on
         * @param isDefaultLeft     Default left branch if the feature is missing
         * @param decisionThreshold The decision threshold
         * @return The created node
         */
        TreeNode.Builder addJunction(int nodeIndex, int featureIndex, boolean isDefaultLeft, double decisionThreshold) {
            int leftChild = numNodes++;
            int rightChild = numNodes++;
            nodes.ensureCapacity(nodeIndex + 1);
            for (int i = nodes.size(); i < nodeIndex + 1; i++) {
                nodes.add(null);
            }

            TreeNode.Builder node = TreeNode.builder(nodeIndex)
                .setDefaultLeft(isDefaultLeft)
                .setLeftChild(leftChild)
                .setRightChild(rightChild)
                .setSplitFeature(featureIndex)
                .setThreshold(decisionThreshold);
            nodes.set(nodeIndex, node);

            // allocate space for the child nodes
            while (nodes.size() <= rightChild) {
                nodes.add(null);
            }

            return node;
        }

        void detectCycle(List<TreeNode.Builder> nodes) {
            if (nodes.isEmpty()) {
                return;
            }
            Set<Integer> visited = new HashSet<>();
            Queue<Integer> toVisit = new ArrayDeque<>(nodes.size());
            toVisit.add(0);
            while(toVisit.isEmpty() == false) {
                Integer nodeIdx = toVisit.remove();
                if (visited.contains(nodeIdx)) {
                    throw new IllegalArgumentException("[tree] contains cycle at node " + nodeIdx);
                }
                visited.add(nodeIdx);
                TreeNode.Builder treeNode = nodes.get(nodeIdx);
                if (treeNode.getLeftChild() != null) {
                    toVisit.add(treeNode.getLeftChild());
                }
                if (treeNode.getRightChild() != null) {
                    toVisit.add(treeNode.getRightChild());
                }
            }
        }

        void detectNullOrMissingNode(List<TreeNode.Builder> nodes) {
            if (nodes.isEmpty()) {
                return;
            }
            if (nodes.get(0) == null) {
                throw new IllegalArgumentException("[tree] must have non-null root node.");
            }
            List<Integer> nullOrMissingNodes = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                TreeNode.Builder currentNode = nodes.get(i);
                if (currentNode == null) {
                    continue;
                }
                if (nodeNullOrMissing(currentNode.getLeftChild())) {
                    nullOrMissingNodes.add(currentNode.getLeftChild());
                }
                if (nodeNullOrMissing(currentNode.getRightChild())) {
                    nullOrMissingNodes.add(currentNode.getRightChild());
                }
            }
            if (nullOrMissingNodes.isEmpty() == false) {
                throw new IllegalArgumentException("[tree] contains null or missing nodes " + nullOrMissingNodes);
            }
        }

        private boolean nodeNullOrMissing(Integer nodeIdx) {
            if (nodeIdx == null) {
                return false;
            }
            return nodeIdx >= nodes.size() || nodes.get(nodeIdx) == null;
        }

        /**
         * Sets the node at {@code nodeIndex} to a leaf node.
         * @param nodeIndex The index as allocated by a call to {@link #addJunction(int, int, boolean, double)}
         * @param value     The prediction value
         * @return this
         */
        Tree.Builder addLeaf(int nodeIndex, double value) {
            for (int i = nodes.size(); i < nodeIndex + 1; i++) {
                nodes.add(null);
            }
            nodes.set(nodeIndex, TreeNode.builder(nodeIndex).setLeafValue(value));
            return this;
        }

        public Tree build() {
            detectNullOrMissingNode(nodes);
            detectCycle(nodes);
            return new Tree(featureNames,
                nodes.stream().map(TreeNode.Builder::build).collect(Collectors.toList()));
        }
    }

}
