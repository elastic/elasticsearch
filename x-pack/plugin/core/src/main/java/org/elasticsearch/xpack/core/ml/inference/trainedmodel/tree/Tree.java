/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class Tree implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Tree.class);
    // TODO should we have regression/classification sub-classes that accept the builder?
    public static final ParseField NAME = new ParseField("tree");

    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField TREE_STRUCTURE = new ParseField("tree_structure");
    public static final ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");

    private static final ObjectParser<Tree.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<Tree.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Tree.Builder, Void> createParser(boolean lenient) {
        ObjectParser<Tree.Builder, Void> parser = new ObjectParser<>(NAME.getPreferredName(), lenient, Tree.Builder::new);
        parser.declareStringArray(Tree.Builder::setFeatureNames, FEATURE_NAMES);
        parser.declareObjectArray(Tree.Builder::setNodes, (p, c) -> TreeNode.fromXContent(p, lenient), TREE_STRUCTURE);
        parser.declareString(Tree.Builder::setTargetType, TargetType.TARGET_TYPE);
        parser.declareStringArray(Tree.Builder::setClassificationLabels, CLASSIFICATION_LABELS);
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
    private final TargetType targetType;
    private final List<String> classificationLabels;

    Tree(List<String> featureNames, List<TreeNode> nodes, TargetType targetType, List<String> classificationLabels) {
        this.featureNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(featureNames, FEATURE_NAMES));
        if (ExceptionsHelper.requireNonNull(nodes, TREE_STRUCTURE).size() == 0) {
            throw new IllegalArgumentException("[tree_structure] must not be empty");
        }
        this.nodes = Collections.unmodifiableList(nodes);
        this.targetType = ExceptionsHelper.requireNonNull(targetType, TargetType.TARGET_TYPE);
        this.classificationLabels = classificationLabels == null ? null : Collections.unmodifiableList(classificationLabels);
    }

    public Tree(StreamInput in) throws IOException {
        this.featureNames = in.readImmutableList(StreamInput::readString);
        this.nodes = in.readImmutableList(TreeNode::new);
        this.targetType = TargetType.fromStream(in);
        if (in.readBoolean()) {
            this.classificationLabels = in.readImmutableList(StreamInput::readString);
        } else {
            this.classificationLabels = null;
        }
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public TargetType targetType() {
        return targetType;
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(featureNames);
        out.writeCollection(nodes);
        targetType.writeTo(out);
        out.writeBoolean(classificationLabels != null);
        if (classificationLabels != null) {
            out.writeStringCollection(classificationLabels);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FEATURE_NAMES.getPreferredName(), featureNames);
        builder.field(TREE_STRUCTURE.getPreferredName(), nodes);
        builder.field(TargetType.TARGET_TYPE.getPreferredName(), targetType.toString());
        if (classificationLabels != null) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
        builder.endObject();
        return builder;
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
            && Objects.equals(nodes, that.nodes)
            && Objects.equals(targetType, that.targetType)
            && Objects.equals(classificationLabels, that.classificationLabels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(featureNames, nodes, targetType, classificationLabels);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void validate() {
        int maxFeatureIndex = maxFeatureIndex();
        if (maxFeatureIndex >= featureNames.size()) {
            throw ExceptionsHelper.badRequestException(
                "feature index [{}] is out of bounds for the [{}] array",
                maxFeatureIndex,
                FEATURE_NAMES.getPreferredName()
            );
        }
        if (nodes.size() > 1) {
            if (featureNames.isEmpty()) {
                throw ExceptionsHelper.badRequestException(
                    "[{}] is empty and the tree has > 1 nodes; num nodes [{}]. " + "The model Must have features if tree is not a stump",
                    FEATURE_NAMES.getPreferredName(),
                    nodes.size()
                );
            }
        }
        checkTargetType();
        detectMissingNodes();
        detectCycle();
        verifyLeafNodeUniformity();
    }

    @Override
    public long estimatedNumOperations() {
        // Grabbing the features from the doc + the depth of the tree
        return (long) Math.ceil(Math.log(nodes.size())) + featureNames.size();
    }

    /**
     * The highest index of a feature used any of the nodes.
     * If no nodes use a feature return -1. This can only happen
     * if the tree contains a single leaf node.
     *
     * @return The max or -1
     */
    int maxFeatureIndex() {
        int maxFeatureIndex = -1;

        for (TreeNode node : nodes) {
            maxFeatureIndex = Math.max(maxFeatureIndex, node.getSplitFeature());
        }

        return maxFeatureIndex;
    }

    private void checkTargetType() {
        if (this.classificationLabels != null && this.targetType != TargetType.CLASSIFICATION) {
            throw ExceptionsHelper.badRequestException("[target_type] should be [classification] if [classification_labels] are provided");
        }
        if (this.targetType != TargetType.CLASSIFICATION && this.nodes.stream().anyMatch(n -> n.getLeafValue().length > 1)) {
            throw ExceptionsHelper.badRequestException("[target_type] should be [classification] if leaf nodes have multiple values");
        }
    }

    private void detectCycle() {
        Set<Integer> visited = Sets.newHashSetWithExpectedSize(nodes.size());
        Queue<Integer> toVisit = new ArrayDeque<>(nodes.size());
        toVisit.add(0);
        while (toVisit.isEmpty() == false) {
            Integer nodeIdx = toVisit.remove();
            if (visited.contains(nodeIdx)) {
                throw ExceptionsHelper.badRequestException("[tree] contains cycle at node {}", nodeIdx);
            }
            visited.add(nodeIdx);
            TreeNode treeNode = nodes.get(nodeIdx);
            if (treeNode.getLeftChild() >= 0) {
                toVisit.add(treeNode.getLeftChild());
            }
            if (treeNode.getRightChild() >= 0) {
                toVisit.add(treeNode.getRightChild());
            }
        }
    }

    private void detectMissingNodes() {
        List<Integer> missingNodes = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            TreeNode currentNode = nodes.get(i);
            if (currentNode == null) {
                continue;
            }
            if (nodeMissing(currentNode.getLeftChild(), nodes)) {
                missingNodes.add(currentNode.getLeftChild());
            }
            if (nodeMissing(currentNode.getRightChild(), nodes)) {
                missingNodes.add(currentNode.getRightChild());
            }
        }
        if (missingNodes.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("[tree] contains missing nodes {}", missingNodes);
        }
    }

    private void verifyLeafNodeUniformity() {
        Integer leafValueLengths = null;
        for (TreeNode node : nodes) {
            if (node.isLeaf()) {
                if (leafValueLengths == null) {
                    leafValueLengths = node.getLeafValue().length;
                } else if (leafValueLengths != node.getLeafValue().length) {
                    throw ExceptionsHelper.badRequestException("[tree.tree_structure] all leaf nodes must have the same number of values");
                }
            }
        }
    }

    private static boolean nodeMissing(int nodeIdx, List<TreeNode> nodes) {
        return nodeIdx >= nodes.size();
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOfCollection(classificationLabels);
        size += RamUsageEstimator.sizeOfCollection(featureNames);
        size += RamUsageEstimator.sizeOfCollection(nodes);
        return size;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        List<Accountable> accountables = new ArrayList<>(nodes.size());
        for (TreeNode node : nodes) {
            accountables.add(Accountables.namedAccountable("tree_node_" + node.getNodeIndex(), node));
        }
        return Collections.unmodifiableCollection(accountables);
    }

    @Override
    public TransportVersion getMinimalCompatibilityVersion() {
        if (nodes.stream().filter(TreeNode::isLeaf).anyMatch(t -> t.getLeafValue().length > 1)) {
            return TransportVersion.V_7_7_0;
        }
        return TransportVersion.V_7_6_0;
    }

    public static class Builder {
        private List<String> featureNames;
        private ArrayList<TreeNode.Builder> nodes;
        private int numNodes;
        private TargetType targetType = TargetType.REGRESSION;
        private List<String> classificationLabels;

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

        public Builder setRoot(TreeNode.Builder root) {
            nodes.set(0, root);
            return this;
        }

        public Builder addNode(TreeNode.Builder node) {
            nodes.add(node);
            return this;
        }

        public Builder setNodes(List<TreeNode.Builder> nodes) {
            this.nodes = new ArrayList<>(ExceptionsHelper.requireNonNull(nodes, TREE_STRUCTURE.getPreferredName()));
            return this;
        }

        public Builder setNodes(TreeNode.Builder... nodes) {
            return setNodes(Arrays.asList(nodes));
        }

        public Builder setTargetType(TargetType targetType) {
            this.targetType = targetType;
            return this;
        }

        public Builder setClassificationLabels(List<String> classificationLabels) {
            this.classificationLabels = classificationLabels;
            return this;
        }

        private void setTargetType(String targetType) {
            this.targetType = TargetType.fromString(targetType);
        }

        /**
         * Add a decision node. Space for the child nodes is allocated
         * @param nodeIndex         Where to place the node. This is either 0 (root) or an existing child node index
         * @param featureIndex      The feature index the decision is made on
         * @param isDefaultLeft     Default left branch if the feature is missing
         * @param decisionThreshold The decision threshold
         * @return The created node
         */
        public TreeNode.Builder addJunction(int nodeIndex, int featureIndex, boolean isDefaultLeft, double decisionThreshold) {
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

        /**
         * Sets the node at {@code nodeIndex} to a leaf node.
         * @param nodeIndex The index as allocated by a call to {@link #addJunction(int, int, boolean, double)}
         * @param value     The prediction value
         * @return this
         */
        public Tree.Builder addLeaf(int nodeIndex, double value) {
            return addLeaf(nodeIndex, Arrays.asList(value));
        }

        public Tree.Builder addLeaf(int nodeIndex, List<Double> value) {
            for (int i = nodes.size(); i < nodeIndex + 1; i++) {
                nodes.add(null);
            }
            nodes.set(nodeIndex, TreeNode.builder(nodeIndex).setLeafValue(value));
            return this;
        }

        public Tree build() {
            if (nodes.stream().anyMatch(Objects::isNull)) {
                throw ExceptionsHelper.badRequestException("[tree] cannot contain null nodes");
            }
            return new Tree(
                featureNames,
                nodes.stream().map(TreeNode.Builder::build).collect(Collectors.toList()),
                targetType,
                classificationLabels
            );
        }
    }

}
