/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RawInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NullInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.classificationLabel;

public class Tree implements LenientlyParsedTrainedModel, StrictlyParsedTrainedModel, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Tree.class);
    // TODO should we have regression/classification sub-classes that accept the builder?
    public static final ParseField NAME = new ParseField("tree");

    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField TREE_STRUCTURE = new ParseField("tree_structure");
    public static final ParseField TARGET_TYPE = new ParseField("target_type");
    public static final ParseField CLASSIFICATION_LABELS = new ParseField("classification_labels");

    private static final ObjectParser<Tree.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<Tree.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<Tree.Builder, Void> createParser(boolean lenient) {
        ObjectParser<Tree.Builder, Void> parser = new ObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            Tree.Builder::new);
        parser.declareStringArray(Tree.Builder::setFeatureNames, FEATURE_NAMES);
        parser.declareObjectArray(Tree.Builder::setNodes, (p, c) -> TreeNode.fromXContent(p, lenient), TREE_STRUCTURE);
        parser.declareString(Tree.Builder::setTargetType, TARGET_TYPE);
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
    private final CachedSupplier<Double> highestOrderCategory;

    Tree(List<String> featureNames, List<TreeNode> nodes, TargetType targetType, List<String> classificationLabels) {
        this.featureNames = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(featureNames, FEATURE_NAMES));
        if(ExceptionsHelper.requireNonNull(nodes, TREE_STRUCTURE).size() == 0) {
            throw new IllegalArgumentException("[tree_structure] must not be empty");
        }
        this.nodes = Collections.unmodifiableList(nodes);
        this.targetType = ExceptionsHelper.requireNonNull(targetType, TARGET_TYPE);
        this.classificationLabels = classificationLabels == null ? null : Collections.unmodifiableList(classificationLabels);
        this.highestOrderCategory = new CachedSupplier<>(() -> this.maxLeafValue());
    }

    public Tree(StreamInput in) throws IOException {
        this.featureNames = Collections.unmodifiableList(in.readStringList());
        this.nodes = Collections.unmodifiableList(in.readList(TreeNode::new));
        this.targetType = TargetType.fromStream(in);
        if (in.readBoolean()) {
            this.classificationLabels = Collections.unmodifiableList(in.readStringList());
        } else {
            this.classificationLabels = null;
        }
        this.highestOrderCategory = new CachedSupplier<>(() -> this.maxLeafValue());
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
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config) {
        if (config.isTargetTypeSupported(targetType) == false) {
            throw ExceptionsHelper.badRequestException(
                "Cannot infer using configuration for [{}] when model target_type is [{}]", config.getName(), targetType.toString());
        }

        List<Double> features = featureNames.stream().map(f -> InferenceHelpers.toDouble(fields.get(f))).collect(Collectors.toList());
        return infer(features, config);
    }

    private InferenceResults infer(List<Double> features, InferenceConfig config) {
        TreeNode node = nodes.get(0);
        while(node.isLeaf() == false) {
            node = nodes.get(node.compare(features));
        }
        return buildResult(node.getLeafValue(), config);
    }

    private InferenceResults buildResult(Double value, InferenceConfig config) {
        // Indicates that the config is useless and the caller just wants the raw value
        if (config instanceof NullInferenceConfig) {
            return new RawInferenceResults(value);
        }
        switch (targetType) {
            case CLASSIFICATION:
                ClassificationConfig classificationConfig = (ClassificationConfig) config;
                List<ClassificationInferenceResults.TopClassEntry> topClasses = InferenceHelpers.topClasses(
                    classificationProbability(value),
                    classificationLabels,
                    classificationConfig.getNumTopClasses());
                return new ClassificationInferenceResults(value, classificationLabel(value, classificationLabels), topClasses, config);
            case REGRESSION:
                return new RegressionInferenceResults(value, config);
            default:
                throw new UnsupportedOperationException("unsupported target_type [" + targetType + "] for inference on tree model");
        }
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
    public TargetType targetType() {
        return targetType;
    }

    private List<Double> classificationProbability(double inferenceValue) {
        // If we are classification, we should assume that the inference return value is whole.
        assert inferenceValue == Math.rint(inferenceValue);
        double maxCategory = this.highestOrderCategory.get();
        // If we are classification, we should assume that the largest leaf value is whole.
        assert maxCategory == Math.rint(maxCategory);
        List<Double> list = new ArrayList<>(Collections.nCopies(Double.valueOf(maxCategory + 1).intValue(), 0.0));
        // TODO, eventually have TreeNodes contain confidence levels
        list.set(Double.valueOf(inferenceValue).intValue(), 1.0);
        return list;
    }

    @Override
    public List<String> classificationLabels() {
        return classificationLabels;
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
        builder.field(TARGET_TYPE.getPreferredName(), targetType.toString());
        if(classificationLabels != null) {
            builder.field(CLASSIFICATION_LABELS.getPreferredName(), classificationLabels);
        }
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
        checkTargetType();
        detectMissingNodes();
        detectCycle();
    }

    @Override
    public long estimatedNumOperations() {
        // Grabbing the features from the doc + the depth of the tree
        return (long)Math.ceil(Math.log(nodes.size())) + featureNames.size();
    }

    private void checkTargetType() {
        if ((this.targetType == TargetType.CLASSIFICATION) != (this.classificationLabels != null)) {
            throw ExceptionsHelper.badRequestException(
                "[target_type] should be [classification] if [classification_labels] is provided, and vice versa");
        }
    }

    private void detectCycle() {
        Set<Integer> visited = new HashSet<>(nodes.size());
        Queue<Integer> toVisit = new ArrayDeque<>(nodes.size());
        toVisit.add(0);
        while(toVisit.isEmpty() == false) {
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

    private static boolean nodeMissing(int nodeIdx, List<TreeNode> nodes) {
        return nodeIdx >= nodes.size();
    }

    private Double maxLeafValue() {
        return targetType == TargetType.CLASSIFICATION ?
            this.nodes.stream().filter(TreeNode::isLeaf).mapToDouble(TreeNode::getLeafValue).max().getAsDouble() :
            null;
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
            if (nodes.stream().anyMatch(Objects::isNull)) {
                throw ExceptionsHelper.badRequestException("[tree] cannot contain null nodes");
            }
            return new Tree(featureNames,
                nodes.stream().map(TreeNode.Builder::build).collect(Collectors.toList()),
                targetType,
                classificationLabels);
        }
    }

}
