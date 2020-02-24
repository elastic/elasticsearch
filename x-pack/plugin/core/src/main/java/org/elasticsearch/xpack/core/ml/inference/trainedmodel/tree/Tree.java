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
import org.elasticsearch.common.collect.Tuple;
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
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ShapPath;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

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
import java.util.stream.IntStream;

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
    // populated lazily when feature importance is calculated
    private double[] nodeEstimates;
    private Integer maxDepth;

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

    public List<TreeNode> getNodes() {
        return nodes;
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config, Map<String, String> featureDecoderMap) {
        if (config.isTargetTypeSupported(targetType) == false) {
            throw ExceptionsHelper.badRequestException(
                "Cannot infer using configuration for [{}] when model target_type is [{}]", config.getName(), targetType.toString());
        }

        List<Double> features = featureNames.stream()
            .map(f -> InferenceHelpers.toDouble(MapHelper.dig(f, fields)))
            .collect(Collectors.toList());

        Map<String, Double> featureImportance = config.requestingImportance() ?
            featureImportance(features, featureDecoderMap) :
            Collections.emptyMap();

        TreeNode node = nodes.get(0);
        while(node.isLeaf() == false) {
            node = nodes.get(node.compare(features));
        }

        return buildResult(node.getLeafValue(), featureImportance, config);
    }

    private InferenceResults buildResult(Double value, Map<String, Double> featureImportance, InferenceConfig config) {
        // Indicates that the config is useless and the caller just wants the raw value
        if (config instanceof NullInferenceConfig) {
            return new RawInferenceResults(value, featureImportance);
        }
        switch (targetType) {
            case CLASSIFICATION:
                ClassificationConfig classificationConfig = (ClassificationConfig) config;
                Tuple<Integer, List<ClassificationInferenceResults.TopClassEntry>> topClasses = InferenceHelpers.topClasses(
                    classificationProbability(value),
                    classificationLabels,
                    null,
                    classificationConfig.getNumTopClasses());
                return new ClassificationInferenceResults(value,
                    classificationLabel(topClasses.v1(), classificationLabels),
                    topClasses.v2(),
                    featureImportance,
                    config);
            case REGRESSION:
                return new RegressionInferenceResults(value, config, featureImportance);
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
        list.set(Double.valueOf(inferenceValue).intValue(), 1.0);
        return list;
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
        int maxFeatureIndex = maxFeatureIndex();
        if (maxFeatureIndex >= featureNames.size()) {
            throw ExceptionsHelper.badRequestException("feature index [{}] is out of bounds for the [{}] array",
                    maxFeatureIndex, FEATURE_NAMES.getPreferredName());
        }
        checkTargetType();
        detectMissingNodes();
        detectCycle();
    }

    @Override
    public Map<String, Double> featureImportance(Map<String, Object> fields, Map<String, String> featureDecoder) {
        if (nodes.stream().allMatch(n -> n.getNumberSamples() == 0)) {
            throw ExceptionsHelper.badRequestException("[tree_structure.number_samples] must be greater than zero for feature importance");
        }
        List<Double> features = featureNames.stream()
            .map(f -> InferenceHelpers.toDouble(MapHelper.dig(f, fields)))
            .collect(Collectors.toList());
        return featureImportance(features, featureDecoder);
    }

    private Map<String, Double> featureImportance(List<Double> fieldValues, Map<String, String> featureDecoder) {
        calculateNodeEstimatesIfNeeded();
        double[] featureImportance = new double[fieldValues.size()];
        int arrSize = ((this.maxDepth + 1) * (this.maxDepth + 2))/2;
        ShapPath.PathElement[] elements = new ShapPath.PathElement[arrSize];
        for (int i = 0; i < arrSize; i++) {
            elements[i] = new ShapPath.PathElement();
        }
        double[] scale = new double[arrSize];
        ShapPath initialPath = new ShapPath(elements, scale);
        shapRecursive(fieldValues, this.nodeEstimates, initialPath, 0, 1.0, 1.0, -1, featureImportance, 0);
        return InferenceHelpers.decodeFeatureImportances(featureDecoder,
            IntStream.range(0, featureImportance.length)
                .boxed()
                .collect(Collectors.toMap(featureNames::get, i -> featureImportance[i])));
    }

    private void calculateNodeEstimatesIfNeeded() {
        if (this.nodeEstimates != null && this.maxDepth != null) {
            return;
        }
        synchronized (this) {
            if (this.nodeEstimates != null && this.maxDepth != null) {
                return;
            }
            double[] estimates = new double[nodes.size()];
            this.maxDepth = fillNodeEstimates(estimates, 0, 0);
            this.nodeEstimates = estimates;
        }
    }

    /**
     * Note, this is a port from https://github.com/elastic/ml-cpp/blob/master/lib/maths/CTreeShapFeatureImportance.cc
     *
     * If improvements in performance or accuracy have been found, it is probably best that the changes are implemented on the native
     * side first and then ported to the Java side.
     */
    private void shapRecursive(List<Double> processedFeatures,
                               double[] nodeValues,
                               ShapPath parentSplitPath,
                               int nodeIndex,
                               double parentFractionZero,
                               double parentFractionOne,
                               int parentFeatureIndex,
                               double[] featureImportance,
                               int nextIndex) {
        ShapPath splitPath = new ShapPath(parentSplitPath, nextIndex);
        TreeNode currNode = nodes.get(nodeIndex);
        nextIndex = splitPath.extend(parentFractionZero, parentFractionOne, parentFeatureIndex, nextIndex);
        if (currNode.isLeaf()) {
            // TODO multi-value????
            double leafValue = nodeValues[nodeIndex];
            for (int i = 1; i < nextIndex; ++i) {
                double scale = splitPath.sumUnwoundPath(i, nextIndex);
                int inputColumnIndex = splitPath.featureIndex(i);
                featureImportance[inputColumnIndex] += scale * (splitPath.fractionOnes(i) - splitPath.fractionZeros(i)) * leafValue;
            }
        } else {
            int hotIndex = currNode.compare(processedFeatures);
            int coldIndex = hotIndex == currNode.getLeftChild() ? currNode.getRightChild() : currNode.getLeftChild();

            double incomingFractionZero = 1.0;
            double incomingFractionOne = 1.0;
            int splitFeature = currNode.getSplitFeature();
            int pathIndex = splitPath.findFeatureIndex(splitFeature, nextIndex);
            if (pathIndex > -1) {
                incomingFractionZero = splitPath.fractionZeros(pathIndex);
                incomingFractionOne = splitPath.fractionOnes(pathIndex);
                nextIndex = splitPath.unwind(pathIndex, nextIndex);
            }

            double hotFractionZero = nodes.get(hotIndex).getNumberSamples() / (double)currNode.getNumberSamples();
            double coldFractionZero = nodes.get(coldIndex).getNumberSamples() / (double)currNode.getNumberSamples();
            shapRecursive(processedFeatures, nodeValues, splitPath,
                hotIndex, incomingFractionZero * hotFractionZero,
                incomingFractionOne, splitFeature, featureImportance, nextIndex);
            shapRecursive(processedFeatures, nodeValues, splitPath,
                coldIndex, incomingFractionZero * coldFractionZero,
                0.0, splitFeature, featureImportance, nextIndex);
        }
    }

    /**
     * This recursively populates the provided {@code double[]} with the node estimated values
     *
     * Used when calculating feature importance.
     * @param nodeEstimates Array to update in place with the node estimated values
     * @param nodeIndex Current node index
     * @param depth Current depth
     * @return The current max depth
     */
    private int fillNodeEstimates(double[] nodeEstimates, int nodeIndex, int depth) {
        TreeNode node = nodes.get(nodeIndex);
        if (node.isLeaf()) {
            nodeEstimates[nodeIndex] = node.getLeafValue();
            return 0;
        }

        int depthLeft = fillNodeEstimates(nodeEstimates, node.getLeftChild(), depth + 1);
        int depthRight = fillNodeEstimates(nodeEstimates, node.getRightChild(), depth + 1);
        long leftWeight = nodes.get(node.getLeftChild()).getNumberSamples();
        long rightWeight = nodes.get(node.getRightChild()).getNumberSamples();
        long divisor = leftWeight + rightWeight;
        double averageValue = divisor == 0 ?
            0.0 :
            (leftWeight * nodeEstimates[node.getLeftChild()] + rightWeight * nodeEstimates[node.getRightChild()]) / divisor;
        nodeEstimates[nodeIndex] = averageValue;
        return Math.max(depthLeft, depthRight) + 1;
    }

    @Override
    public long estimatedNumOperations() {
        // Grabbing the features from the doc + the depth of the tree
        return (long)Math.ceil(Math.log(nodes.size())) + featureNames.size();
    }

    @Override
    public boolean supportsFeatureImportance() {
        return true;
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
            throw ExceptionsHelper.badRequestException(
                "[target_type] should be [classification] if [classification_labels] are provided");
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
