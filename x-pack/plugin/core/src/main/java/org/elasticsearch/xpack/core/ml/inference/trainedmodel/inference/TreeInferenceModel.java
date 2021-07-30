/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RawInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.TopClassEntry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NullInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ShapPath;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.inference.utils.Statistics;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;
import static org.apache.lucene.util.RamUsageEstimator.sizeOf;
import static org.apache.lucene.util.RamUsageEstimator.sizeOfCollection;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.classificationLabel;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceHelpers.decodeFeatureImportances;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree.CLASSIFICATION_LABELS;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree.FEATURE_NAMES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree.TREE_STRUCTURE;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.DECISION_TYPE;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.DEFAULT_LEFT;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.LEAF_VALUE;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.LEFT_CHILD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.NUMBER_SAMPLES;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.RIGHT_CHILD;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.SPLIT_FEATURE;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeNode.THRESHOLD;

public class TreeInferenceModel implements InferenceModel {

    private static final Logger LOGGER = LogManager.getLogger(TreeInferenceModel.class);
    public static final long SHALLOW_SIZE = shallowSizeOfInstance(TreeInferenceModel.class);

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TreeInferenceModel, Void> PARSER = new ConstructingObjectParser<>(
        "tree_inference_model",
        true,
        a -> new TreeInferenceModel(
            (List<String>)a[0],
            (List<NodeBuilder>)a[1],
            a[2] == null ? null : TargetType.fromString((String)a[2]),
            (List<String>)a[3]));

    static {
        PARSER.declareStringArray(constructorArg(), FEATURE_NAMES);
        PARSER.declareObjectArray(constructorArg(), NodeBuilder.PARSER::apply, TREE_STRUCTURE);
        PARSER.declareString(optionalConstructorArg(), TargetType.TARGET_TYPE);
        PARSER.declareStringArray(optionalConstructorArg(), CLASSIFICATION_LABELS);
    }

    public static TreeInferenceModel fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Node[] nodes;
    private String[] featureNames;
    private final TargetType targetType;
    private List<String> classificationLabels;
    private final double highOrderCategory;
    private final int maxDepth;
    private final int leafSize;
    private volatile boolean preparedForInference = false;

    TreeInferenceModel(List<String> featureNames,
                       List<NodeBuilder> nodes,
                       @Nullable TargetType targetType,
                       List<String> classificationLabels) {
        this.featureNames = ExceptionsHelper.requireNonNull(featureNames, FEATURE_NAMES).toArray(String[]::new);
        if(ExceptionsHelper.requireNonNull(nodes, TREE_STRUCTURE).size() == 0) {
            throw new IllegalArgumentException("[tree_structure] must not be empty");
        }
        this.nodes = nodes.stream().map(NodeBuilder::build).toArray(Node[]::new);
        this.targetType = targetType == null ? TargetType.REGRESSION : targetType;
        this.classificationLabels = classificationLabels == null ? null : Collections.unmodifiableList(classificationLabels);
        this.highOrderCategory = maxLeafValue();
        int leafSize = 1;
        for (Node node : this.nodes) {
            if (node instanceof LeafNode) {
                leafSize = ((LeafNode)node).leafValue.length;
                break;
            }
        }
        this.leafSize = leafSize;
        this.maxDepth = getDepth(this.nodes, 0, 0);
    }

    @Override
    public String[] getFeatureNames() {
        return featureNames;
    }

    @Override
    public TargetType targetType() {
        return targetType;
    }

    @Override
    public InferenceResults infer(Map<String, Object> fields, InferenceConfig config, Map<String, String> featureDecoderMap) {
        return innerInfer(InferenceModel.extractFeatures(featureNames, fields), config, featureDecoderMap);
    }

    @Override
    public InferenceResults infer(double[] features, InferenceConfig config) {
        return innerInfer(features, config, Collections.emptyMap());
    }

    private InferenceResults innerInfer(double[] features, InferenceConfig config, Map<String, String> featureDecoderMap) {
        if (config.isTargetTypeSupported(targetType) == false) {
            throw ExceptionsHelper.badRequestException(
                "Cannot infer using configuration for [{}] when model target_type is [{}]", config.getName(), targetType.toString());
        }
        if (preparedForInference == false) {
            throw ExceptionsHelper.serverError("model is not prepared for inference");
        }
        double[][] featureImportance = config.requestingImportance() ?
            featureImportance(features) :
            new double[0][];

        return buildResult(getLeaf(features), featureImportance, featureDecoderMap, config);
    }

    private InferenceResults buildResult(double[] value,
                                         double[][] featureImportance,
                                         Map<String, String> featureDecoderMap,
                                         InferenceConfig config) {
        assert value != null && value.length > 0;
        // Indicates that the config is useless and the caller just wants the raw value
        if (config instanceof NullInferenceConfig) {
            return new RawInferenceResults(value, featureImportance);
        }
        Map<String, double[]> decodedFeatureImportance = config.requestingImportance() ?
            decodeFeatureImportances(featureDecoderMap,
                IntStream.range(0, featureImportance.length)
                    .boxed()
                    .collect(Collectors.toMap(i -> featureNames[i], i -> featureImportance[i]))) :
            Collections.emptyMap();
        switch (targetType) {
            case CLASSIFICATION:
                ClassificationConfig classificationConfig = (ClassificationConfig) config;
                Tuple<InferenceHelpers.TopClassificationValue, List<TopClassEntry>> topClasses = InferenceHelpers.topClasses(
                    classificationProbability(value),
                    classificationLabels,
                    null,
                    classificationConfig.getNumTopClasses(),
                    classificationConfig.getPredictionFieldType());
                final InferenceHelpers.TopClassificationValue classificationValue = topClasses.v1();
                return new ClassificationInferenceResults(classificationValue.getValue(),
                    classificationLabel(classificationValue.getValue(), classificationLabels),
                    topClasses.v2(),
                    InferenceHelpers.transformFeatureImportanceClassification(decodedFeatureImportance,
                        classificationLabels,
                        classificationConfig.getPredictionFieldType()),
                    config,
                    classificationValue.getProbability(),
                    classificationValue.getScore());
            case REGRESSION:
                return new RegressionInferenceResults(value[0],
                    config,
                    InferenceHelpers.transformFeatureImportanceRegression(decodedFeatureImportance));
            default:
                throw new UnsupportedOperationException("unsupported target_type [" + targetType + "] for inference on tree model");
        }
    }

    private double[] classificationProbability(double[] inferenceValue) {
        // Multi-value leaves, indicates that the leaves contain an array of values.
        // The index of which corresponds to classification values
        if (inferenceValue.length > 1) {
            return Statistics.softMax(inferenceValue);
        }
        // If we are classification, we should assume that the inference return value is whole.
        assert inferenceValue[0] == Math.rint(inferenceValue[0]);
        double maxCategory = this.highOrderCategory;
        // If we are classification, we should assume that the largest leaf value is whole.
        assert maxCategory == Math.rint(maxCategory);
        double[] list = Collections.nCopies(Double.valueOf(maxCategory + 1).intValue(), 0.0)
            .stream()
            .mapToDouble(Double::doubleValue)
            .toArray();
        list[Double.valueOf(inferenceValue[0]).intValue()] = 1.0;
        return list;
    }

    private double[] getLeaf(double[] features) {
        Node node = nodes[0];
        while(node.isLeaf() == false) {
            node = nodes[node.compare(features)];
        }
        return ((LeafNode)node).leafValue;
    }

    public double[][] featureImportance(double[] fieldValues) {
        double[][] featureImportance = new double[fieldValues.length][leafSize];
        for (int i = 0; i < fieldValues.length; i++) {
            featureImportance[i] = new double[leafSize];
        }
        int arrSize = ((this.maxDepth + 1) * (this.maxDepth + 2))/2;
        ShapPath.PathElement[] elements = new ShapPath.PathElement[arrSize];
        for (int i = 0; i < arrSize; i++) {
            elements[i] = new ShapPath.PathElement();
        }
        double[] scale = new double[arrSize];
        ShapPath initialPath = new ShapPath(elements, scale);
        shapRecursive(fieldValues, initialPath, 0, 1.0, 1.0, -1, featureImportance, 0);
        return featureImportance;
    }

    /**
     * Note, this is a port from https://github.com/elastic/ml-cpp/blob/master/lib/maths/CTreeShapFeatureImportance.cc
     *
     * If improvements in performance or accuracy have been found, it is probably best that the changes are implemented on the native
     * side first and then ported to the Java side.
     */
    private void shapRecursive(double[] processedFeatures,
                               ShapPath parentSplitPath,
                               int nodeIndex,
                               double parentFractionZero,
                               double parentFractionOne,
                               int parentFeatureIndex,
                               double[][] featureImportance,
                               int nextIndex) {
        ShapPath splitPath = new ShapPath(parentSplitPath, nextIndex);
        Node currNode = nodes[nodeIndex];
        nextIndex = splitPath.extend(parentFractionZero, parentFractionOne, parentFeatureIndex, nextIndex);
        if (currNode.isLeaf()) {
            double[] leafValue = ((LeafNode)currNode).leafValue;
            for (int i = 1; i < nextIndex; ++i) {
                int inputColumnIndex = splitPath.featureIndex(i);
                double scaled = splitPath.sumUnwoundPath(i, nextIndex) * (splitPath.fractionOnes(i) - splitPath.fractionZeros(i));
                for (int j = 0; j < leafValue.length; j++) {
                    featureImportance[inputColumnIndex][j] += scaled * leafValue[j];
                }
            }
        } else {
            InnerNode innerNode = (InnerNode)currNode;
            int hotIndex = currNode.compare(processedFeatures);
            int coldIndex = hotIndex == innerNode.leftChild ? innerNode.rightChild : innerNode.leftChild;

            double incomingFractionZero = 1.0;
            double incomingFractionOne = 1.0;
            int splitFeature = innerNode.splitFeature;
            int pathIndex = splitPath.findFeatureIndex(splitFeature, nextIndex);
            if (pathIndex > -1) {
                incomingFractionZero = splitPath.fractionZeros(pathIndex);
                incomingFractionOne = splitPath.fractionOnes(pathIndex);
                nextIndex = splitPath.unwind(pathIndex, nextIndex);
            }

            double hotFractionZero = nodes[hotIndex].getNumberSamples() / (double)currNode.getNumberSamples();
            double coldFractionZero = nodes[coldIndex].getNumberSamples() / (double)currNode.getNumberSamples();
            shapRecursive(processedFeatures, splitPath,
                hotIndex, incomingFractionZero * hotFractionZero,
                incomingFractionOne, splitFeature, featureImportance, nextIndex);
            shapRecursive(processedFeatures, splitPath,
                coldIndex, incomingFractionZero * coldFractionZero,
                0.0, splitFeature, featureImportance, nextIndex);
        }
    }

    @Override
    public boolean supportsFeatureImportance() {
        return true;
    }

    @Override
    public String getName() {
        return "tree";
    }

    @Override
    public void rewriteFeatureIndices(Map<String, Integer> newFeatureIndexMapping) {
        LOGGER.debug(() -> new ParameterizedMessage("rewriting features {}", newFeatureIndexMapping));
        if (preparedForInference) {
            return;
        }
        preparedForInference = true;
        if (newFeatureIndexMapping == null || newFeatureIndexMapping.isEmpty()) {
            return;
        }
        for (Node node : nodes) {
            if (node.isLeaf()) {
                continue;
            }
            InnerNode treeNode = (InnerNode)node;
            Integer newSplitFeatureIndex = newFeatureIndexMapping.get(featureNames[treeNode.splitFeature]);
            if (newSplitFeatureIndex == null) {
                throw new IllegalArgumentException("[tree] failed to optimize for inference");
            }
            treeNode.splitFeature = newSplitFeatureIndex;
        }
        this.featureNames = new String[0];
        // Since we are not top level, we no longer need local classification labels
        this.classificationLabels = null;
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += sizeOfCollection(classificationLabels);
        size += sizeOf(featureNames);
        size += sizeOf(nodes);
        return size;
    }

    private double maxLeafValue() {
        if (targetType != TargetType.CLASSIFICATION) {
            return Double.NaN;
        }
        double max = 0.0;
        for (Node node : this.nodes) {
            if (node instanceof LeafNode) {
                LeafNode leafNode = (LeafNode) node;
                if (leafNode.leafValue.length > 1) {
                    return leafNode.leafValue.length;
                } else {
                    max = Math.max(leafNode.leafValue[0], max);
                }
            }
        }
        return max;
    }

    public Node[] getNodes() {
        return nodes;
    }

    @Override
    public String toString() {
        return "TreeInferenceModel{" +
            "nodes=" + Arrays.toString(nodes) +
            ", featureNames=" + Arrays.toString(featureNames) +
            ", targetType=" + targetType +
            ", classificationLabels=" + classificationLabels +
            ", highOrderCategory=" + highOrderCategory +
            ", maxDepth=" + maxDepth +
            ", leafSize=" + leafSize +
            ", preparedForInference=" + preparedForInference +
            '}';
    }

    private static int getDepth(Node[] nodes, int nodeIndex, int depth) {
        Node node = nodes[nodeIndex];
        if (node instanceof LeafNode) {
            return 0;
        }
        InnerNode innerNode = (InnerNode)node;
        int depthLeft = getDepth(nodes, innerNode.leftChild, depth + 1);
        int depthRight = getDepth(nodes, innerNode.rightChild, depth + 1);
        return Math.max(depthLeft, depthRight) + 1;
    }

    static class NodeBuilder {

        private static final ObjectParser<NodeBuilder, Void> PARSER = new ObjectParser<>(
            "tree_inference_model_node",
            true,
            NodeBuilder::new);
        static {
            PARSER.declareDouble(NodeBuilder::setThreshold, THRESHOLD);
            PARSER.declareField(NodeBuilder::setOperator,
                p -> Operator.fromString(p.text()),
                DECISION_TYPE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt(NodeBuilder::setLeftChild, LEFT_CHILD);
            PARSER.declareInt(NodeBuilder::setRightChild, RIGHT_CHILD);
            PARSER.declareBoolean(NodeBuilder::setDefaultLeft, DEFAULT_LEFT);
            PARSER.declareInt(NodeBuilder::setSplitFeature, SPLIT_FEATURE);
            PARSER.declareDoubleArray(NodeBuilder::setLeafValue, LEAF_VALUE);
            PARSER.declareLong(NodeBuilder::setNumberSamples, NUMBER_SAMPLES);
        }

        private Operator operator = Operator.LTE;
        private double threshold = Double.NaN;
        private int splitFeature = -1;
        private boolean defaultLeft = false;
        private int leftChild = -1;
        private int rightChild = -1;
        private long numberSamples;
        private double[] leafValue = new double[0];

        public NodeBuilder setOperator(Operator operator) {
            this.operator = operator;
            return this;
        }

        public NodeBuilder setThreshold(double threshold) {
            this.threshold = threshold;
            return this;
        }

        public NodeBuilder setSplitFeature(int splitFeature) {
            this.splitFeature = splitFeature;
            return this;
        }

        public NodeBuilder setDefaultLeft(boolean defaultLeft) {
            this.defaultLeft = defaultLeft;
            return this;
        }

        public NodeBuilder setLeftChild(int leftChild) {
            this.leftChild = leftChild;
            return this;
        }

        public NodeBuilder setRightChild(int rightChild) {
            this.rightChild = rightChild;
            return this;
        }

        public NodeBuilder setNumberSamples(long numberSamples) {
            this.numberSamples = numberSamples;
            return this;
        }

        private NodeBuilder setLeafValue(List<Double> leafValue) {
            return setLeafValue(leafValue.stream().mapToDouble(Double::doubleValue).toArray());
        }

        public NodeBuilder setLeafValue(double[] leafValue) {
            this.leafValue = leafValue;
            return this;
        }

        Node build() {
            if (this.leftChild < 0) {
                return new LeafNode(leafValue, numberSamples);
            }
            return new InnerNode(operator,
                threshold,
                splitFeature,
                defaultLeft,
                leftChild,
                rightChild,
                numberSamples);
        }
    }

    public abstract static class Node implements Accountable {
        int compare(double[] features) {
            throw new IllegalArgumentException("cannot call compare against a leaf node.");
        }

        abstract long getNumberSamples();

        public boolean isLeaf() {
            return this instanceof LeafNode;
        }
    }

    public static class InnerNode extends Node {

        public static final long SHALLOW_SIZE = shallowSizeOfInstance(InnerNode.class);

        private final Operator operator;
        private final double threshold;
        // Allowed to be adjusted for inference optimization
        private int splitFeature;
        private final boolean defaultLeft;
        private final int leftChild;
        private final int rightChild;
        private final long numberSamples;

        InnerNode(Operator operator,
                  double threshold,
                  int splitFeature,
                  boolean defaultLeft,
                  int leftChild,
                  int rightChild,
                  long numberSamples) {
            this.operator = operator;
            this.threshold = threshold;
            this.splitFeature = splitFeature;
            this.defaultLeft = defaultLeft;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
            this.numberSamples = numberSamples;
        }

        @Override
        public int compare(double[] features) {
            double feature = features[splitFeature];
            if (isMissing(feature)) {
                return defaultLeft ? leftChild : rightChild;
            }
            return operator.test(feature, threshold) ? leftChild : rightChild;
        }

        @Override
        long getNumberSamples() {
            return numberSamples;
        }

        private static boolean isMissing(double feature) {
            return Numbers.isValidDouble(feature) == false;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE;
        }

        @Override
        public String toString() {
            return "InnerNode{" +
                "operator=" + operator +
                ", threshold=" + threshold +
                ", splitFeature=" + splitFeature +
                ", defaultLeft=" + defaultLeft +
                ", leftChild=" + leftChild +
                ", rightChild=" + rightChild +
                ", numberSamples=" + numberSamples +
                '}';
        }
    }

    public static class LeafNode extends Node {
        public static final long SHALLOW_SIZE = shallowSizeOfInstance(LeafNode.class);
        private final double[] leafValue;
        private final long numberSamples;

        LeafNode(double[] leafValue, long numberSamples) {
            this.leafValue = leafValue;
            this.numberSamples = numberSamples;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + sizeOf(leafValue);
        }

        @Override
        long getNumberSamples() {
            return numberSamples;
        }

        public double[] getLeafValue() {
            return leafValue;
        }

        @Override
        public String toString() {
            return "LeafNode{" +
                "leafValue=" + Arrays.toString(leafValue) +
                ", numberSamples=" + numberSamples +
                '}';
        }
    }
}
