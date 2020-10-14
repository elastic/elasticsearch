/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Operator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class TreeNode implements ToXContentObject, Writeable, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TreeNode.class);
    public static final String NAME = "tree_node";

    public static final ParseField DECISION_TYPE = new ParseField("decision_type");
    public static final ParseField THRESHOLD = new ParseField("threshold");
    public static final ParseField LEFT_CHILD = new ParseField("left_child");
    public static final ParseField RIGHT_CHILD = new ParseField("right_child");
    public static final ParseField DEFAULT_LEFT = new ParseField("default_left");
    public static final ParseField SPLIT_FEATURE = new ParseField("split_feature");
    public static final ParseField NODE_INDEX = new ParseField("node_index");
    public static final ParseField SPLIT_GAIN = new ParseField("split_gain");
    public static final ParseField LEAF_VALUE = new ParseField("leaf_value");
    public static final ParseField NUMBER_SAMPLES = new ParseField("number_samples");

    private static final ObjectParser<TreeNode.Builder, Void> LENIENT_PARSER = createParser(true);
    private static final ObjectParser<TreeNode.Builder, Void> STRICT_PARSER = createParser(false);

    private static ObjectParser<TreeNode.Builder, Void> createParser(boolean lenient) {
        ObjectParser<TreeNode.Builder, Void> parser = new ObjectParser<>(
            NAME,
            lenient,
            TreeNode.Builder::new);
        parser.declareDouble(TreeNode.Builder::setThreshold, THRESHOLD);
        parser.declareField(TreeNode.Builder::setOperator,
            p -> Operator.fromString(p.text()),
            DECISION_TYPE,
            ObjectParser.ValueType.STRING);
        parser.declareInt(TreeNode.Builder::setLeftChild, LEFT_CHILD);
        parser.declareInt(TreeNode.Builder::setRightChild, RIGHT_CHILD);
        parser.declareBoolean(TreeNode.Builder::setDefaultLeft, DEFAULT_LEFT);
        parser.declareInt(TreeNode.Builder::setSplitFeature, SPLIT_FEATURE);
        parser.declareInt(TreeNode.Builder::setNodeIndex, NODE_INDEX);
        parser.declareDouble(TreeNode.Builder::setSplitGain, SPLIT_GAIN);
        parser.declareDoubleArray(TreeNode.Builder::setLeafValue, LEAF_VALUE);
        parser.declareLong(TreeNode.Builder::setNumberSamples, NUMBER_SAMPLES);
        return parser;
    }

    public static TreeNode.Builder fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final Operator operator;
    private final double threshold;
    private final int splitFeature;
    private final int nodeIndex;
    private final double splitGain;
    private final double[] leafValue;
    private final boolean defaultLeft;
    private final int leftChild;
    private final int rightChild;
    private final long numberSamples;


    private TreeNode(Operator operator,
                     Double threshold,
                     Integer splitFeature,
                     int nodeIndex,
                     Double splitGain,
                     List<Double> leafValue,
                     Boolean defaultLeft,
                     Integer leftChild,
                     Integer rightChild,
                     long numberSamples) {
        this.operator = operator == null ? Operator.LTE : operator;
        this.threshold  = threshold == null ? Double.NaN : threshold;
        this.splitFeature = splitFeature == null ? -1 : splitFeature;
        this.nodeIndex = nodeIndex;
        this.splitGain  = splitGain == null ? Double.NaN : splitGain;
        this.leafValue = leafValue == null ? new double[0] : leafValue.stream().mapToDouble(Double::doubleValue).toArray();
        this.defaultLeft = defaultLeft == null ? false : defaultLeft;
        this.leftChild  = leftChild == null ? -1 : leftChild;
        this.rightChild = rightChild == null ? -1 : rightChild;
        if (numberSamples < 0) {
            throw new IllegalArgumentException("[" + NUMBER_SAMPLES.getPreferredName() + "] must be greater than or equal to 0");
        }
        this.numberSamples = numberSamples;
    }

    public TreeNode(StreamInput in) throws IOException {
        operator = Operator.readFromStream(in);
        threshold = in.readDouble();
        splitFeature = in.readInt();
        splitGain = in.readDouble();
        nodeIndex = in.readVInt();
        leafValue = in.readDoubleArray();
        defaultLeft = in.readBoolean();
        leftChild = in.readInt();
        rightChild = in.readInt();
        numberSamples = in.readVLong();
    }

    public Operator getOperator() {
        return operator;
    }

    public double getThreshold() {
        return threshold;
    }

    public int getSplitFeature() {
        return splitFeature;
    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public double getSplitGain() {
        return splitGain;
    }

    public double[] getLeafValue() {
        return leafValue;
    }

    public boolean isDefaultLeft() {
        return defaultLeft;
    }

    public int getLeftChild() {
        return leftChild;
    }

    public int getRightChild() {
        return rightChild;
    }

    public boolean isLeaf() {
        return leftChild < 0;
    }

    public long getNumberSamples() {
        return numberSamples;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        operator.writeTo(out);
        out.writeDouble(threshold);
        out.writeInt(splitFeature);
        out.writeDouble(splitGain);
        out.writeVInt(nodeIndex);
        out.writeDoubleArray(leafValue);
        out.writeBoolean(defaultLeft);
        out.writeInt(leftChild);
        out.writeInt(rightChild);
        out.writeVLong(numberSamples);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DECISION_TYPE.getPreferredName(), operator);
        addOptionalDouble(builder, THRESHOLD, threshold);
        if (splitFeature > -1) {
            builder.field(SPLIT_FEATURE.getPreferredName(), splitFeature);
        }
        addOptionalDouble(builder, SPLIT_GAIN, splitGain);
        builder.field(NODE_INDEX.getPreferredName(), nodeIndex);
        if (leafValue.length > 0) {
            builder.field(LEAF_VALUE.getPreferredName(), leafValue);
        }
        builder.field(DEFAULT_LEFT.getPreferredName(), defaultLeft);
        if (leftChild >= 0) {
            builder.field(LEFT_CHILD.getPreferredName(), leftChild);
        }
        if (rightChild >= 0) {
            builder.field(RIGHT_CHILD.getPreferredName(), rightChild);
        }
        builder.field(NUMBER_SAMPLES.getPreferredName(), numberSamples);
        builder.endObject();
        return builder;
    }

    private void addOptionalDouble(XContentBuilder builder, ParseField field, double value) throws IOException {
        if (Numbers.isValidDouble(value)) {
            builder.field(field.getPreferredName(), value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeNode that = (TreeNode) o;
        return Objects.equals(operator, that.operator)
            && Objects.equals(threshold, that.threshold)
            && Objects.equals(splitFeature, that.splitFeature)
            && Objects.equals(nodeIndex, that.nodeIndex)
            && Objects.equals(splitGain, that.splitGain)
            && Arrays.equals(leafValue, that.leafValue)
            && Objects.equals(defaultLeft, that.defaultLeft)
            && Objects.equals(leftChild, that.leftChild)
            && Objects.equals(rightChild, that.rightChild)
            && Objects.equals(numberSamples, that.numberSamples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator,
            threshold,
            splitFeature,
            splitGain,
            nodeIndex,
            Arrays.hashCode(leafValue),
            defaultLeft,
            leftChild,
            rightChild,
            numberSamples);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder(int nodeIndex) {
        return new Builder(nodeIndex);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + this.leafValue.length * Double.BYTES;
    }

    public static class Builder {
        private Operator operator;
        private Double threshold;
        private Integer splitFeature;
        private int nodeIndex;
        private Double splitGain;
        private List<Double> leafValue;
        private Boolean defaultLeft;
        private Integer leftChild;
        private Integer rightChild;
        private long numberSamples;

        public Builder(int nodeIndex) {
            this.nodeIndex = nodeIndex;
        }

        private Builder() {
        }

        public Builder setOperator(Operator operator) {
            this.operator = operator;
            return this;
        }

        public Builder setThreshold(Double threshold) {
            this.threshold = threshold;
            return this;
        }

        public Builder setSplitFeature(Integer splitFeature) {
            this.splitFeature = splitFeature;
            return this;
        }

        public Builder setNodeIndex(Integer nodeIndex) {
            this.nodeIndex = nodeIndex;
            return this;
        }

        public Builder setSplitGain(Double splitGain) {
            this.splitGain = splitGain;
            return this;
        }

        public Builder setLeafValue(double leafValue) {
            return this.setLeafValue(Collections.singletonList(leafValue));
        }

        public Builder setLeafValue(List<Double> leafValue) {
            this.leafValue = leafValue;
            return this;
        }

        List<Double> getLeafValue() {
            return this.leafValue;
        }

        public Builder setDefaultLeft(Boolean defaultLeft) {
            this.defaultLeft = defaultLeft;
            return this;
        }

        public Builder setLeftChild(Integer leftChild) {
            this.leftChild = leftChild;
            return this;
        }

        public Integer getLeftChild() {
            return leftChild;
        }

        public Builder setRightChild(Integer rightChild) {
            this.rightChild = rightChild;
            return this;
        }

        public Integer getRightChild() {
            return rightChild;
        }

        public Builder setNumberSamples(long numberSamples) {
            this.numberSamples = numberSamples;
            return this;
        }

        public void validate() {
            if (nodeIndex < 0) {
                throw new IllegalArgumentException("[node_index] must be a non-negative integer.");
            }
            if (leftChild == null) { // leaf validations
                if (leafValue == null) {
                    throw new IllegalArgumentException("[leaf_value] is required for a leaf node.");
                }
                if (leafValue.stream().anyMatch(Objects::isNull)) {
                    throw new IllegalArgumentException("[leaf_value] cannot have null values.");
                }
            } else {
                if (leftChild < 0) {
                    throw new IllegalArgumentException("[left_child] must be a non-negative integer.");
                }
                if (rightChild != null && rightChild < 0) {
                    throw new IllegalArgumentException("[right_child] must be a non-negative integer.");
                }
                if (threshold == null) {
                    throw new IllegalArgumentException("[threshold] must exist for non-leaf node.");
                }
            }
        }

        public TreeNode build() {
            validate();
            return new TreeNode(operator,
                threshold,
                splitFeature,
                nodeIndex,
                splitGain,
                leafValue,
                defaultLeft,
                leftChild,
                rightChild,
                numberSamples);
        }
    }
}
