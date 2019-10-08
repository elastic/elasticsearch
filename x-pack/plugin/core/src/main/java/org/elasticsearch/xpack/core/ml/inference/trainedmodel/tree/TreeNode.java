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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Operator;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TreeNode implements ToXContentObject, Writeable {

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
        parser.declareDouble(TreeNode.Builder::setLeafValue, LEAF_VALUE);
        return parser;
    }

    public static TreeNode.Builder fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final Operator operator;
    private final Double threshold;
    private final Integer splitFeature;
    private final int nodeIndex;
    private final Double splitGain;
    private final Double leafValue;
    private final boolean defaultLeft;
    private final int leftChild;
    private final int rightChild;


    TreeNode(Operator operator,
             Double threshold,
             Integer splitFeature,
             Integer nodeIndex,
             Double splitGain,
             Double leafValue,
             Boolean defaultLeft,
             Integer leftChild,
             Integer rightChild) {
        this.operator = operator == null ? Operator.LTE : operator;
        this.threshold  = threshold;
        this.splitFeature = splitFeature;
        this.nodeIndex = ExceptionsHelper.requireNonNull(nodeIndex, NODE_INDEX.getPreferredName());
        this.splitGain  = splitGain;
        this.leafValue = leafValue;
        this.defaultLeft = defaultLeft == null ? false : defaultLeft;
        this.leftChild  = leftChild == null ? -1 : leftChild;
        this.rightChild = rightChild == null ? -1 : rightChild;
    }

    public TreeNode(StreamInput in) throws IOException {
        operator = Operator.readFromStream(in);
        threshold = in.readOptionalDouble();
        splitFeature = in.readOptionalInt();
        splitGain = in.readOptionalDouble();
        nodeIndex = in.readInt();
        leafValue = in.readOptionalDouble();
        defaultLeft = in.readBoolean();
        leftChild = in.readInt();
        rightChild = in.readInt();
    }


    public Operator getOperator() {
        return operator;
    }

    public Double getThreshold() {
        return threshold;
    }

    public Integer getSplitFeature() {
        return splitFeature;
    }

    public Integer getNodeIndex() {
        return nodeIndex;
    }

    public Double getSplitGain() {
        return splitGain;
    }

    public Double getLeafValue() {
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

    public int compare(List<Double> features) {
        if (isLeaf()) {
            throw new IllegalArgumentException("cannot call compare against a leaf node.");
        }
        Double feature = features.get(splitFeature);
        if (isMissing(feature)) {
            return defaultLeft ? leftChild : rightChild;
        }
        return operator.test(feature, threshold) ? leftChild : rightChild;
    }

    private boolean isMissing(Double feature) {
        return feature == null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        operator.writeTo(out);
        out.writeOptionalDouble(threshold);
        out.writeOptionalInt(splitFeature);
        out.writeOptionalDouble(splitGain);
        out.writeInt(nodeIndex);
        out.writeOptionalDouble(leafValue);
        out.writeBoolean(defaultLeft);
        out.writeInt(leftChild);
        out.writeInt(rightChild);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addOptionalField(builder, DECISION_TYPE, operator);
        addOptionalField(builder, THRESHOLD, threshold);
        addOptionalField(builder, SPLIT_FEATURE, splitFeature);
        addOptionalField(builder, SPLIT_GAIN, splitGain);
        builder.field(NODE_INDEX.getPreferredName(), nodeIndex);
        addOptionalField(builder, LEAF_VALUE, leafValue);
        builder.field(DEFAULT_LEFT.getPreferredName(), defaultLeft);
        if (leftChild >= 0) {
            builder.field(LEFT_CHILD.getPreferredName(), leftChild);
        }
        if (rightChild >= 0) {
            builder.field(RIGHT_CHILD.getPreferredName(), rightChild);
        }
        builder.endObject();
        return builder;
    }

    private void addOptionalField(XContentBuilder builder, ParseField field, Object value) throws IOException {
        if (value != null) {
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
            && Objects.equals(leafValue, that.leafValue)
            && Objects.equals(defaultLeft, that.defaultLeft)
            && Objects.equals(leftChild, that.leftChild)
            && Objects.equals(rightChild, that.rightChild);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator,
            threshold,
            splitFeature,
            splitGain,
            nodeIndex,
            leafValue,
            defaultLeft,
            leftChild,
            rightChild);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder(int nodeIndex) {
        return new Builder(nodeIndex);
    }
    
    public static class Builder {
        private Operator operator;
        private Double threshold;
        private Integer splitFeature;
        private int nodeIndex;
        private Double splitGain;
        private Double leafValue;
        private Boolean defaultLeft;
        private Integer leftChild;
        private Integer rightChild;

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

        public Builder setLeafValue(Double leafValue) {
            this.leafValue = leafValue;
            return this;
        }

        public Builder setDefaultLeft(Boolean defaultLeft) {
            this.defaultLeft = defaultLeft;
            return this;
        }

        public Builder setLeftChild(Integer leftChild) {
            this.leftChild = leftChild;
            return this;
        }

        Integer getLeftChild() {
            return leftChild;
        }

        public Builder setRightChild(Integer rightChild) {
            this.rightChild = rightChild;
            return this;
        }

        Integer getRightChild() {
            return rightChild;
        }

        public void validate() {
            if (nodeIndex < 0) {
                throw new IllegalArgumentException("[node_index] must be a non-negative integer.");
            }
            if (leftChild == null) { // leaf validations
                if (leafValue == null) {
                    throw new IllegalArgumentException("[leaf_value] is required for a leaf node.");
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
                rightChild);
        }
    }
}
