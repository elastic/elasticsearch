/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.model.tree;

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
    public static final ParseField SPLIT_INDEX = new ParseField("split_index");
    public static final ParseField SPLIT_GAIN = new ParseField("split_gain");
    public static final ParseField INTERNAL_VALUE = new ParseField("internal_value");

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
        parser.declareInt(TreeNode.Builder::setSplitIndex, SPLIT_INDEX);
        parser.declareDouble(TreeNode.Builder::setSplitGain, SPLIT_GAIN);
        parser.declareDouble(TreeNode.Builder::setInternalValue, INTERNAL_VALUE);
        return parser;
    }

    public static TreeNode.Builder fromXContent(XContentParser parser, boolean lenient) {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    private final Operator operator;
    private final Double threshold;
    private final Integer splitFeature;
    private final Integer splitIndex;
    private final Double splitGain;
    private final Double internalValue;
    private final boolean defaultLeft;
    private final int leftChild;
    private final int rightChild;


    TreeNode(Operator operator,
             Double threshold,
             Integer splitFeature,
             Integer splitIndex,
             Double splitGain,
             Double internalValue,
             Boolean defaultLeft,
             Integer leftChild,
             Integer rightChild) {
        this.operator = operator == null ? Operator.LTE : operator;
        this.threshold  = threshold;
        this.splitFeature = splitFeature;
        this.splitIndex = splitIndex;
        this.splitGain  = splitGain;
        this.internalValue = internalValue;
        this.defaultLeft = defaultLeft == null ? false : defaultLeft;
        this.leftChild  = leftChild == null ? -1 : leftChild;
        this.rightChild = rightChild == null ? -1 : rightChild;
    }

    public TreeNode(StreamInput in) throws IOException {
        operator = Operator.readFromStream(in);
        threshold = in.readOptionalDouble();
        splitFeature = in.readOptionalInt();
        splitGain = in.readOptionalDouble();
        splitIndex = in.readOptionalInt();
        internalValue = in.readOptionalDouble();
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

    public Integer getSplitIndex() {
        return splitIndex;
    }

    public Double getSplitGain() {
        return splitGain;
    }

    public Double getInternalValue() {
        return internalValue;
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
        return leftChild < 1;
    }

    public int compare(List<Double> features) {
        if (isLeaf()) {
            throw new IllegalArgumentException("cannot call compare against a leaf node.");
        }
        Double feature = features.get(splitIndex);
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
        out.writeOptionalInt(splitIndex);
        out.writeOptionalDouble(internalValue);
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
        addOptionalField(builder, SPLIT_INDEX, splitIndex);
        addOptionalField(builder, INTERNAL_VALUE, internalValue);
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
            && Objects.equals(splitIndex, that.splitIndex)
            && Objects.equals(splitGain, that.splitGain)
            && Objects.equals(internalValue, that.internalValue)
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
            splitIndex,
            internalValue,
            defaultLeft,
            leftChild,
            rightChild);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private Operator operator;
        private Double threshold;
        private Integer splitFeature;
        private Integer splitIndex;
        private Double splitGain;
        private Double internalValue;
        private Boolean defaultLeft;
        private Integer leftChild;
        private Integer rightChild;

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

        public Builder setSplitIndex(Integer splitIndex) {
            this.splitIndex = splitIndex;
            return this;
        }

        public Builder setSplitGain(Double splitGain) {
            this.splitGain = splitGain;
            return this;
        }

        public Builder setInternalValue(Double internalValue) {
            this.internalValue = internalValue;
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
            if (leftChild == null) { // leaf validations
                if (internalValue == null) {
                    throw new IllegalArgumentException("[internal_value] is required for a leaf node.");
                }
            } else {
                if (leftChild < 0) {
                    throw new IllegalArgumentException("[left_child] must be a non-negative integer.");
                }
                if (rightChild != null && rightChild < 0) {
                    throw new IllegalArgumentException("[right_child] must be a non-negative integer.");
                }
                if (splitIndex == null || splitIndex < 0) {
                    throw new IllegalArgumentException("[split_index] must be a non-negative integer.");
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
                splitIndex, 
                splitGain, 
                internalValue, 
                defaultLeft, 
                leftChild, 
                rightChild);
        }
    }
}
