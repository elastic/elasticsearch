/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.tree;

import org.elasticsearch.client.ml.job.config.Operator;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class TreeNode implements ToXContentObject {

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


    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
            NAME,
            true,
            Builder::new);
    static {
        PARSER.declareDouble(Builder::setThreshold, THRESHOLD);
        PARSER.declareField(Builder::setOperator,
            p -> Operator.fromString(p.text()),
            DECISION_TYPE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(Builder::setLeftChild, LEFT_CHILD);
        PARSER.declareInt(Builder::setRightChild, RIGHT_CHILD);
        PARSER.declareBoolean(Builder::setDefaultLeft, DEFAULT_LEFT);
        PARSER.declareInt(Builder::setSplitFeature, SPLIT_FEATURE);
        PARSER.declareInt(Builder::setNodeIndex, NODE_INDEX);
        PARSER.declareDouble(Builder::setSplitGain, SPLIT_GAIN);
        PARSER.declareDoubleArray(Builder::setLeafValue, LEAF_VALUE);
        PARSER.declareLong(Builder::setNumberSamples, NUMBER_SAMPLES);
    }

    public static Builder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Operator operator;
    private final Double threshold;
    private final Integer splitFeature;
    private final int nodeIndex;
    private final Double splitGain;
    private final List<Double> leafValue;
    private final Boolean defaultLeft;
    private final Integer leftChild;
    private final Integer rightChild;
    private final Long numberSamples;


    TreeNode(Operator operator,
             Double threshold,
             Integer splitFeature,
             int nodeIndex,
             Double splitGain,
             List<Double> leafValue,
             Boolean defaultLeft,
             Integer leftChild,
             Integer rightChild,
             Long numberSamples) {
        this.operator = operator;
        this.threshold  = threshold;
        this.splitFeature = splitFeature;
        this.nodeIndex = nodeIndex;
        this.splitGain  = splitGain;
        this.leafValue = leafValue;
        this.defaultLeft = defaultLeft;
        this.leftChild  = leftChild;
        this.rightChild = rightChild;
        this.numberSamples = numberSamples;
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

    public List<Double> getLeafValue() {
        return leafValue;
    }

    public Boolean isDefaultLeft() {
        return defaultLeft;
    }

    public Integer getLeftChild() {
        return leftChild;
    }

    public Integer getRightChild() {
        return rightChild;
    }

    public Long getNumberSamples() {
        return numberSamples;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        addOptionalField(builder, DECISION_TYPE, operator);
        addOptionalField(builder, THRESHOLD, threshold);
        addOptionalField(builder, SPLIT_FEATURE, splitFeature);
        addOptionalField(builder, SPLIT_GAIN, splitGain);
        addOptionalField(builder, NODE_INDEX, nodeIndex);
        addOptionalField(builder, LEAF_VALUE, leafValue);
        addOptionalField(builder, DEFAULT_LEFT, defaultLeft );
        addOptionalField(builder, LEFT_CHILD, leftChild);
        addOptionalField(builder, RIGHT_CHILD, rightChild);
        addOptionalField(builder, NUMBER_SAMPLES, numberSamples);
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
            leafValue,
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
        private Long numberSamples;

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

        public Builder setNodeIndex(int nodeIndex) {
            this.nodeIndex = nodeIndex;
            return this;
        }

        public Builder setSplitGain(Double splitGain) {
            this.splitGain = splitGain;
            return this;
        }

        public Builder setLeafValue(List<Double> leafValue) {
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

        public Builder setNumberSamples(Long numberSamples) {
            this.numberSamples = numberSamples;
            return this;
        }

        public TreeNode build() {
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
