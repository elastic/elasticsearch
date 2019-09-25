/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference.trainedmodel.tree;

import org.elasticsearch.client.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Tree implements TrainedModel {

    public static final String NAME = "tree";

    public static final ParseField FEATURE_NAMES = new ParseField("feature_names");
    public static final ParseField TREE_STRUCTURE = new ParseField("tree_structure");

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, true, Builder::new);

    static {
        PARSER.declareStringArray(Builder::setFeatureNames, FEATURE_NAMES);
        PARSER.declareObjectArray(Builder::setNodes, (p, c) -> TreeNode.fromXContent(p), TREE_STRUCTURE);
    }

    public static Tree fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).build();
    }

    private final List<String> featureNames;
    private final List<TreeNode> nodes;

    Tree(List<String> featureNames, List<TreeNode> nodes) {
        this.featureNames = Collections.unmodifiableList(Objects.requireNonNull(featureNames));
        this.nodes = Collections.unmodifiableList(Objects.requireNonNull(nodes));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<String> getFeatureNames() {
        return featureNames;
    }

    public List<TreeNode> getNodes() {
        return nodes;
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
        public Builder addLeaf(int nodeIndex, double value) {
            for (int i = nodes.size(); i < nodeIndex + 1; i++) {
                nodes.add(null);
            }
            nodes.set(nodeIndex, TreeNode.builder(nodeIndex).setLeafValue(value));
            return this;
        }

        public Tree build() {
            return new Tree(featureNames,
                nodes.stream().map(TreeNode.Builder::build).collect(Collectors.toList()));
        }
    }

}
