/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.aggregations.AggregationExecutionException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.sizeOfCollection;
import static org.apache.lucene.util.RamUsageEstimator.sizeOfMap;
import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationTokenTree.WILD_CARD;

/**
 * Tree node classes for the categorization token tree.
 *
 * Two major node types exist:
 *  - Inner: which are nodes that have children token nodes
 *  - Leaf: Which collection multiple {@link TextCategorization} based on similarity restrictions
 */
abstract class TreeNode implements Accountable {

    private static final Logger LOGGER = LogManager.getLogger(TreeNode.class);

    private long count;

    TreeNode(long count) {
        this.count = count;
    }

    abstract void mergeWith(TreeNode otherNode);

    abstract boolean isLeaf();

    final void incCount(long count) {
        this.count += count;
    }

    final long getCount() {
        return count;
    }

    // TODO add option for calculating the cost of adding the new group
    abstract TextCategorization addLog(BytesRef[] logTokens, long docCount, TreeNodeFactory treeNodeFactory);

    abstract TextCategorization getLogGroup(BytesRef[] logTokens);

    abstract List<TextCategorization> getAllChildrenLogGroups();

    abstract void collapseTinyChildren();

    static class LeafTreeNode extends TreeNode {
        private final List<TextCategorization> textCategorizations;
        private final double similarityThreshold;

        LeafTreeNode(long count, double similarityThreshold) {
            super(count);
            this.textCategorizations = new ArrayList<>();
            this.similarityThreshold = similarityThreshold;
        }

        public boolean isLeaf() {
            return true;
        }

        @Override
        void mergeWith(TreeNode treeNode) {
            if (treeNode == null) {
                return;
            }
            if (treeNode.isLeaf() == false) {
                throw new UnsupportedOperationException(
                    "cannot merge leaf node with non-leaf node in categorization tree \n[" + this + "]\n[" + treeNode + "]"
                );
            }
            incCount(treeNode.getCount());
            LeafTreeNode otherLeaf = (LeafTreeNode) treeNode;
            for (TextCategorization group : otherLeaf.textCategorizations) {
                if (getAndUpdateLogGroup(group.getCategorization(), group.getCount()).isPresent() == false) {
                    putNewLogGroup(group);
                }
            }
        }

        @Override
        public long ramBytesUsed() {
            return Long.BYTES // count
                + NUM_BYTES_OBJECT_REF // list reference
                + Double.BYTES  // similarityThreshold
                + sizeOfCollection(textCategorizations);
        }

        @Override
        public TextCategorization addLog(BytesRef[] logTokens, long docCount, TreeNodeFactory treeNodeFactory) {
            return getAndUpdateLogGroup(logTokens, docCount).orElseGet(() -> {
                // Need to update the tree if possible
                TextCategorization group = treeNodeFactory.newGroup(docCount, logTokens);
                LOGGER.trace(() -> new ParameterizedMessage("created group! [{}]", group));
                return putNewLogGroup(group);
            });
        }

        @Override
        List<TextCategorization> getAllChildrenLogGroups() {
            return textCategorizations;
        }

        @Override
        void collapseTinyChildren() {}

        private Optional<TextCategorization> getAndUpdateLogGroup(BytesRef[] logTokens, long docCount) {
            return getBestLogGroup(logTokens).map(bestGroupAndSimilarity -> {
                if (bestGroupAndSimilarity.v2() >= similarityThreshold) {
                    bestGroupAndSimilarity.v1().addLog(logTokens, docCount);
                    return bestGroupAndSimilarity.v1();
                }
                return null;
            });
        }

        TextCategorization putNewLogGroup(TextCategorization group) {
            textCategorizations.add(group);
            return group;
        }

        private Optional<Tuple<TextCategorization, Double>> getBestLogGroup(BytesRef[] logTokens) {
            if (textCategorizations.isEmpty()) {
                return Optional.empty();
            }
            if (textCategorizations.size() == 1) {
                return Optional.of(
                    new Tuple<>(textCategorizations.get(0), textCategorizations.get(0).calculateSimilarity(logTokens).getSimilarity())
                );
            }
            TextCategorization.Similarity maxSimilarity = null;
            TextCategorization bestGroup = null;
            for (TextCategorization textCategorization : this.textCategorizations) {
                TextCategorization.Similarity groupSimilarity = textCategorization.calculateSimilarity(logTokens);
                if (maxSimilarity == null || groupSimilarity.compareTo(maxSimilarity) > 0) {
                    maxSimilarity = groupSimilarity;
                    bestGroup = textCategorization;
                }
            }
            return Optional.of(new Tuple<>(bestGroup, maxSimilarity.getSimilarity()));
        }

        @Override
        public TextCategorization getLogGroup(final BytesRef[] logTokens) {
            return getBestLogGroup(logTokens).map(Tuple::v1).orElse(null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LeafTreeNode that = (LeafTreeNode) o;
            return Double.compare(that.similarityThreshold, similarityThreshold) == 0
                && Objects.equals(textCategorizations, that.textCategorizations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(textCategorizations, similarityThreshold);
        }
    }

    static class InnerTreeNode extends TreeNode {

        private final Map<BytesRef, TreeNode> children;
        private final int childrenTokenPos;
        private final int maxChildren;
        private final PriorityQueue<Tuple<BytesRef, Long>> smallestChild;

        InnerTreeNode(long count, int childrenTokenPos, int maxChildren) {
            super(count);
            children = new HashMap<>();
            this.childrenTokenPos = childrenTokenPos;
            this.maxChildren = maxChildren;
            this.smallestChild = new PriorityQueue<>(maxChildren, Comparator.comparing(Tuple::v2));
        }

        boolean isLeaf() {
            return false;
        }

        @Override
        public TextCategorization getLogGroup(final BytesRef[] logTokens) {
            return getChild(logTokens[childrenTokenPos]).or(() -> getChild(WILD_CARD))
                .map(node -> node.getLogGroup(logTokens))
                .orElse(null);
        }

        @Override
        public long ramBytesUsed() {
            return Long.BYTES // count
                + NUM_BYTES_OBJECT_REF // children reference
                + Integer.BYTES // childrenTokenPos
                + Integer.BYTES // maxChildren
                + NUM_BYTES_OBJECT_REF // smallestChildReference
                + sizeOfMap(children, 0)
                // Number of items in the queue, reference to tuple, and then the tuple references
                + (long) smallestChild.size() * (NUM_BYTES_OBJECT_REF + NUM_BYTES_OBJECT_REF + NUM_BYTES_OBJECT_REF + Long.BYTES);
        }

        @Override
        public TextCategorization addLog(final BytesRef[] logTokens, final long docCount, final TreeNodeFactory treeNodeFactory) {
            BytesRef currentToken = logTokens[childrenTokenPos];
            TreeNode child = getChild(currentToken).map(node -> {
                node.incCount(docCount);
                if (smallestChild.isEmpty() == false && smallestChild.peek().v1().equals(currentToken)) {
                    smallestChild.add(smallestChild.poll());
                }
                return node;
            }).orElseGet(() -> {
                if (docCount > 1) {
                    LOGGER.trace(
                        () -> new ParameterizedMessage(
                            "got a token [{}] with doc_count [{}] percentage [{}]",
                            logTokens[childrenTokenPos].utf8ToString(),
                            docCount,
                            (double) docCount / docCount
                        )
                    );
                }
                TreeNode newNode = treeNodeFactory.newNode(docCount, childrenTokenPos + 1, logTokens);
                return addChild(currentToken, newNode);
            });
            return child.addLog(logTokens, docCount, treeNodeFactory);
        }

        @Override
        void collapseTinyChildren() {
            if (this.isLeaf()) {
                return;
            }
            if (children.size() <= 1) {
                return;
            }
            Optional<TreeNode> maybeWildChild = getChild(WILD_CARD).or(() -> {
                if ((double) smallestChild.peek().v2() / this.getCount() <= 1.0 / maxChildren) {
                    TreeNode tinyChild = children.remove(smallestChild.poll().v1());
                    return Optional.of(addChild(WILD_CARD, tinyChild));
                }
                return Optional.empty();
            });
            if (maybeWildChild.isPresent()) {
                TreeNode wildChild = maybeWildChild.get();
                Tuple<BytesRef, Long> tinyNode;
                while ((tinyNode = smallestChild.poll()) != null) {
                    // If we have no more tiny nodes, stop iterating over them
                    if ((double) tinyNode.v2() / this.getCount() > 1.0 / maxChildren) {
                        smallestChild.add(tinyNode);
                        break;
                    } else {
                        wildChild.mergeWith(children.remove(tinyNode.v1()));
                    }
                }
            }
            children.values().forEach(TreeNode::collapseTinyChildren);
        }

        @Override
        void mergeWith(TreeNode treeNode) {
            if (treeNode == null) {
                return;
            }
            incCount(treeNode.count);
            if (treeNode.isLeaf()) {
                throw new UnsupportedOperationException(
                    "cannot merge non-leaf node with leaf node in categorization tree \n[" + this + "]\n[" + treeNode + "]"
                );
            }
            InnerTreeNode innerTreeNode = (InnerTreeNode) treeNode;
            TreeNode siblingWildChild = innerTreeNode.children.remove(WILD_CARD);
            addChild(WILD_CARD, siblingWildChild);
            Tuple<BytesRef, Long> siblingChild;
            while ((siblingChild = innerTreeNode.smallestChild.poll()) != null) {
                TreeNode nephewNode = innerTreeNode.children.remove(siblingChild.v1());
                addChild(siblingChild.v1(), nephewNode);
            }
        }

        private TreeNode addChild(BytesRef token, TreeNode node) {
            if (node == null || token == null) {
                return null;
            }
            Optional<TreeNode> existingChild = getChild(token).map(existingNode -> {
                existingNode.mergeWith(node);
                if (smallestChild.isEmpty() == false && smallestChild.peek().v1().equals(token)) {
                    smallestChild.poll();
                    smallestChild.add(Tuple.tuple(token, existingNode.getCount()));
                }
                return existingNode;
            });
            if (existingChild.isPresent()) {
                return existingChild.get();
            }
            if (children.size() == maxChildren) {
                return getChild(WILD_CARD).map(wildChild -> {
                    final TreeNode toMerge;
                    final TreeNode toReturn;
                    if (smallestChild.isEmpty() == false && node.getCount() > smallestChild.peek().v2()) {
                        toMerge = children.remove(smallestChild.poll().v1());
                        addChildAndUpdateSmallest(token, node);
                        toReturn = node;
                    } else {
                        toMerge = node;
                        toReturn = wildChild;
                    }
                    wildChild.mergeWith(toMerge);
                    return toReturn;
                }).orElseThrow(() -> new AggregationExecutionException("Missing wild_card child even though maximum children reached"));
            }
            // we are about to hit the limit, add a wild card if we need to and then add the new child as appropriate
            if (children.size() == maxChildren - 1) {
                // If we already have a wild token, simply adding the new token is acceptable as we won't breach our limit
                if (children.containsKey(WILD_CARD)) {
                    addChildAndUpdateSmallest(token, node);
                } else { // if we don't have a wild card child, we need to add one now
                    if (token.equals(WILD_CARD)) {
                        addChildAndUpdateSmallest(token, node);
                    } else {
                        if (smallestChild.isEmpty() == false && node.count > smallestChild.peek().v2()) {
                            addChildAndUpdateSmallest(WILD_CARD, children.remove(smallestChild.poll().v1()));
                            addChildAndUpdateSmallest(token, node);
                        } else {
                            addChildAndUpdateSmallest(WILD_CARD, node);
                        }
                    }
                }
            } else {
                addChildAndUpdateSmallest(token, node);
            }
            return node;
        }

        private void addChildAndUpdateSmallest(BytesRef token, TreeNode node) {
            children.put(token, node);
            if (token.equals(WILD_CARD) == false) {
                smallestChild.add(Tuple.tuple(token, node.count));
            }
        }

        private Optional<TreeNode> getChild(BytesRef token) {
            TreeNode node = children.get(token);
            return node == null ? Optional.empty() : Optional.of(node);
        }

        public List<TextCategorization> getAllChildrenLogGroups() {
            return children.values().stream().flatMap(c -> c.getAllChildrenLogGroups().stream()).collect(Collectors.toList());
        }

        boolean hasChild(BytesRef value) {
            return children.containsKey(value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InnerTreeNode treeNode = (InnerTreeNode) o;
            return childrenTokenPos == treeNode.childrenTokenPos
                && getCount() == treeNode.getCount()
                && Objects.equals(children, treeNode.children)
                && Objects.equals(smallestChild, treeNode.smallestChild);
        }

        @Override
        public int hashCode() {
            return Objects.hash(children, childrenTokenPos, smallestChild, getCount());
        }
    }

}
