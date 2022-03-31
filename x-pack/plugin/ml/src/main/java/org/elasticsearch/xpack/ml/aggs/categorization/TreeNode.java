/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
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
import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash.WILD_CARD_ID;

/**
 * Tree node classes for the categorization token tree.
 *
 * Two major node types exist:
 *  - Inner: which are nodes that have children token nodes
 *  - Leaf: Which collection multiple {@link TextCategorization} based on similarity restrictions
 */
abstract class TreeNode implements Accountable {

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
    abstract TextCategorization addText(int[] tokenIds, long docCount, CategorizationTokenTree treeNodeFactory);

    abstract TextCategorization getCategorization(int[] tokenIds);

    abstract List<TextCategorization> getAllChildrenTextCategorizations();

    abstract void collapseTinyChildren();

    static class LeafTreeNode extends TreeNode {
        private final List<TextCategorization> textCategorizations;
        private final int similarityThreshold;

        LeafTreeNode(long count, int similarityThreshold) {
            super(count);
            this.textCategorizations = new ArrayList<>();
            this.similarityThreshold = similarityThreshold;
            if (similarityThreshold < 1 || similarityThreshold > 100) {
                throw new IllegalArgumentException("similarityThreshold must be between 1 and 100");
            }
        }

        @Override
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
                if (getAndUpdateTextCategorization(group.getCategorization(), group.getCount()).isPresent() == false) {
                    putNewTextCategorization(group);
                }
            }
        }

        @Override
        public long ramBytesUsed() {
            return Long.BYTES // count
                + NUM_BYTES_OBJECT_REF // list reference
                + Integer.BYTES  // similarityThreshold
                + sizeOfCollection(textCategorizations);
        }

        @Override
        public TextCategorization addText(int[] tokenIds, long docCount, CategorizationTokenTree treeNodeFactory) {
            return getAndUpdateTextCategorization(tokenIds, docCount).orElseGet(() -> {
                // Need to update the tree if possible
                TextCategorization categorization = putNewTextCategorization(treeNodeFactory.newCategorization(docCount, tokenIds));
                // Get the regular size bytes from the TextCategorization and how much it costs to reference it
                treeNodeFactory.incSize(categorization.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
                return categorization;
            });
        }

        @Override
        List<TextCategorization> getAllChildrenTextCategorizations() {
            return textCategorizations;
        }

        @Override
        void collapseTinyChildren() {}

        private Optional<TextCategorization> getAndUpdateTextCategorization(int[] tokenIds, long docCount) {
            return getBestCategorization(tokenIds).map(bestGroupAndSimilarity -> {
                if ((bestGroupAndSimilarity.v2() * 100) >= similarityThreshold) {
                    bestGroupAndSimilarity.v1().addTokens(tokenIds, docCount);
                    return bestGroupAndSimilarity.v1();
                }
                return null;
            });
        }

        TextCategorization putNewTextCategorization(TextCategorization categorization) {
            textCategorizations.add(categorization);
            return categorization;
        }

        private Optional<Tuple<TextCategorization, Double>> getBestCategorization(int[] tokenIds) {
            if (textCategorizations.isEmpty()) {
                return Optional.empty();
            }
            if (textCategorizations.size() == 1) {
                return Optional.of(
                    new Tuple<>(textCategorizations.get(0), textCategorizations.get(0).calculateSimilarity(tokenIds).getSimilarity())
                );
            }
            TextCategorization.Similarity maxSimilarity = null;
            TextCategorization bestGroup = null;
            for (TextCategorization textCategorization : this.textCategorizations) {
                TextCategorization.Similarity groupSimilarity = textCategorization.calculateSimilarity(tokenIds);
                if (maxSimilarity == null || groupSimilarity.compareTo(maxSimilarity) > 0) {
                    maxSimilarity = groupSimilarity;
                    bestGroup = textCategorization;
                }
            }
            return Optional.of(new Tuple<>(bestGroup, maxSimilarity.getSimilarity()));
        }

        @Override
        public TextCategorization getCategorization(final int[] tokenIds) {
            return getBestCategorization(tokenIds).map(Tuple::v1).orElse(null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LeafTreeNode that = (LeafTreeNode) o;
            return that.similarityThreshold == similarityThreshold && Objects.equals(textCategorizations, that.textCategorizations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(textCategorizations, similarityThreshold);
        }
    }

    static class InnerTreeNode extends TreeNode {

        // TODO: Change to LongObjectMap?
        private final Map<Integer, TreeNode> children;
        private final int childrenTokenPos;
        private final int maxChildren;
        private final PriorityQueue<NativeIntLongPair> smallestChild;

        InnerTreeNode(long count, int childrenTokenPos, int maxChildren) {
            super(count);
            children = new HashMap<>();
            this.childrenTokenPos = childrenTokenPos;
            this.maxChildren = maxChildren;
            this.smallestChild = new PriorityQueue<>(maxChildren, Comparator.comparing(NativeIntLongPair::count));
        }

        @Override
        boolean isLeaf() {
            return false;
        }

        @Override
        public TextCategorization getCategorization(final int[] tokenIds) {
            return getChild(tokenIds[childrenTokenPos]).or(() -> getChild(WILD_CARD_ID))
                .map(node -> node.getCategorization(tokenIds))
                .orElse(null);
        }

        @Override
        public long ramBytesUsed() {
            return Long.BYTES // count
                + NUM_BYTES_OBJECT_REF // children reference
                + Integer.BYTES // childrenTokenPos
                + Integer.BYTES // maxChildren
                + NUM_BYTES_OBJECT_REF // smallestChildReference
                + sizeOfMap(children, NUM_BYTES_OBJECT_REF) // children,
                // Number of items in the queue, reference to tuple, and then the tuple references
                + (long) smallestChild.size() * (NUM_BYTES_OBJECT_REF + Integer.BYTES + Long.BYTES);
        }

        @Override
        public TextCategorization addText(final int[] tokenIds, final long docCount, final CategorizationTokenTree treeNodeFactory) {
            final int currentToken = tokenIds[childrenTokenPos];
            TreeNode child = getChild(currentToken).map(node -> {
                node.incCount(docCount);
                if (smallestChild.isEmpty() == false && smallestChild.peek().tokenId == currentToken) {
                    smallestChild.add(smallestChild.poll());
                }
                return node;
            }).orElseGet(() -> {
                TreeNode newNode = treeNodeFactory.newNode(docCount, childrenTokenPos + 1, tokenIds);
                // The size of the node + entry (since it is a map entry) + extra reference for priority queue
                treeNodeFactory.incSize(
                    newNode.ramBytesUsed() + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + RamUsageEstimator.NUM_BYTES_OBJECT_REF
                );
                return addChild(currentToken, newNode);
            });
            return child.addText(tokenIds, docCount, treeNodeFactory);
        }

        @Override
        void collapseTinyChildren() {
            if (this.isLeaf()) {
                return;
            }
            if (children.size() <= 1) {
                return;
            }
            Optional<TreeNode> maybeWildChild = getChild(WILD_CARD_ID).or(() -> {
                if (smallestChild.size() > 0 && (double) smallestChild.peek().count / this.getCount() <= 1.0 / maxChildren) {
                    TreeNode tinyChild = children.remove(smallestChild.poll().tokenId);
                    return Optional.of(addChild(WILD_CARD_ID, tinyChild));
                }
                return Optional.empty();
            });
            if (maybeWildChild.isPresent()) {
                TreeNode wildChild = maybeWildChild.get();
                NativeIntLongPair tinyNode;
                while ((tinyNode = smallestChild.poll()) != null) {
                    // If we have no more tiny nodes, stop iterating over them
                    if ((double) tinyNode.count / this.getCount() > 1.0 / maxChildren) {
                        smallestChild.add(tinyNode);
                        break;
                    } else {
                        wildChild.mergeWith(children.remove(tinyNode.tokenId));
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
            TreeNode siblingWildChild = innerTreeNode.children.remove(WILD_CARD_ID);
            addChild(WILD_CARD_ID, siblingWildChild);
            NativeIntLongPair siblingChild;
            while ((siblingChild = innerTreeNode.smallestChild.poll()) != null) {
                TreeNode nephewNode = innerTreeNode.children.remove(siblingChild.tokenId);
                addChild(siblingChild.tokenId, nephewNode);
            }
        }

        private TreeNode addChild(int tokenId, TreeNode node) {
            if (node == null) {
                return null;
            }
            Optional<TreeNode> existingChild = getChild(tokenId).map(existingNode -> {
                existingNode.mergeWith(node);
                if (smallestChild.isEmpty() == false && smallestChild.peek().tokenId == tokenId) {
                    smallestChild.poll();
                    smallestChild.add(NativeIntLongPair.of(tokenId, existingNode.getCount()));
                }
                return existingNode;
            });
            if (existingChild.isPresent()) {
                return existingChild.get();
            }
            if (children.size() == maxChildren) {
                return getChild(WILD_CARD_ID).map(wildChild -> {
                    final TreeNode toMerge;
                    final TreeNode toReturn;
                    if (smallestChild.isEmpty() == false && node.getCount() > smallestChild.peek().count) {
                        toMerge = children.remove(smallestChild.poll().tokenId);
                        addChildAndUpdateSmallest(tokenId, node);
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
                if (children.containsKey(WILD_CARD_ID)) {
                    addChildAndUpdateSmallest(tokenId, node);
                } else { // if we don't have a wild card child, we need to add one now
                    if (tokenId == WILD_CARD_ID) {
                        addChildAndUpdateSmallest(tokenId, node);
                    } else {
                        if (smallestChild.isEmpty() == false && node.count > smallestChild.peek().count) {
                            addChildAndUpdateSmallest(WILD_CARD_ID, children.remove(smallestChild.poll().tokenId));
                            addChildAndUpdateSmallest(tokenId, node);
                        } else {
                            addChildAndUpdateSmallest(WILD_CARD_ID, node);
                        }
                    }
                }
            } else {
                addChildAndUpdateSmallest(tokenId, node);
            }
            return node;
        }

        private void addChildAndUpdateSmallest(int tokenId, TreeNode node) {
            children.put(tokenId, node);
            if (tokenId != WILD_CARD_ID) {
                smallestChild.add(NativeIntLongPair.of(tokenId, node.count));
            }
        }

        private Optional<TreeNode> getChild(int tokenId) {
            return Optional.ofNullable(children.get(tokenId));
        }

        public List<TextCategorization> getAllChildrenTextCategorizations() {
            return children.values().stream().flatMap(c -> c.getAllChildrenTextCategorizations().stream()).collect(Collectors.toList());
        }

        boolean hasChild(int tokenId) {
            return children.containsKey(tokenId);
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

    private static class NativeIntLongPair {
        private final int tokenId;
        private final long count;

        static NativeIntLongPair of(int tokenId, long count) {
            return new NativeIntLongPair(tokenId, count);
        }

        NativeIntLongPair(int tokenId, long count) {
            this.tokenId = tokenId;
            this.count = count;
        }

        public long count() {
            return count;
        }
    }

}
