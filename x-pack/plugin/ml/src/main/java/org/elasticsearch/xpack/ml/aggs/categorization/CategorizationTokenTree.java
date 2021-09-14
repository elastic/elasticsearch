/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

/**
 * Categorized semi-structured text utilizing the drain algorithm: https://arxiv.org/pdf/1806.04356.pdf
 * With the following key differntiators
 *  - This structure keeps track of the "smallest" sub-tree. So, instead of naively adding a new "*" node, the smallest sub-tree
 *    is transformed if the incoming token has a higher doc_count.
 *  - Additionally, similarities are weighted, which allows for nicer merging of existing log categories
 *  - An optional tree reduction step is available to collapse together tiny sub-trees
 *
 *
 * The main implementation is a fixed-sized prefix tree.
 * Consequently, this assumes that splits that give us more information come earlier in the text.
 *
 * Examples:
 *
 * Given log values:
 *
 * Node is online
 * Node is offline
 *
 * With a fixed tree depth of 2 we would get the following splits
 *                  3 // initial root is the number of tokens
 *                  |
 *               "Node" // first prefix node of value "Node"
 *                 |
 *               "is"
 *              /    \
 * [Node is online] [Node is offline] //the individual categories for this simple case
 *
 * If the similarityThreshold was less than 0.6, the result would be a single category [Node is *]
 *
 */
public class CategorizationTokenTree implements Accountable, TreeNodeFactory {

    static final BytesRef WILD_CARD = new BytesRef("*");
    private static final Logger LOGGER = LogManager.getLogger(CategorizationTokenTree.class);

    private final int maxDepth;
    private final int maxChildren;
    private final double similarityThreshold;
    private final AtomicLong idGen = new AtomicLong();
    // TODO statically allocate an array like DuplicateByteSequenceSpotter ???
    private final Map<Integer, TreeNode> root = new HashMap<>();
    private long sizeInBytes;

    public CategorizationTokenTree(int maxChildren, int maxDepth, double similarityThreshold) {
        assert maxChildren > 0 && maxDepth >= 0;
        this.maxChildren = maxChildren;
        this.maxDepth = maxDepth;
        this.similarityThreshold = similarityThreshold;
        this.sizeInBytes = Integer.BYTES // maxDepth
            + Integer.BYTES // maxChildren
            + Double.BYTES // similarityThreshold
            + NUM_BYTES_OBJECT_REF + Long.BYTES // idGen
            + NUM_BYTES_OBJECT_REF // tree map
            + Long.BYTES; // sizeInBytes
    }

    public List<InternalCategorizationAggregation.Bucket> toIntermediateBuckets() {
        return root.values().stream().flatMap(c -> c.getAllChildrenLogGroups().stream()).map(lg -> {
            InternalCategorizationAggregation.Bucket bucket = new InternalCategorizationAggregation.Bucket(
                new InternalCategorizationAggregation.BucketKey(lg.getCategorization()),
                lg.getCount(),
                InternalAggregations.EMPTY
            );
            bucket.bucketOrd = lg.bucketOrd;
            return bucket;
        }).collect(Collectors.toList());
    }

    void mergeSmallestChildren() {
        root.values().forEach(TreeNode::collapseTinyChildren);
    }

    public TextCategorization parseLogLine(final BytesRef[] logTokens) {
        return parseLogLine(logTokens, 1);
    }

    public TextCategorization parseLogLineConst(final BytesRef[] logTokens) {
        TreeNode currentNode = this.root.get(logTokens.length);
        if (currentNode == null) { // we are missing an entire sub tree. New log length found
            return null;
        }
        return currentNode.getLogGroup(logTokens);
    }

    public TextCategorization parseLogLine(final BytesRef[] logTokens, long docCount) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("parsing tokens [{}]", Arrays.stream(logTokens).map(BytesRef::utf8ToString).collect(Collectors.joining(" ")));
        }
        TreeNode currentNode = this.root.get(logTokens.length);
        if (currentNode == null) { // we are missing an entire sub tree. New log length found
            currentNode = newNode(docCount, 0, logTokens);
            this.root.put(logTokens.length, currentNode);
        } else {
            currentNode.incCount(docCount);
        }
        return currentNode.addLog(logTokens, docCount, this);
    }

    @Override
    public TreeNode newNode(long docCount, int tokenPos, BytesRef[] tokens) {
        TreeNode node = tokenPos < maxDepth - 1 && tokenPos < tokens.length
            ? new TreeNode.InnerTreeNode(docCount, tokenPos, maxChildren)
            : new TreeNode.LeafTreeNode(docCount, similarityThreshold);
        // The size of the node + entry (since it is a map entry) + extra reference for priority queue
        sizeInBytes += node.ramBytesUsed() + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return node;
    }

    @Override
    public TextCategorization newGroup(long docCount, BytesRef[] logTokens) {
        TextCategorization group = new TextCategorization(logTokens, docCount, idGen.incrementAndGet());
        // Get the regular size bytes from the LogGroup and how much it costs to reference it
        sizeInBytes += group.ramBytesUsed() + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return group;
    }

    @Override
    public long ramBytesUsed() {
        return sizeInBytes;
    }

}
