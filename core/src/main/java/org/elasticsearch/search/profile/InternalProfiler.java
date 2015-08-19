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

package org.elasticsearch.search.profile;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.InternalProfileResult;
import org.elasticsearch.search.profile.InternalProfileBreakdown;
import org.elasticsearch.search.profile.ProfileBreakdown;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This class acts as a thread-local storage for profiling a query.  It also
 * builds a representation of the query tree which is built constructed
 * "online" as the weights are wrapped by ContextIndexSearcher.  This allows us
 * to know the relationship between nodes in tree without explicitly
 * walking the tree or pre-wrapping everything
 */
public class InternalProfiler {

    /**
     * Maps the Query to it's profiled timings
     */
    private Map<Query, InternalProfileBreakdown> timings;

    /**
     * Maps the Query to it's list of children.  This is basically
     * the dependency tree
     */
    private Map<Query, ArrayList<Query>> tree;

    /**
     * A temporary stack used to record where we are in the dependency
     * tree.
     */
    private Deque<Query> stack;

    /**
     * The root Query at the top of the tree
     */
    private Query root;

    public InternalProfiler() {
        timings = new HashMap<>(10);
        stack = new LinkedBlockingDeque<>(10);
        tree = new HashMap<>(10);
    }

    /**
     * Start timing a query for a specific Timing context
     * @param query     The query to start profiling
     * @param timing    The Timing context to time
     */
    public void startTime(Query query, ProfileBreakdown.TimingType timing) {
        InternalProfileBreakdown queryTimings = timings.get(query);

        if (queryTimings == null) {
            queryTimings = new InternalProfileBreakdown();
        }

        queryTimings.startTime(timing);
        timings.put(query, queryTimings);
    }

    /**
     * Stop and save the elapsed time for this query for a specific Timing context
     * @param query     The query to stop profiling
     * @param timing    The Timing context to time
     */
    public void stopAndRecordTime(Query query, ProfileBreakdown.TimingType timing) {
        InternalProfileBreakdown queryTimings = timings.get(query);
        queryTimings.stopAndRecordTime(timing);
        timings.put(query, queryTimings);
    }

    /**
     * When a query is rewritten, the top-level Query will no longer match the one
     * saved in our tree.  This method essentially swaps the rewritten query
     * for the old version.  This works because, currently, the profiler can
     * only record the total rewrite time for the top-level query rather than individual
     * rewrite times for each node in the tree.  We only need to alter the root
     * node to reconcile the change.
     *
     * If we ever start timing all the individual rewrites, this hack will no longer
     * work ;)
     *
     * @param original      The original query
     * @param rewritten     The rewritten query
     */
    public void reconcileRewrite(Query original, Query rewritten) {

        // If the original and rewritten are identical, no need to reconcile
        if (original.equals(rewritten)) {
            return;
        }

        InternalProfileBreakdown originalTimings = timings.get(original);

        InternalProfileBreakdown rewrittenTimings = timings.get(rewritten);
        if (rewrittenTimings == null) {
            rewrittenTimings = new InternalProfileBreakdown();
        }
        rewrittenTimings.setTime(ProfileBreakdown.TimingType.REWRITE, originalTimings.getTime(ProfileBreakdown.TimingType.REWRITE));
        timings.put(rewritten, rewrittenTimings);
        timings.remove(original);
    }

    /**
     * Push the query onto the dependency stack so that we can record where
     * we are in the query tree.  This should be called only once per node.
     *
     * @param query  The query that we are currently visiting
     */
    public void pushQuery(Query query) {
        if (stack.size() != 0) {
            updateParent(query);
        } else {
            root = query;
        }

        addNode(query);
        stack.add(query);
    }

    /**
     * Removes the last (e.g. most recent) value on the stack
     */
    public void pollLast() {
        stack.pollLast();
    }

    /**
     * After the query has been run and profiled, we need to merge the flat timing map
     * with the dependency graph to build a data structure that mirrors the original
     * query tree
     *
     * @return a hierarchical representation of the profiled query tree
     */
    public InternalProfileResult finalizeProfileResults() {
        return doFinalizeProfileResults(root);
    }

    /**
     * Recursive helper to finalize a node in the dependency tree
     * @param query  The node we are currently finalizing
     * @return       A hierarchical representation of the tree inclusive of children at this level
     */
    private InternalProfileResult doFinalizeProfileResults(Query query) {
        InternalProfileResult rootNode =  new InternalProfileResult(query, timings.get(query));
        ArrayList<Query> children = tree.get(query);

        for (Query child : children) {
            InternalProfileResult childNode = doFinalizeProfileResults(child);
            rootNode.addChild(childNode);
        }

        return rootNode;
    }

    /**
     * Internal helper to add initialize a new node in the dependency tree.
     * Initialized with an empty list of children
     *
     * @param query  The query to add to the tree
     */
    private void addNode(Query query) {
        tree.put(query, new ArrayList<Query>(5));
    }

    /**
     * Internal helper to add a child to the current parent node
     *
     * @param child The child to add to the current parent
     */
    private void updateParent(Query child) {
        Query parent = stack.peekLast();
        ArrayList<Query> parentNode = tree.get(parent);
        parentNode.add(child);
        tree.put(parent, parentNode);
    }
}
