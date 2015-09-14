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

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This class tracks the dependency tree for queries (scoring and rewriting) and
 * generates {@link InternalProfileBreakdown} for each node in the tree.  It also finalizes the tree
 * and returns a list of {@link InternalProfileResult} that can be serialized back to the client
 */
public class InternalProfileTree {

    private ArrayList<InternalProfileBreakdown> timings;

    /** Maps the Query to it's list of children.  This is basically the dependency tree */
    private ArrayList<ArrayList<Integer>> tree;

    /** A list of the original queries, keyed by index position */
    private ArrayList<Query> queries;

    /** A list of top-level "roots".  Each root can have its own tree of profiles */
    private ArrayList<Integer> roots;

    /** A map that translates a rewritten query to the original query's token */
    private Map<Query, Integer> rewriteMap;

    /** A temporary stack used to record where we are in the dependency tree.  Only used by scoring queries */
    private Deque<Integer> stack;

    private int currentToken = 0;

    public InternalProfileTree() {
        timings = new ArrayList<>(10);
        stack = new LinkedBlockingDeque<>(10);
        tree = new ArrayList<>(10);
        queries = new ArrayList<>(10);
        roots = new ArrayList<>(10);
        rewriteMap = new HashMap<>(5);
    }

    /** Get the {@link ProfileBreakdown} for the given query, potentially creating it if it did not exist. */
    public ProfileBreakdown getBreakDown(Query query, boolean rewrite) {
        return rewrite ? getRewriteBreakdown(query) : getQueryBreakdown(query);
    }

    /**
     * Returns a {@link ProfileBreakdown} for a rewriting query.  Rewrites don't follow a
     * recursive progression, so we cannot use the stack to track their progress.  Instead,
     * we check to see if they have a common rewritten "ancestor" query and link to that,
     * otherwise we treat it as a new root query
     *
     * @param query The rewriting query we wish to profile
     * @return      A ProfileBreakdown for this query
     */
    private ProfileBreakdown getRewriteBreakdown(Query query) {
        int token = currentToken;
        currentToken += 1;

        Integer parent = rewriteMap.get(query);

        // If we have this query in the rewriteMap, that means a previous
        // query rewrote into it ... meaning we can attach to it as a child
        if (parent != null) {
            ArrayList<Integer> parentNode = tree.get(parent);
            parentNode.add(token);
            tree.set(parent, parentNode);
        } else {

            // We couldn't find a rewritten query to
            // attach to, so just add it as a top-level root
            roots.add(token);
        }

        return addDependencyNode(query, token, true);
    }

    /**
     * Returns a {@link ProfileBreakdown} for a scoring query.  Scoring queries (e.g. those
     * that are past the rewrite phase and are now being wrapped by createWeight() ) follow
     * a recursive progression.  We can track the dependency tree by a simple stack
     *
     * The only hiccup is that the first scoring query will be identical to the last rewritten
     * query, so we need to take special care to fix that
     *
     * @param query The scoring query we wish to profile
     * @return      A ProfileBreakdown for this query
     */
    private ProfileBreakdown getQueryBreakdown(Query query) {
        int token = currentToken;

        boolean stackEmpty = stack.size() == 0;

        // If the stack is empty, we are either a new root query or we are the first
        // scoring query after rewriting
        if (stackEmpty) {
            Integer parentRewrite = rewriteMap.get(query);

            // If we find a rewritten query that is identical to us, we need to use
            // the Breakdown generated during the rewrite phase instead of creating
            // a new one
            if (parentRewrite != null) {

                stack.add(parentRewrite);
                InternalProfileBreakdown breakdown = timings.get(parentRewrite);

                // This breakdown no longer needs reconciling, since it will receive
                // scoring times directly
                breakdown.setNeedsReconciling(false);
                return breakdown;
            }

            // We couldn't find a rewritten query to attach to, so just add it as a
            // top-level root. This is just a precaution: it really shouldn't happen.
            // We would only get here if a top-level query that never rewrites for some reason.
            roots.add(token);
        }

        updateParent(token);

        // Increment the token since we are adding a new node
        currentToken += 1;
        stack.add(token);

        return addDependencyNode(query, token, false);
    }

    /**
     * Helper method to add a new node to the dependency tree.
     *
     * Initializes a new list in the dependency tree, saves the query and
     * generates a new {@link InternalProfileBreakdown} to track the timings
     * of this query
     *
     * @param query             The query to profile
     * @param token             The assigned token for this query
     * @param needsReconciling  Whether this timing breakdown needs reconciliation after profiling completes
     * @return                  A InternalProfileBreakdown to profile this query
     */
    private InternalProfileBreakdown addDependencyNode(Query query, int token, boolean needsReconciling) {

        // Add a new slot in the dependency tree
        tree.add(new ArrayList<>(5));

        // Save our query for lookup later
        queries.add(query);

        InternalProfileBreakdown queryTimings = new InternalProfileBreakdown(needsReconciling);
        timings.add(token, queryTimings);
        return queryTimings;
    }

    /**
     * Saves the rewritten version of the original query.
     *
     * This is used to construct the dependency tree for the rewritten portion
     * of the query, since it cannot follow the stack-based model
     *
     * @param original   The original query
     * @param rewritten  The query after rewriting
     */
    public void setRewrittenQuery(Query original, Query rewritten) {
        int token = queries.indexOf(original);
        if (token != -1) {
            rewriteMap.put(rewritten, token);
        }
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
    public List<InternalProfileResult> finalizeProfileResults() {
        ArrayList<InternalProfileResult> results = new ArrayList<>(5);
        for (Integer root : roots) {
            results.add(doFinalizeProfileResults(root));
        }
        return results;
    }

    /**
     * Recursive helper to finalize a node in the dependency tree
     * @param token  The node we are currently finalizing
     * @return       A hierarchical representation of the tree inclusive of children at this level
     */
    private InternalProfileResult doFinalizeProfileResults(int token) {

        Query query = queries.get(token);
        InternalProfileBreakdown breakdown = timings.get(token);
        InternalProfileResult rootNode =  new InternalProfileResult(query, breakdown);
        ArrayList<Integer> children = tree.get(token);

        // If a Breakdown was generated during the rewrite phase, it will only
        // have timings for the rewrite.  We need to "reconcile" those timings by
        // merging our time with the time of our children
        boolean needsReconciling = breakdown.needsReconciling();
        InternalProfileBreakdown reconciledBreakdown = null;

        if (needsReconciling) {
            reconciledBreakdown = new InternalProfileBreakdown(false);
        }

        for (Integer child : children) {
            InternalProfileResult childNode = doFinalizeProfileResults(child);
            rootNode.addChild(childNode);

            if (needsReconciling) {
                reconciledBreakdown.merge((InternalProfileBreakdown)childNode.getTimeBreakdown());
            }
        }

        if (needsReconciling) {
            reconciledBreakdown.merge(breakdown);
            rootNode.setTimings(reconciledBreakdown);
        }

        return rootNode;
    }


    /**
     * Internal helper to add a child to the current parent node
     *
     * @param childToken The child to add to the current parent
     */
    private void updateParent(int childToken) {
        int parent = stack.peekLast();
        ArrayList<Integer> parentNode = tree.get(parent);
        parentNode.add(childToken);
        tree.set(parent, parentNode);
    }

}
