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

public class InternalProfileTree {

    private ArrayList<InternalProfileBreakdown> timings;

    /**
     * Maps the Query to it's list of children.  This is basically
     * the dependency tree
     */
    private ArrayList<ArrayList<Integer>> tree;

    private ArrayList<Query> queries;

    private ArrayList<Integer> roots;

    private Map<Query, Integer> rewriteMap;

    /**
     * A temporary stack used to record where we are in the dependency
     * tree.
     */
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

    private ProfileBreakdown getRewriteBreakdown(Query query) {
        int token = currentToken;
        currentToken += 1;

        Integer parent = rewriteMap.get(query);

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

    private ProfileBreakdown getQueryBreakdown(Query query) {
        int token = currentToken;

        boolean stackEmpty = stack.size() == 0;

        if (stackEmpty) {
            Integer parentRewrite = rewriteMap.get(query);

            if (parentRewrite != null) {
                // push the already-crated rewrite Timing onto the stack
                stack.add(parentRewrite);
                InternalProfileBreakdown breakdown = timings.get(parentRewrite);
                breakdown.setNeedsReconciling(false);
                return breakdown;
            }

            // We couldn't find a rewritten query to
            // attach to, so just add it as a top-level root
            roots.add(token);
        }

        updateParent(token);

        // Only increment the token if we are adding a new node
        currentToken += 1;
        stack.add(token);

        return addDependencyNode(query, token, false);
    }

    private InternalProfileBreakdown addDependencyNode(Query query, int token, boolean needsReconciling) {

        // Add a new slot in the dependency tree
        tree.add(new ArrayList<>(5));

        // Save our query for lookup later
        queries.add(query);

        InternalProfileBreakdown queryTimings = new InternalProfileBreakdown(needsReconciling);
        timings.add(token, queryTimings);
        return queryTimings;
    }

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
