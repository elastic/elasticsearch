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
import org.elasticsearch.common.Nullable;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This class tracks the dependency tree for queries (scoring and rewriting) and
 * generates {@link ProfileBreakdown} for each node in the tree.  It also finalizes the tree
 * and returns a list of {@link InternalProfileResult} that can be serialized back to the client
 */
public class InternalProfileTree {

    private static Map<String, Long> mergeTimings(Iterable<Map<String, Long>> timings) {
        Iterator<Map<String, Long>> timingIt = timings.iterator();
        Map<String, Long> merged = new HashMap<>(ProfileBreakdown.EMPTY_TIMINGS);
        while (timingIt.hasNext()) {
            for (Map.Entry<String, Long> entry : timingIt.next().entrySet()) {
                merged.put(entry.getKey(), merged.get(entry.getKey()) + entry.getValue());
            }
        }
        return Collections.unmodifiableMap(merged);
    }

    private ArrayList<ProfileBreakdown> timings;

    /** Maps the Query to it's list of children.  This is basically the dependency tree */
    private ArrayList<ArrayList<Integer>> tree;

    /** A list of the original queries, keyed by index position */
    private ArrayList<Query> queries;

    /** A list of top-level "roots".  Each root can have its own tree of profiles */
    private ArrayList<Integer> roots;

    /** A map that translates a rewritten query to the original query's token */
    private Map<RewrittenQuery, Integer> rewriteMap;

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
        return rewrite ? new ProfileBreakdown(true) : getQueryBreakdown(query);
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
            Integer parentToken = getRewrittenParentToken(query);

            // If we find a rewritten query that is identical to us, we need to use
            // the Breakdown generated during the rewrite phase instead of creating
            // a new one
            if (parentToken != null) {

                stack.add(parentToken);
                ProfileBreakdown breakdown = timings.get(parentToken);

                // This breakdown no longer needs reconciling, since it will receive
                // scoring times directly
                breakdown.setNeedsReconciling(false);
                return breakdown;
            }

            // We couldn't find a rewritten query to attach to, so just add it as a
            // top-level root. This is just a precaution: it really shouldn't happen.
            // We would only get here if a top-level query that never rewrites for some reason.
            roots.add(token);

            // Increment the token since we are adding a new node, but notably, do not
            // updateParent() because this was added as a root
            currentToken += 1;
            stack.add(token);

            return addDependencyNode(query, token, false);
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
     * generates a new {@link ProfileBreakdown} to track the timings
     * of this query
     *
     * @param query             The query to profile
     * @param token             The assigned token for this query
     * @param needsReconciling  Whether this timing breakdown needs reconciliation after profiling completes
     * @return                  A ProfileBreakdown to profile this query
     */
    private ProfileBreakdown addDependencyNode(Query query, int token, boolean needsReconciling) {

        // Add a new slot in the dependency tree
        tree.add(new ArrayList<>(5));

        // Save our query for lookup later
        queries.add(query);

        ProfileBreakdown queryTimings = new ProfileBreakdown(needsReconciling);
        timings.add(token, queryTimings);
        return queryTimings;
    }

    /**
     * Attaches a dangling Breakdown to the profiling timing tree.  This method should be used
     * to attach Breakdowns generated for rewrites -- it should not be used for "scoring" queries
     * as they are already part of the timing tree
     *
     * This method inspects the (original, rewritten) tuple to determine if the query rewrote
     * into itself, if the original is a child of a previous rewrite, or if the rewrite is a new
     * "root" query
     *
     * @param original   The original query
     * @param rewritten  The query after rewriting
     */
    public void setRewrittenQuery(Query original, Query rewritten, ProfileBreakdown breakdown) {

        RewrittenQuery r = new RewrittenQuery(original, rewritten);

        // If the rewrite maps to itself, merge the timing in with the existin entry
        if (rewriteMap.containsKey(r)) {
            int token = rewriteMap.get(r);
            ProfileBreakdown timing = timings.get(token);
            timing.merge(breakdown);
            return;
        }

        // If the rewrite pair wasn't found, see if there is an entry whose rewritten
        // is our original

        Integer parentToken = getRewrittenParentToken(original);
        if (parentToken != null) {
            // Found a "parent" rewrite, add as a child
            addRewriteToTree(r, parentToken, breakdown);
            return;
        }

        // No parent, so this will add it as a root
        addRewriteToTree(r, breakdown);
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
        ProfileBreakdown breakdown = timings.get(token);
        InternalProfileResult rootNode =  new InternalProfileResult(query, breakdown.toTimingMap());
        ArrayList<Integer> children = tree.get(token);

        // If a Breakdown was generated during the rewrite phase, it will only
        // have timings for the rewrite.  We need to "reconcile" those timings by
        // merging our time with the time of our children
        boolean needsReconciling = breakdown.needsReconciling();
        List<Map<String, Long>> timings = null;

        if (needsReconciling) {
            timings = new ArrayList<>();
        }

        for (Integer child : children) {
            InternalProfileResult childNode = doFinalizeProfileResults(child);
            rootNode.addChild(childNode);

            if (needsReconciling) {
                timings.add(childNode.getTimeBreakdown());
            }
        }

        if (needsReconciling) {
            rootNode.setTimings(mergeTimings(timings));
        }

        return rootNode;
    }


    /**
     * Internal helper to add a child to the current parent node
     *
     * @param childToken The child to add to the current parent
     */
    private void updateParent(int childToken) {
        Integer parent = stack.peekLast();
        ArrayList<Integer> parentNode = tree.get(parent);
        parentNode.add(childToken);
        tree.set(parent, parentNode);
    }

    /**
     * Convenience overload for {@link #addRewriteToTree(RewrittenQuery, Integer, ProfileBreakdown)}
     * when there is no parent to the query
     *
     * @see #addRewriteToTree(RewrittenQuery, Integer, ProfileBreakdown) for more details
     */
    private void addRewriteToTree(RewrittenQuery r, ProfileBreakdown breakdown) {
        addRewriteToTree(r, null, breakdown);
    }

    /**
     * Adds the rewritten query to the dependency tree as a child to `parentToken`, or as a root if
     * `parentToken` is null.
     *
     * @param r            The (original, rewritten) tuple to add
     * @param parentToken  The parent to this rewritten query, or null if there are no parents
     * @param breakdown    The profiled timings for this query
     */
    private void addRewriteToTree(RewrittenQuery r, Integer parentToken, ProfileBreakdown breakdown) {

        int token = currentToken;
        currentToken += 1;

        if (parentToken != null) {
            // Update our parent, and add ourselves to the tree
            ArrayList<Integer> parentNode = tree.get(parentToken);
            parentNode.add(token);
            tree.set(parentToken, parentNode);
            tree.add(new ArrayList<>(5));
        } else {
            roots.add(token);
        }

        // Save the query and retroactively add ourself ot the timing list
        tree.add(new ArrayList<>(5));
        queries.add(r.original);
        timings.add(token, breakdown);
        rewriteMap.put(r, token);
    }

    /**
     * Searches through the rewrite map to see if the query exists as a
     * rewritten version of a different query (which means the current query
     * is a child of a previous one).
     *
     * It does this by linearly iterating over the entrySet and evaluating
     * each key.  There is probably a more clever way to do this
     *
     * @param query The query we wish to check for rewritten parents
     * @return      The integer token of our parent, or null if there is no parent
     */
    private @Nullable Integer getRewrittenParentToken(Query query) {
        return getRewrittenParentToken(query, -1, 0);
    }

    private @Nullable Integer getRewrittenParentToken(Query query, int lastToken, int level) {

        // Recursion safeguard, just in case pathological queries are provided.
        // Returning null will just mess up the hierarchy, all the various
        // components will still be recorded correctly
        if (level > 10) {
            return null;
        }

        // TODO better way to do this?
        for (Map.Entry<RewrittenQuery, Integer> entry : rewriteMap.entrySet()) {
            if (entry.getKey().rewritten.equals(query) && entry.getValue() > lastToken) {
                // Found a matching query, but we need to check if there is a
                // "descendant" rewrite that would be more appropriate to embed under
                Integer descendant = getRewrittenParentToken(entry.getKey().rewritten, entry.getValue(), level + 1);

                return descendant != null ? descendant : entry.getValue();
            }
        }

        return null;
    }

    /**
     * A utility tuple class which allows us to store (original, rewritten)
     * tuples in the map.  Equality and hashcode are based on the underlying
     * queries.
     */
    public class RewrittenQuery {
        public Query original;
        public Query rewritten;

        public RewrittenQuery(Query original, Query rewritten) {
            this.original = original;
            this.rewritten = rewritten;
        }

        @Override
        public boolean equals(Object obj) {

            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (!(obj instanceof RewrittenQuery)) {
                return false;
            }

            RewrittenQuery r = (RewrittenQuery) obj;

            return r.original.equals(original) && r.rewritten.equals(rewritten);
        }

        @Override
        public int hashCode() {
            return original.hashCode() + rewritten.hashCode();
        }
    }

}
