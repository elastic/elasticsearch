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

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.CollectorResult.CollectorReason;

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
    private ArrayList<InternalProfileBreakdown> timings;

    private ArrayList<InternalProfileBreakdown> rewriteTimings;

    /**
     * Maps the Query to it's list of children.  This is basically
     * the dependency tree
     */
    private ArrayList<ArrayList<Integer>> tree;

    private ArrayList<Query> queries;

    private ArrayList<Query> rewriteQueries;

    private ArrayList<Integer> roots;

    /**
     * A temporary stack used to record where we are in the dependency
     * tree.
     */
    private Deque<Integer> stack;

    /**
     * The root Query at the top of the tree
     */
    //private int root;

    private int currentToken = 0;

    private int currentRewriteToken = 0;

    /**
     * The root Collector used in the search
     */
    private InternalProfileCollector collector;

    public InternalProfiler() {
        timings = new ArrayList<>(10);
        rewriteTimings = new ArrayList<>(10);
        stack = new LinkedBlockingDeque<>(10);
        tree = new ArrayList<>(10);
        queries = new ArrayList<>(10);
        rewriteQueries = new ArrayList<>(10);
        roots = new ArrayList<>(5);
    }

    /** Get the {@link ProfileBreakdown} for the given query, potentially creating it if it did not exist. */
    public ProfileBreakdown getProfileBreakDown(int token) {
        InternalProfileBreakdown queryTimings = new InternalProfileBreakdown();
        timings.add(token, queryTimings);
        return queryTimings;
    }

    public ProfileBreakdown getRewriteProfileBreakDown(int token) {
        InternalProfileBreakdown queryTimings = new InternalProfileBreakdown();
        rewriteTimings.add(token, queryTimings);
        return queryTimings;
    }

    public void appendRewrittenQuery(int token, Query rewriten) {

    }


    /**
     * Push the query onto the dependency stack so that we can record where
     * we are in the query tree.  This should be called only once per node.
     *
     * @param query  The query that we are currently visiting
     */
    public int getToken(Query query) {
        int token = currentToken;
        currentToken += 1;

        if (stack.size() != 0) {
            updateParent(token);
        } else {
            roots.add(token);
        }

        // Add a new slot in the dependency tree
        tree.add(new ArrayList<>(5));

        // Save our query for lookup later
        queries.add(query);

        //addNode(token);
        stack.add(token);

        return token;
    }

    public int getRewriteToken(Query query) {
        int token = currentRewriteToken;
        currentRewriteToken += 1;

        rewriteQueries.add(query);

        return token;
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

        for (int i = 0; i < rewriteTimings.size(); i++) {
            Query query = rewriteQueries.get(i);
            InternalProfileBreakdown timing = rewriteTimings.get(i);
            InternalProfileResult rewriteNode =  new InternalProfileResult(query, timing);
            results.add(rewriteNode);
        }
        return results;
    }

    public InternalProfileCollector finalizeCollectors() {
        return collector;
    }

    /**
     * Return the current root Collector for this search
     */
    public InternalProfileCollector getCollector() {
        return collector;
    }

    /**
     * Set the current root Collector.  Note there is no cycle
     * protection!
     *
     * @param collector The new Collector which should become root
     */
    public void setCollector(InternalProfileCollector collector) {
        assert(collector != null);
        assert(this.collector == null || !this.collector.equals(collector));
        this.collector = collector;
    }

    /**
     * Helper method to wrap a Collector in an InternalProfileCollector and create
     * the dependency tree in the process
     *
     * @param profiler The InternalProfiler associated with the search context
     * @param original The Collector to be wrapped
     * @param purpose  A "hint" for the user to understand the context the Collector is being used in
     * @return         A Collector which has been wrapped for profiling
     */
    public static Collector wrapCollector(InternalProfiler profiler, Collector original, CollectorReason purpose) {
        if (profiler != null && !(original instanceof InternalProfileCollector)) {
            InternalProfileCollector c = new InternalProfileCollector(original, purpose);

            // Add the existing collector as a child to our newly wrapped one,
            // then set it as the root collector
            c.addChild(profiler.getCollector());
            profiler.setCollector(c);
            return c;
        }
        return original;
    }

    /**
     * Convenience helper for wrapCollector() when you do not have / do not know an
     * appropriate Reason "hint".  Uses GENERAL as the Reason
     *
     * @param profiler The InternalProfiler associated with the search context
     * @param original The Collector to be wrapped
     * @return         A Collector which has been wrapped for profiling
     */
    public static Collector wrapCollector(InternalProfiler profiler, Collector original) {
        return wrapCollector(profiler, original, CollectorReason.GENERAL);
    }

    /**
     * Helper method to wrap BucketCollectors which are not being used for global
     * aggregations
     *
     * @param profiler The InternalProfiler associated with the search context
     * @param original The Collector to be wrapped
     * @return         A Collector which has been wrapped for profiling
     */
    public static Collector wrapBucketCollector(InternalProfiler profiler, Collector original) {
        return wrapBucketCollector(profiler, original, false);
    }

    /**
     * Helper method to wrap BucketCollectors.  If the bucket is a global aggregation,
     * it will adjust the dependency tree as necessary, otherwise the instantiated
     * InternalProfileCollector is left "dangling", since it will be wired into the graph
     * later
     *
     * @param profiler The InternalProfiler associated with the search context
     * @param original The Collector to be wrapped
     * @param global   True if this Collector is being used as a global aggregation
     * @return         A Collector which has been wrapped for profiling
     */
    public static Collector wrapBucketCollector(InternalProfiler profiler, Collector original, boolean global) {
        if (profiler != null && !(original instanceof InternalProfileCollector)) {
            if (global) {
                // Global aggs are built after all search phase is done, so
                // global agg collectors must be appended to the root collector.
                InternalProfileCollector c = new InternalProfileCollector(original, CollectorReason.AGGREGATION_GLOBAL);
                profiler.getCollector().addChild(c);
                return c;
            }
            // Otherwise this is a non-global agg, so it'll be wrapped by a multi later and we don't need
            // to set children now
            return new InternalProfileCollector(original, CollectorReason.AGGREGATION);
        }
        return original;
    }

    /**
     * Helper method to wrap MultiCollectors in an InternalProfileCollector.
     *
     * This method will wrap a Multi if it hasn't been wrapped yet.  It will then
     * wire up the dependency tree such that the Multi has all the children in `constituents`.
     * Ideally, all Collectors in `constituents` will be pre-wrapped in an InternalProfileCollector,
     * but if not, this method will also wrap them as GENERAL
     *
     * `constituents` param is needed since there is no way to ask a Collector if it wraps one or
     * more Collectors, or to retrieve those.  Instead we rely on the calling code to provide the
     * list.
     *
     * @param profiler      The InternalProfiler associated with the search context
     * @param multi         The MultiCollector to wrap
     * @param constituents  The list of Collectors that the MultiCollector contains
     * @return              A wrapped MultiCollector
     */
    public static Collector wrapMultiCollector(InternalProfiler profiler, Collector multi, List<Collector> constituents) {
        if (profiler != null) {

            // If the multicollector hasn't been wrapped yet, wrap it
            if (!(multi instanceof InternalProfileCollector)) {
                multi = new InternalProfileCollector(multi, CollectorReason.SEARCH_MULTI);
            } else if (constituents.size() == 1) {
                // If multi is wrapped already, and size is one, the child has already
                // been configured and we need to bail otherwise we create a cycle
                // pointing to our self
                return multi;
            }

            // Walk through all the children of the multi and point them as children
            // to the multi
            for (Collector c : constituents) {

                // Safety mechanism.  Hopefully all collectors were wrapped in the
                // calling code, but if not, wrap them in a generic ProfileCollector
                if (!(c instanceof InternalProfileCollector)) {
                    c = new InternalProfileCollector(c, CollectorReason.GENERAL);
                }
                ((InternalProfileCollector) multi).addChild((InternalProfileCollector) c);
            }

            // Add the existing collector as a child to our newly wrapped one,
            // then set it as the root collector
            ((InternalProfileCollector) multi).addChild(profiler.getCollector());
            profiler.setCollector((InternalProfileCollector) multi);
        }
        return multi;
    }

    /**
     * Recursive helper to finalize a node in the dependency tree
     * @param token  The node we are currently finalizing
     * @return       A hierarchical representation of the tree inclusive of children at this level
     */
    private InternalProfileResult doFinalizeProfileResults(int token) {

        Query query = queries.get(token);
        InternalProfileResult rootNode =  new InternalProfileResult(query, timings.get(token));
        ArrayList<Integer> children = tree.get(token);

        for (Integer child : children) {
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
    private void addNode(int query) {
        //tree.put(query, new ArrayList<Query>(5));
        tree.add(new ArrayList<>(5));
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
