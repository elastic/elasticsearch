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

import org.elasticsearch.search.profile.query.QueryProfileBreakdown;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public abstract class AbstractInternalProfileTree<PB extends AbstractProfileBreakdown<?>, E> {

    protected ArrayList<PB> timings;
    /** Maps the Query to it's list of children.  This is basically the dependency tree */
    protected ArrayList<ArrayList<Integer>> tree;
    /** A list of the original queries, keyed by index position */
    protected ArrayList<E> elements;
    /** A list of top-level "roots".  Each root can have its own tree of profiles */
    protected ArrayList<Integer> roots;
    /** A temporary stack used to record where we are in the dependency tree. */
    protected Deque<Integer> stack;
    private int currentToken = 0;

    public AbstractInternalProfileTree() {
        timings = new ArrayList<>(10);
        stack = new ArrayDeque<>(10);
        tree = new ArrayList<>(10);
        elements = new ArrayList<>(10);
        roots = new ArrayList<>(10);
    }

    /**
     * Returns a {@link QueryProfileBreakdown} for a scoring query.  Scoring queries (e.g. those
     * that are past the rewrite phase and are now being wrapped by createWeight() ) follow
     * a recursive progression.  We can track the dependency tree by a simple stack
     *
     * The only hiccup is that the first scoring query will be identical to the last rewritten
     * query, so we need to take special care to fix that
     *
     * @param query The scoring query we wish to profile
     * @return      A ProfileBreakdown for this query
     */
    public PB getProfileBreakdown(E query) {
        int token = currentToken;

        boolean stackEmpty = stack.isEmpty();

        // If the stack is empty, we are a new root query
        if (stackEmpty) {

            // We couldn't find a rewritten query to attach to, so just add it as a
            // top-level root. This is just a precaution: it really shouldn't happen.
            // We would only get here if a top-level query that never rewrites for some reason.
            roots.add(token);

            // Increment the token since we are adding a new node, but notably, do not
            // updateParent() because this was added as a root
            currentToken += 1;
            stack.add(token);

            return addDependencyNode(query, token);
        }

        updateParent(token);

        // Increment the token since we are adding a new node
        currentToken += 1;
        stack.add(token);

        return addDependencyNode(query, token);
    }

    /**
     * Helper method to add a new node to the dependency tree.
     *
     * Initializes a new list in the dependency tree, saves the query and
     * generates a new {@link QueryProfileBreakdown} to track the timings of
     * this query
     *
     * @param element
     *            The element to profile
     * @param token
     *            The assigned token for this element
     * @return A ProfileBreakdown to profile this element
     */
    private PB addDependencyNode(E element, int token) {

        // Add a new slot in the dependency tree
        tree.add(new ArrayList<>(5));

        // Save our query for lookup later
        elements.add(element);

        PB queryTimings = createProfileBreakdown();
        timings.add(token, queryTimings);
        return queryTimings;
    }

    protected abstract PB createProfileBreakdown();

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
    public List<ProfileResult> getTree() {
        ArrayList<ProfileResult> results = new ArrayList<>(5);
        for (Integer root : roots) {
            results.add(doGetTree(root));
        }
        return results;
    }

    /**
     * Recursive helper to finalize a node in the dependency tree
     * @param token  The node we are currently finalizing
     * @return       A hierarchical representation of the tree inclusive of children at this level
     */
    private ProfileResult doGetTree(int token) {
        E element = elements.get(token);
        PB breakdown = timings.get(token);
        Map<String, Long> timings = breakdown.toTimingMap();
        List<Integer> children = tree.get(token);
        List<ProfileResult> childrenProfileResults = Collections.emptyList();

        if (children != null) {
            childrenProfileResults = new ArrayList<>(children.size());
            for (Integer child : children) {
                ProfileResult childNode = doGetTree(child);
                childrenProfileResults.add(childNode);
            }
        }

        // TODO this would be better done bottom-up instead of top-down to avoid
        // calculating the same times over and over...but worth the effort?
        String type = getTypeFromElement(element);
        String description = getDescriptionFromElement(element);
        return new ProfileResult(type, description, timings, childrenProfileResults);
    }

    protected abstract String getTypeFromElement(E element);

    protected abstract String getDescriptionFromElement(E element);

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

}
