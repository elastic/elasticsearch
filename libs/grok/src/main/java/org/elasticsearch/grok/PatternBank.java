/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

public class PatternBank {

    public static PatternBank EMPTY = new PatternBank(Map.of());

    private final Map<String, String> bank;

    public PatternBank(Map<String, String> bank) {
        Objects.requireNonNull(bank, "bank must not be null");
        forbidCircularReferences(bank);

        // the bank reference should be unmodifiable, based on a defensive copy of the passed-in bank, and
        // maintain the iteration order of the passed-in bank (assuming there was a meaningful order)
        this.bank = Collections.unmodifiableMap(new LinkedHashMap<>(bank));
    }

    public String get(String patternName) {
        return bank.get(patternName);
    }

    public Map<String, String> bank() {
        return bank;
    }

    /**
     * Extends a pattern bank with extra patterns, returning a new pattern bank.
     * <p>
     * The returned bank will be the same reference as the original pattern bank if the extra patterns map is null or empty.
     *
     * @param extraPatterns the patterns to extend this bank with (may be empty or null)
     * @return the extended pattern bank
     */
    public PatternBank extendWith(Map<String, String> extraPatterns) {
        if (extraPatterns == null || extraPatterns.isEmpty()) {
            return this;
        }

        var extendedBank = new LinkedHashMap<>(bank);
        extendedBank.putAll(extraPatterns);
        return new PatternBank(extendedBank);
    }

    /**
     * Checks whether patterns reference each other in a circular manner and if so fail with an exception.
     * <p>
     * In a pattern, anything between <code>%{</code> and <code>}</code> or <code>:</code> is considered
     * a reference to another named pattern. This method will navigate to all these named patterns and
     * check for a circular reference.
     */
    static void forbidCircularReferences(Map<String, String> bank) {
        detectCycles(bank, PatternBank::getPatternNamesForPattern);
    }

    /*
     * This method traverses the directed graph, and throws an IllegalArgumentException if any cycles are detected.
     * @param directedGraph A directed graph. The keys are all the nodes in the graph. The values are strings that can be converted to
     *                     a list of neighboring nodes using nodeToNeighborsFunction
     * @param nodeToNeighborsFunction A function that maps the values of directedGraph to a list of neighboring nodes
     */
    private static void detectCycles(
        Map<String, String> directedGraph,
        BiFunction<Map<String, String>, String, List<String>> nodeToNeighborsFunction
    ) {
        Set<String> allVisitedNodes = new HashSet<>();
        Set<String> nodesVisitedMoreThanOnceInAPath = new HashSet<>();
        // Walk the full path starting at each node in the graph:
        for (String traversalStartNode : directedGraph.keySet()) {
            if (nodesVisitedMoreThanOnceInAPath.contains(traversalStartNode) == false && allVisitedNodes.contains(traversalStartNode)) {
                // If we have seen this node before in a path, and it only appeared once in that path, there is no need to check it again
                continue;
            }
            Set<String> visited = new LinkedHashSet<>();
            Deque<String> toBeVisited = new ArrayDeque<>();
            toBeVisited.push(traversalStartNode);
            while (toBeVisited.isEmpty() == false) {
                String node = toBeVisited.pop();
                if (visited.isEmpty() == false && traversalStartNode.equals(node)) {
                    throw new IllegalArgumentException("circular reference detected: " + String.join("->", visited) + "->" + node);
                } else if (visited.contains(node)) {
                    /*
                     * We are only looking for a cycle starting and ending at traversalStartNode right now. But this node has bee been
                     * visited more than once in the path rooted at traversalStartNode. This could be because it is a cycle, or could be
                     * because two nodes in the path both point to it. We add it to nodesVisitedMoreThanOnceInAPath so that we make sure
                     * to check the path rooted at this node later.
                     */
                    nodesVisitedMoreThanOnceInAPath.add(node);
                    continue;
                }
                visited.add(node);
                for (String neighbor : nodeToNeighborsFunction.apply(directedGraph, node)) {
                    toBeVisited.push(neighbor);
                }
            }
            allVisitedNodes.addAll(visited);
        }
    }

    /**
     * This method returns the list of pattern names (if any) found in the bank for the pattern named patternName. If no pattern names
     * are found, and empty list is returned. If any of the list of pattern names to be returned does not exist in the bank, an exception
     * is thrown.
     */
    private static List<String> getPatternNamesForPattern(Map<String, String> bank, String patternName) {
        String pattern = bank.get(patternName);
        List<String> patternReferences = new ArrayList<>();
        for (int i = pattern.indexOf("%{"); i != -1; i = pattern.indexOf("%{", i + 1)) {
            int begin = i + 2;
            int bracketIndex = pattern.indexOf('}', begin);
            int columnIndex = pattern.indexOf(':', begin);
            int end;
            if (bracketIndex != -1 && columnIndex == -1) {
                end = bracketIndex;
            } else if (columnIndex != -1 && bracketIndex == -1) {
                end = columnIndex;
            } else if (bracketIndex != -1) {
                end = Math.min(bracketIndex, columnIndex);
            } else {
                throw new IllegalArgumentException("pattern [" + pattern + "] has an invalid syntax");
            }
            String otherPatternName = pattern.substring(begin, end);
            if (patternReferences.contains(otherPatternName) == false) {
                patternReferences.add(otherPatternName);
                String otherPattern = bank.get(otherPatternName);
                if (otherPattern == null) {
                    throw new IllegalArgumentException(
                        "pattern [" + patternName + "] is referencing a non-existent pattern [" + otherPatternName + "]"
                    );
                }
            }
        }
        return patternReferences;
    }
}
