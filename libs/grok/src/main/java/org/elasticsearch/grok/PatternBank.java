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
     * Checks whether patterns reference each other in a circular manner and if so fail with an IllegalArgumentException. It will also
     * fail if any pattern value contains a pattern name that does not exist in the bank.
     * <p>
     * In a pattern, anything between <code>%{</code> and <code>}</code> or <code>:</code> is considered
     * a reference to another named pattern. This method will navigate to all these named patterns and
     * check for a circular reference.
     */
    static void forbidCircularReferences(Map<String, String> bank) {
        Set<String> allVisitedNodes = new HashSet<>();
        Set<String> nodesVisitedMoreThanOnceInAPath = new HashSet<>();
        // Walk the full path starting at each node in the graph:
        for (String traversalStartNode : bank.keySet()) {
            if (nodesVisitedMoreThanOnceInAPath.contains(traversalStartNode) == false && allVisitedNodes.contains(traversalStartNode)) {
                // If we have seen this node before in a path, and it only appeared once in that path, there is no need to check it again
                continue;
            }
            Set<String> visitedFromThisStartNode = new LinkedHashSet<>();
            /*
             * This stack records where we are in the graph. Each String[] in the stack represents a collection of neighbors to the first
             * non-null node in the layer below it. Null means that the path from that location has been fully traversed. Once all nodes
             * at a layer have been set to null, the layer is popped. So for example say we have the graph
             * ( 1 -> (2 -> (4, 5, 8), 3 -> (6, 7))) then when we are at 6 via 1 -> 3 -> 6, the stack looks like this:
             * [6, 7]
             * [null, 3]
             * [1]
             */
            Deque<String[]> stack = new ArrayDeque<>();
            stack.push(new String[] { traversalStartNode });
            // This is used so that we know that we're unwinding the stack and know not to get the current node's neighbors again.
            boolean unwinding = false;
            while (stack.isEmpty() == false) {
                String[] currentLevel = stack.peek();
                int firstNonNullIndex = findFirstNonNull(currentLevel);
                String node = currentLevel[firstNonNullIndex];
                boolean endOfThisPath = false;
                if (unwinding) {
                    // We have completed all of this node's neighbors and have popped back to the node
                    endOfThisPath = true;
                } else if (traversalStartNode.equals(node) && stack.size() > 1) {
                    Deque<String> reversedPath = new ArrayDeque<>();
                    for (String[] level : stack) {
                        reversedPath.push(level[findFirstNonNull(level)]);
                    }
                    throw new IllegalArgumentException("circular reference detected: " + String.join("->", reversedPath));
                } else if (visitedFromThisStartNode.contains(node)) {
                    /*
                     * We are only looking for a cycle starting and ending at traversalStartNode right now. But this node has been
                     * visited more than once in the path rooted at traversalStartNode. This could be because it is a cycle, or could be
                     * because two nodes in the path both point to it. We add it to nodesVisitedMoreThanOnceInAPath so that we make sure
                     * to check the path rooted at this node later.
                     */
                    nodesVisitedMoreThanOnceInAPath.add(node);
                    endOfThisPath = true;
                } else {
                    visitedFromThisStartNode.add(node);
                    String[] neighbors = getPatternNamesForPattern(bank, node);
                    if (neighbors.length == 0) {
                        endOfThisPath = true;
                    } else {
                        stack.push(neighbors);
                    }
                }
                if (endOfThisPath) {
                    if (firstNonNullIndex == currentLevel.length - 1) {
                        // We have handled all the neighbors at this level -- there are no more non-null ones
                        stack.pop();
                        unwinding = true;
                    } else {
                        currentLevel[firstNonNullIndex] = null;
                        unwinding = false;
                    }
                } else {
                    unwinding = false;
                }
            }
            allVisitedNodes.addAll(visitedFromThisStartNode);
        }
    }

    private static int findFirstNonNull(String[] level) {
        for (int i = 0; i < level.length; i++) {
            if (level[i] != null) {
                return i;
            }
        }
        return -1;
    }

    /**
     * This method returns the array of pattern names (if any) found in the bank for the pattern named patternName. If no pattern names
     * are found, an empty array is returned. If any of the list of pattern names to be returned does not exist in the bank, an exception
     * is thrown.
     */
    private static String[] getPatternNamesForPattern(Map<String, String> bank, String patternName) {
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
        return patternReferences.toArray(new String[0]);
    }
}
