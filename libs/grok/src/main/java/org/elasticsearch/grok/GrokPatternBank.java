/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record GrokPatternBank(Map<String, String> bank) {

    public GrokPatternBank {
        bank = Map.copyOf(bank);
        forbidCircularReferences();
    }


    /**
     * Checks whether patterns reference each other in a circular manner and if so fail with an exception
     *
     * In a pattern, anything between <code>%{</code> and <code>}</code> or <code>:</code> is considered
     * a reference to another named pattern. This method will navigate to all these named patterns and
     * check for a circular reference.
     */
    private void forbidCircularReferences() {

        // first ensure that the pattern bank contains no simple circular references (i.e., any pattern
        // containing an immediate reference to itself) as those can cause the remainder of this algorithm
        // to recurse infinitely
        for (Map.Entry<String, String> entry : bank.entrySet()) {
            if (patternReferencesItself(entry.getValue(), entry.getKey())) {
                throw new IllegalArgumentException("circular reference in pattern [" + entry.getKey() + "][" + entry.getValue() + "]");
            }
        }

        // next, recursively check any other pattern names referenced in each pattern
        for (Map.Entry<String, String> entry : bank.entrySet()) {
            String name = entry.getKey();
            String pattern = entry.getValue();
            innerForbidCircularReferences(name, new ArrayList<>(), pattern);
        }
    }

    private void innerForbidCircularReferences(String patternName, List<String> path, String pattern) {
        if (patternReferencesItself(pattern, patternName)) {
            String message;
            if (path.isEmpty()) {
                message = "circular reference in pattern [" + patternName + "][" + pattern + "]";
            } else {
                message = "circular reference in pattern ["
                    + path.remove(path.size() - 1)
                    + "]["
                    + pattern
                    + "] back to pattern ["
                    + patternName
                    + "]";
                // add rest of the path:
                if (path.isEmpty() == false) {
                    message += " via patterns [" + String.join("=>", path) + "]";
                }
            }
            throw new IllegalArgumentException(message);
        }

        // next check any other pattern names found in the pattern
        for (int i = pattern.indexOf("%{"); i != -1; i = pattern.indexOf("%{", i + 1)) {
            int begin = i + 2;
            int bracketIndex = pattern.indexOf('}', begin);
            int columnIndex = pattern.indexOf(':', begin);
            int end;
            if (bracketIndex != -1 && columnIndex == -1) {
                end = bracketIndex;
            } else if (columnIndex != -1 && bracketIndex == -1) {
                end = columnIndex;
            } else if (bracketIndex != -1 && columnIndex != -1) {
                end = Math.min(bracketIndex, columnIndex);
            } else {
                throw new IllegalArgumentException("pattern [" + pattern + "] has circular references to other pattern definitions");
            }
            String otherPatternName = pattern.substring(begin, end);
            path.add(otherPatternName);
            innerForbidCircularReferences(patternName, path, bank.get(otherPatternName));
        }
    }

    private static boolean patternReferencesItself(String pattern, String patternName) {
        return pattern.contains("%{" + patternName + "}") || pattern.contains("%{" + patternName + ":");
    }
}
