/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
            innerForbidCircularReferences(bank, name, new ArrayList<>(), pattern);
        }
    }

    private static void innerForbidCircularReferences(Map<String, String> bank, String patternName, List<String> path, String pattern) {
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
                throw new IllegalArgumentException("pattern [" + pattern + "] has an invalid syntax");
            }
            String otherPatternName = pattern.substring(begin, end);
            path.add(otherPatternName);
            String otherPattern = bank.get(otherPatternName);
            if (otherPattern == null) {
                throw new IllegalArgumentException(
                    "pattern [" + patternName + "] is referencing a non-existent pattern [" + otherPatternName + "]"
                );
            }

            innerForbidCircularReferences(bank, patternName, path, otherPattern);
        }
    }

    private static boolean patternReferencesItself(String pattern, String patternName) {
        return pattern.contains("%{" + patternName + "}") || pattern.contains("%{" + patternName + ":");
    }
}
