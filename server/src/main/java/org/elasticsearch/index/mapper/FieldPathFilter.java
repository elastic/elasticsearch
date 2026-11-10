/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

/**
 * Filters the parsing of a mapping to a limited set of fields
 * <p>
 * All field parents are included: both container fields such as objects,
 * and multi-field parents.
 * <p>
 * Instances track the position of the parser.  A new one must be made
 * for each parse, and should not be shared across threads.
 */
public final class FieldPathFilter {

    private final List<String> patterns;
    private final Deque<String> path = new ArrayDeque<>();

    private FieldPathFilter(List<String> patterns) {
        this.patterns = patterns;
    }

    /**
     * A filter that keeps the fields matched by {@code fieldPatterns}, each of which is a field path that may use {@code *}
     * as a wildcard, as in {@link org.elasticsearch.common.regex.Regex#simpleMatch}.
     */
    public static FieldPathFilter forPatterns(Collection<String> fieldPatterns) {
        return new FieldPathFilter(List.copyOf(fieldPatterns));
    }

    /**
     * Enters the property {@code name} of the object being parsed, which may be a dotted path.
     *
     * @return {@code true} if the property is to be parsed, in which case the caller has to {@link #exit} afterwards.
     *         If {@code false} the property is to be skipped and there is nothing to exit.
     */
    boolean enter(String name) {
        String fullPath = path.isEmpty() ? name : path.peek() + "." + name;
        for (String pattern : patterns) {
            if (mayMatchFieldAtOrBelow(pattern, fullPath)) {
                path.push(fullPath);
                return true;
            }
        }
        return false;
    }

    void exit() {
        path.pop();
    }

    private static boolean mayMatchFieldAtOrBelow(String pattern, String fullPath) {
        int wildcard = pattern.indexOf('*');
        if (wildcard < 0) {
            return pattern.equals(fullPath) || pattern.startsWith(fullPath + ".");
        }
        // a wildcard may match dots too, so anything that starts like the pattern may match
        String literalPrefix = pattern.substring(0, wildcard);
        return fullPath.startsWith(literalPrefix) || literalPrefix.startsWith(fullPath + ".");
    }
}
