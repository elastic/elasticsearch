/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.support.filtering;

import org.elasticsearch.core.Glob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterPath {
    private static final String WILDCARD = "*";
    private static final String DOUBLE_WILDCARD = "**";

    // This is ridiculously large, but we can be 100% certain that if any filter tries to exceed this depth then it is a mistake
    static final int MAX_TREE_DEPTH = 500;

    private final Map<String, FilterPath> termsChildren;
    private final FilterPath[] wildcardChildren;
    private final String pattern;
    private final boolean isDoubleWildcard;
    private final boolean isFinalNode;
    /**
     * When {@code true}, an incomplete suffix match on this node may still match at a deeper object nesting level. Set for nodes
     * beneath a single-segment {@code *} wildcard (map parity); not set under {@code **}.
     */
    private final boolean deferSuffixAcrossObjects;

    private FilterPath(
        String pattern,
        boolean isFinalNode,
        Map<String, FilterPath> termsChildren,
        FilterPath[] wildcardChildren,
        boolean deferSuffixAcrossObjects
    ) {
        this.pattern = pattern;
        this.isFinalNode = isFinalNode;
        this.termsChildren = Collections.unmodifiableMap(termsChildren);
        this.wildcardChildren = wildcardChildren;
        this.isDoubleWildcard = pattern.equals(DOUBLE_WILDCARD);
        this.deferSuffixAcrossObjects = deferSuffixAcrossObjects;
    }

    public boolean hasDoubleWildcard() {
        if (isDoubleWildcard || pattern.contains(DOUBLE_WILDCARD)) {
            return true;
        }
        for (FilterPath filterPath : wildcardChildren) {
            if (filterPath.hasDoubleWildcard()) {
                return true;
            }
        }
        for (FilterPath filterPath : termsChildren.values()) {
            if (filterPath.hasDoubleWildcard()) {
                return true;
            }
        }
        return false;
    }

    private String getPattern() {
        return pattern;
    }

    private boolean isFinalNode() {
        return isFinalNode;
    }

    /**
     * check if the name matches filter nodes
     * if the name equals the filter node name, the node will add to nextFilters.
     * if the filter node is a final node, it means the name matches the pattern, and return true
     * if the name don't equal a final node, then return false, continue to check the inner filter node
     * if current node is a double wildcard node, the node will also add to nextFilters.
     * @param name the xcontent property name
     * @param nextFilters nextFilters is a List, used to check the inner property of name
     * @param matchFieldNamesWithDots support dot in field name or not
     * @return true if the name equal a final node, otherwise return false
     */
    public boolean matches(String name, List<FilterPath> nextFilters, boolean matchFieldNamesWithDots) {
        return matches(name, nextFilters, matchFieldNamesWithDots, false);
    }

    /**
     * Matches a field name while optionally retaining incomplete wildcard matches across nested objects.
     *
     * @param name field name to match
     * @param nextFilters receives filters that may match child fields
     * @param matchFieldNamesWithDots whether dots in field names are treated as path separators
     * @param deferIncompleteMatches whether to enable map-parity suffix backtracking across nested objects
     * @return {@code true} if the field name completes this filter path
     */
    public boolean matches(String name, List<FilterPath> nextFilters, boolean matchFieldNamesWithDots, boolean deferIncompleteMatches) {
        if (nextFilters == null) {
            return false;
        }

        // match dot first
        if (matchFieldNamesWithDots) {
            // contains dot and not the first or last char
            int dotIndex = name.indexOf('.');
            if ((dotIndex != -1) && (dotIndex != 0) && (dotIndex != name.length() - 1)) {
                return matchFieldNamesWithDots(name, dotIndex, nextFilters);
            }
        }
        return matchSegment(name, nextFilters, deferIncompleteMatches);
    }

    private boolean matchFieldNamesWithDots(String name, int dotIndex, List<FilterPath> nextFilters) {
        String prefixName = name.substring(0, dotIndex);
        String suffixName = name.substring(dotIndex + 1);
        List<FilterPath> prefixFilterPath = new ArrayList<>();
        // Defer only across nested object fields, not across segments of one field name containing dots.
        boolean prefixMatch = matches(prefixName, prefixFilterPath, true, false);
        // if prefixMatch return true(because prefix is a final FilterPath node)
        if (prefixMatch) {
            return true;
        }
        // if has prefixNextFilter, use them to match suffix
        for (FilterPath filter : prefixFilterPath) {
            boolean matches = filter.matches(suffixName, nextFilters, true, false);
            if (matches) {
                return true;
            }
        }
        return false;
    }

    /**
     * Match one path segment against this node. When {@code deferIncompleteMatches} is {@code true}, keep this node active if
     * the segment did not complete the pattern but the pattern suffix may still match at a deeper nesting level (map parity).
     */
    private boolean matchSegment(String name, List<FilterPath> nextFilters, boolean deferIncompleteMatches) {
        int nextFiltersSizeBefore = nextFilters.size();

        FilterPath termNode = termsChildren.get(name);
        if (termNode != null) {
            if (termNode.isFinalNode()) {
                return true;
            } else {
                nextFilters.add(termNode);
            }
        }

        for (FilterPath wildcardNode : wildcardChildren) {
            String wildcardPattern = wildcardNode.getPattern();
            if (Glob.globMatch(wildcardPattern, name)) {
                if (wildcardNode.isFinalNode()) {
                    return true;
                } else {
                    nextFilters.add(wildcardNode);
                }
            }
        }

        if (isDoubleWildcard) {
            nextFilters.add(this);
        } else if (shouldDeferWildcardMatch(nextFilters, nextFiltersSizeBefore, deferIncompleteMatches)) {
            nextFilters.add(this);
        } else if (shouldDeferSuffixMatch(nextFilters, nextFiltersSizeBefore, deferIncompleteMatches)) {
            nextFilters.add(this);
        }

        return false;
    }

    private boolean shouldDeferWildcardMatch(List<FilterPath> nextFilters, int nextFiltersSizeBefore, boolean deferIncompleteMatches) {
        return deferIncompleteMatches
            && WILDCARD.equals(pattern)
            && isFinalNode == false
            && hasPendingChildren()
            && nextFilters.size() == nextFiltersSizeBefore;
    }

    private boolean shouldDeferSuffixMatch(List<FilterPath> nextFilters, int nextFiltersSizeBefore, boolean deferIncompleteMatches) {
        return deferIncompleteMatches
            && deferSuffixAcrossObjects
            && isFinalNode == false
            && pattern.isEmpty() == false
            && hasPendingChildren()
            && nextFilters.size() == nextFiltersSizeBefore;
    }

    private boolean hasPendingChildren() {
        return termsChildren.isEmpty() == false || wildcardChildren.length > 0;
    }

    private static class FilterPathBuilder {

        private static class BuildNode {
            private final Map<String, BuildNode> children;
            private final boolean isFinalNode;

            BuildNode(boolean isFinalNode) {
                children = new HashMap<>();
                this.isFinalNode = isFinalNode;
            }
        }

        private final BuildNode root = new BuildNode(false);

        void insert(String filter) {
            insertNode(filter, root, 0);
        }

        FilterPath build() {
            return buildPath("", root, false);
        }

        static void insertNode(String filter, BuildNode node, int depth) {
            if (depth > MAX_TREE_DEPTH) {
                throw new IllegalArgumentException(
                    "Filter exceeds maximum depth at [" + (filter.length() > 100 ? filter.substring(0, 100) : filter) + "]"
                );
            }
            int end = filter.length();
            int splitPosition = -1;
            boolean findEscapes = false;
            for (int i = 0; i < end; i++) {
                char c = filter.charAt(i);
                if (c == '.') {
                    splitPosition = i;
                    break;
                } else if ((c == '\\') && (i + 1 < end) && (filter.charAt(i + 1) == '.')) {
                    ++i;
                    findEscapes = true;
                }
            }

            if (splitPosition > 0) {
                String field = findEscapes ? filter.substring(0, splitPosition).replace("\\.", ".") : filter.substring(0, splitPosition);
                BuildNode child = node.children.computeIfAbsent(field, f -> new BuildNode(false));
                if (false == child.isFinalNode) {
                    insertNode(filter.substring(splitPosition + 1), child, depth + 1);
                }
            } else {
                String field = findEscapes ? filter.replace("\\.", ".") : filter;
                node.children.put(field, new BuildNode(true));
            }
        }

        static FilterPath buildPath(String segment, BuildNode node, boolean deferSuffixAcrossObjects) {
            Map<String, FilterPath> termsChildren = new HashMap<>();
            List<FilterPath> wildcardChildren = new ArrayList<>();
            for (Map.Entry<String, BuildNode> entry : node.children.entrySet()) {
                String childName = entry.getKey();
                BuildNode childNode = entry.getValue();
                boolean childDeferSuffixAcrossObjects = deferSuffixAcrossObjects;
                if (WILDCARD.equals(childName)) {
                    childDeferSuffixAcrossObjects = true;
                } else if (DOUBLE_WILDCARD.equals(childName)) {
                    childDeferSuffixAcrossObjects = false;
                }
                FilterPath childFilterPath = buildPath(childName, childNode, childDeferSuffixAcrossObjects);
                if (childName.contains(WILDCARD)) {
                    wildcardChildren.add(childFilterPath);
                } else {
                    termsChildren.put(childName, childFilterPath);
                }
            }
            return new FilterPath(
                segment,
                node.isFinalNode,
                termsChildren,
                wildcardChildren.toArray(new FilterPath[0]),
                deferSuffixAcrossObjects
            );
        }
    }

    public static FilterPath[] compile(Set<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        FilterPathBuilder builder = new FilterPathBuilder();
        for (String filter : filters) {
            if (filter != null) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    builder.insert(filter);
                }
            }
        }
        FilterPath filterPath = builder.build();
        return Collections.singletonList(filterPath).toArray(new FilterPath[0]);
    }
}
