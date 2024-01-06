/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    private final Map<String, FilterPath> termsChildren;
    private final FilterPath[] wildcardChildren;
    private final String pattern;
    private final boolean isDoubleWildcard;
    private final boolean isFinalNode;

    private FilterPath(String pattern, boolean isFinalNode, Map<String, FilterPath> termsChildren, FilterPath[] wildcardChildren) {
        this.pattern = pattern;
        this.isFinalNode = isFinalNode;
        this.termsChildren = Collections.unmodifiableMap(termsChildren);
        this.wildcardChildren = wildcardChildren;
        this.isDoubleWildcard = pattern.equals(DOUBLE_WILDCARD);
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
    public boolean matches(String name, List<FilterPath> nextFilters, boolean matchFieldNamesWithDots) { // << here
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
        }

        return false;
    }

    private boolean matchFieldNamesWithDots(String name, int dotIndex, List<FilterPath> nextFilters) {
        String prefixName = name.substring(0, dotIndex);
        String suffixName = name.substring(dotIndex + 1);
        List<FilterPath> prefixFilterPath = new ArrayList<>();
        boolean prefixMatch = matches(prefixName, prefixFilterPath, true);
        // if prefixMatch return true(because prefix is a final FilterPath node)
        if (prefixMatch) {
            return true;
        }
        // if has prefixNextFilter, use them to match suffix
        for (FilterPath filter : prefixFilterPath) {
            boolean matches = filter.matches(suffixName, nextFilters, true);
            if (matches) {
                return true;
            }
        }
        return false;
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
            insertNode(filter, root);
        }

        FilterPath build() {
            return buildPath("", root);
        }

        static void insertNode(String filter, BuildNode node) {
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
                String field = findEscapes
                    ? filter.substring(0, splitPosition).replaceAll("\\\\.", ".")
                    : filter.substring(0, splitPosition);
                BuildNode child = node.children.get(field);
                if (child == null) {
                    child = new BuildNode(false);
                    node.children.put(field, child);
                }
                if (false == child.isFinalNode) {
                    insertNode(filter.substring(splitPosition + 1), child);
                }
            } else {
                String field = findEscapes ? filter.replaceAll("\\\\.", ".") : filter;
                node.children.put(field, new BuildNode(true));
            }
        }

        static FilterPath buildPath(String segment, BuildNode node) {
            Map<String, FilterPath> termsChildren = new HashMap<>();
            List<FilterPath> wildcardChildren = new ArrayList<>();
            for (Map.Entry<String, BuildNode> entry : node.children.entrySet()) {
                String childName = entry.getKey();
                BuildNode childNode = entry.getValue();
                FilterPath childFilterPath = buildPath(childName, childNode);
                if (childName.contains(WILDCARD)) {
                    wildcardChildren.add(childFilterPath);
                } else {
                    termsChildren.put(childName, childFilterPath);
                }
            }
            return new FilterPath(segment, node.isFinalNode, termsChildren, wildcardChildren.toArray(new FilterPath[0]));
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
