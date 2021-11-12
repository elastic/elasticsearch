/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import org.elasticsearch.core.Glob;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterPath {
    private static final String WILDCARD = "*";
    private static final String DOUBLE_WILDCARD = "**";

    private final Map<String, FilterPath> termsChildren;
    private final Map<String, FilterPath> wildcardChildren;
    private final boolean isDoubleWildcard;
    private final boolean isFinalNode;
    private boolean hasDoubleWildcard;

    private FilterPath(boolean isDoubleWildcard, boolean isFinalNode) {
        this.termsChildren = new HashMap<>();
        this.wildcardChildren = new LinkedHashMap<>();
        this.isDoubleWildcard = isDoubleWildcard;
        this.isFinalNode = isFinalNode;
    }

    public boolean hasDoubleWildcard() {
        return hasDoubleWildcard;
    }

    private void insert(String filter) {
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
            String field = filter.substring(0, splitPosition);
            if (field.contains(DOUBLE_WILDCARD)) {
                hasDoubleWildcard = true;
            }
            FilterPath child;
            if (field.contains(WILDCARD)) {
                child = wildcardChildren.get(field);
                if (child == null) {
                    if (field.equals(DOUBLE_WILDCARD)) {
                        child = new FilterPath(true, false);
                    } else {
                        child = new FilterPath(false, false);
                    }
                    wildcardChildren.put(field, child);
                }
            } else {
                child = termsChildren.get(field);
                if (child == null) {
                    child = new FilterPath(false, false);
                    termsChildren.put(field, child);
                }
            }
            if (false == child.isFinalNode()) {
                child.insert(filter.substring(splitPosition + 1));
                if (child.hasDoubleWildcard) {
                    hasDoubleWildcard = true;
                }
            }
        } else {
            String field = findEscapes ? filter.replaceAll("\\\\.", ".") : filter;
            if (field.contains(DOUBLE_WILDCARD)) {
                hasDoubleWildcard = true;
            }
            if (field.equals(DOUBLE_WILDCARD)) {
                wildcardChildren.put(field, new FilterPath(true, true));
            } else if (field.contains(WILDCARD)) {
                wildcardChildren.put(field, new FilterPath(false, true));
            } else {
                termsChildren.put(field, new FilterPath(false, true));
            }
        }
    }

    /**
     * check if the name matches filter nodes
     * if the name equals the filter node name, the node will add to nextFilters.
     * if the filter node is a final node, it means the name matches the pattern, and return true
     * if the name don't equal a final node, then return false, continue to check the inner filter node
     * if current node is a double wildcard node, the node will also add to nextFilters.
     * @param name the xcontent property name
     * @param nextFilters nextFilters is a List, used to check the inner property of name
     * @return true if the name equal a final node, otherwise return false
     */
    boolean matches(String name, List<FilterPath> nextFilters) {
        if (nextFilters == null) {
            return false;
        }

        FilterPath termNode = termsChildren.get(name);
        if (termNode != null) {
            if (termNode.isFinalNode()) {
                return true;
            } else {
                nextFilters.add(termNode);
            }
        }

        for (Map.Entry<String, FilterPath> entry : wildcardChildren.entrySet()) {
            String wildcardPattern = entry.getKey();
            if (Glob.globMatch(wildcardPattern, name)) {
                FilterPath wildcardNode = entry.getValue();
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

    private boolean isFinalNode() {
        return isFinalNode;
    }

    public static FilterPath[] compile(Set<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        FilterPath FilterPath = new FilterPath(false, false);
        for (String filter : filters) {
            if (filter != null) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    FilterPath.insert(filter);
                }
            }
        }

        return Collections.singletonList(FilterPath).toArray(new FilterPath[0]);
    }
}
