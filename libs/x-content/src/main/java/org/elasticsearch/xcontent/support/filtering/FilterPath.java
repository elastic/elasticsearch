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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FilterPath {
    private static final String WILDCARD = "*";
    private static final String DOUBLE_WILDCARD = "**";

    private final Map<String, FilterPath> termsChildren;
    private final Map<String, FilterPath> wildcardChildren;
    private final boolean doubleWildcard;
    private final boolean isFinalNode;
    private boolean hasDoubleWildcard;

    public FilterPath(boolean doubleWildcard, boolean isFinalNode) {
        this.termsChildren = new HashMap<>();
        this.wildcardChildren = new HashMap<>();
        this.doubleWildcard = doubleWildcard;
        this.isFinalNode = isFinalNode;
    }

    public boolean hasDoubleWildcard() {
        if (hasDoubleWildcard) {
            return true;
        }

        for (FilterPath filterPath : wildcardChildren.values()) {
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

    public void insert(String filter) {
        int end = filter.length();
        for (int i = 0; i < end;) {
            char c = filter.charAt(i);
            if (c == '.') {
                String field = filter.substring(0, i).replaceAll("\\\\.", ".");
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
                    child.insert(filter.substring(i + 1));
                }
                return;
            }
            ++i;
            if ((c == '\\') && (i < end) && (filter.charAt(i) == '.')) {
                ++i;
            }
        }

        String field = filter.replaceAll("\\\\.", ".");
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

    public boolean matches(String name, List<FilterPath> nextFilters) {
        FilterPath termNode = termsChildren.get(name);
        if (termNode != null) {
            if (termNode.isFinalNode()) {
                return true;
            } else {
                nextFilters.add(termNode);
            }
        }

        for (String wildcardPattern : wildcardChildren.keySet()) {
            if (Glob.globMatch(wildcardPattern, name)) {
                FilterPath wildcardNode = wildcardChildren.get(wildcardPattern);
                if (wildcardNode.isFinalNode()) {
                    return true;
                } else {
                    nextFilters.add(wildcardNode);
                }
            }
        }

        if (doubleWildcard) {
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

        FilterPath filterPath = new FilterPath(false, false);
        for (String filter : filters) {
            if (filter != null) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    filterPath.insert(filter);
                }
            }
        }

        return Collections.singletonList(filterPath).toArray(new FilterPath[0]);
    }
}
