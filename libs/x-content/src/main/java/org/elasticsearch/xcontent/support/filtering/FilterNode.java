/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FilterNode {
    private static final String WILDCARD = "*";
    private static final String DOUBLE_WILDCARD = "**";

    private final Map<String, FilterNode> termsFilters;
    private final Map<String, FilterNode> wildcardFilters;
    private final String filter;
    private final boolean doubleWildcard;

    public FilterNode(String filter) {
        this.termsFilters = new HashMap<>();
        this.wildcardFilters = new HashMap<>();
        this.filter = filter;
        this.doubleWildcard = DOUBLE_WILDCARD.equals(filter);
    }

    public FilterNode getTermFilter(String field) {
        return termsFilters.get(field);
    }

    public void putTermFilter(String field, FilterNode filterNode) {
        termsFilters.put(field, filterNode);
    }

    public FilterNode getWildcardFilter(String field) {
        return wildcardFilters.get(field);
    }

    public void putWildcardFilter(String field, FilterNode filterNode) {
        wildcardFilters.put(field, filterNode);
    }

    public Map<String, FilterNode> getWildcardFilters() {
        return wildcardFilters;
    }

    public boolean isEnd() {
        return termsFilters.isEmpty() && wildcardFilters.isEmpty();
    }

    public boolean isDoubleWildcard() {
        return doubleWildcard;
    }

    public boolean hasDoubleWildcard() {
        if (filter == null) {
            return false;
        }
        if(filter.indexOf("**") >= 0) {
            return true;
        }

        for (FilterNode filterNode : wildcardFilters.values()) {
            if (filterNode.hasDoubleWildcard()) {
                return true;
            }
        }

        return false;
    }

    public String getFilter() {
        return filter;
    }

    public void insert(String filter) {
        int end = filter.length();
        for (int i = 0; i < end;) {
            char c = filter.charAt(i);
            if (c == '.') {
                String field = filter.substring(0, i).replaceAll("\\\\.", ".");
                FilterNode child;
                if (field.contains(WILDCARD)) {
                    child = getWildcardFilter(field);
                    if (child == null) {
                        child = new FilterNode(field);
                        putWildcardFilter(field, child);
                    }
                } else {
                    child = getTermFilter(field);
                    if (child == null) {
                        child = new FilterNode(field);
                        putTermFilter(field, child);
                    }
                }
                child.insert(filter.substring(i + 1));
                return;
            }
            ++i;
            if ((c == '\\') && (i < end) && (filter.charAt(i) == '.')) {
                ++i;
            }
        }

        String field = filter.replaceAll("\\\\.", ".");
        if (field.contains(WILDCARD)) {
            putWildcardFilter(field, new FilterNode(field));
        } else {
            putTermFilter(field, new FilterNode(field));
        }
    }

    public static FilterNode[] compile(Set<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }

        FilterNode filterNode = new FilterNode("");
        for (String filter : filters) {
            if (filter != null) {
                filter = filter.trim();
                if (filter.length() > 0) {
                    filterNode.insert(filter);
                }
            }
        }

        return Collections.singletonList(filterNode).toArray(new FilterNode[0]);
    }
}
