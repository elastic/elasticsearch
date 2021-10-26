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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.core.Glob;

public class FilterNode {
    private static final String WILDCARD = "*";
    private static final String DOUBLE_WILDCARD = "**";

    private final Map<String, FilterNode> termsFilters;
    private final Map<String, FilterNode> wildcardFilters;
    private final boolean doubleWildcard;
    private boolean hasDoubleWildcard;

    public FilterNode(String field) {
        this.termsFilters = new HashMap<>();
        this.wildcardFilters = new HashMap<>();
        this.doubleWildcard = DOUBLE_WILDCARD.equals(field);
    }

    public boolean hasDoubleWildcard() {
        return hasDoubleWildcard;
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
                FilterNode child;
                if (field.contains(WILDCARD)) {
                    child = wildcardFilters.get(field);
                    if (child == null) {
                        child = new FilterNode(field);
                        wildcardFilters.put(field, child);
                    }
                } else {
                    child = termsFilters.get(field);
                    if (child == null) {
                        child = new FilterNode(field);
                        termsFilters.put(field, child);
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
        if (field.contains(DOUBLE_WILDCARD)) {
            hasDoubleWildcard = true;
        }
        if (field.contains(WILDCARD)) {
            wildcardFilters.put(field, new FilterNode(field));
        } else {
            termsFilters.put(field, new FilterNode(field));
        }
    }

    public boolean matches(String name, List<FilterNode> nextFilters) {
        if (doubleWildcard) {
            nextFilters.add(this);
        }

        FilterNode termNode = termsFilters.get(name);
        if (termNode != null) {
            if (termNode.isEnd()) {
                return true;
            } else {
                nextFilters.add(termNode);
            }
        }

        for (Map.Entry<String, FilterNode> entry : wildcardFilters.entrySet()) {
            String wildcardPattern = entry.getKey();
            FilterNode wildcardNode = entry.getValue();
            if (Glob.globMatch(wildcardPattern, name)) {
                if (wildcardNode.isEnd()) {
                    return true;
                } else {
                    nextFilters.add(wildcardNode);
                }
            }
        }

        return false;
    }

    private boolean isEnd() {
        return termsFilters.isEmpty() && wildcardFilters.isEmpty();
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
