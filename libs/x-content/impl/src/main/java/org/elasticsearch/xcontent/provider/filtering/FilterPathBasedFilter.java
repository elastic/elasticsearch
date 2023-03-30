/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider.filtering;

import com.fasterxml.jackson.core.filter.TokenFilter;

import org.elasticsearch.xcontent.support.filtering.FilterPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FilterPathBasedFilter extends TokenFilter {

    /**
     * Marker value that should be used to indicate that a property name
     * or value matches one of the filter paths.
     */
    private static final TokenFilter MATCHING = new TokenFilter() {
        @Override
        public String toString() {
            return "MATCHING";
        }
    };

    /**
     * Marker value that should be used to indicate that none of the
     * property names/values matches one of the filter paths.
     */
    private static final TokenFilter NO_MATCHING = new TokenFilter() {
        @Override
        public String toString() {
            return "NO_MATCHING";
        }
    };

    private final FilterPath[] filters;

    private final boolean inclusive;

    private final boolean matchFieldNamesWithDots;

    public FilterPathBasedFilter(FilterPath[] filters, boolean inclusive, boolean matchFieldNamesWithDots) {
        if (filters == null || filters.length == 0) {
            throw new IllegalArgumentException("filters cannot be null or empty");
        }
        this.inclusive = inclusive;
        this.filters = filters;
        this.matchFieldNamesWithDots = matchFieldNamesWithDots;
    }

    public FilterPathBasedFilter(Set<String> filters, boolean inclusive) {
        this(FilterPath.compile(filters), inclusive, false);
    }

    /**
     * Evaluates if a property name matches one of the given filter paths.
     */
    private TokenFilter evaluate(String name, FilterPath[] filterPaths) {
        if (filterPaths != null) {
            List<FilterPath> nextFilters = new ArrayList<>();
            for (FilterPath filter : filterPaths) {
                boolean matches = filter.matches(name, nextFilters, matchFieldNamesWithDots);
                if (matches) {
                    return MATCHING;
                }
            }

            if (nextFilters.isEmpty() == false) {
                return new FilterPathBasedFilter(
                    nextFilters.toArray(new FilterPath[nextFilters.size()]),
                    inclusive,
                    matchFieldNamesWithDots
                );
            }
        }
        return NO_MATCHING;
    }

    @Override
    public TokenFilter includeProperty(String name) {
        TokenFilter filter = evaluate(name, filters);
        if (filter == MATCHING) {
            return inclusive ? TokenFilter.INCLUDE_ALL : null;
        }
        if (filter == NO_MATCHING) {
            return inclusive ? null : TokenFilter.INCLUDE_ALL;
        }
        return filter;
    }

    @Override
    protected boolean _includeScalar() {
        return inclusive == false;
    }
}
