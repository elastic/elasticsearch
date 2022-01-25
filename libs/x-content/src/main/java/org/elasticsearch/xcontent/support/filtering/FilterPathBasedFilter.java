/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.support.filtering;

import com.fasterxml.jackson.core.filter.TokenFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FilterPathBasedFilter extends TokenFilter {

    /**
     * Marker value that should be used to indicate that a property name
     * or value matches one of the filter paths.
     */
    private static final FilterPathBasedFilter MATCHING = new FilterPathBasedFilter() {
        @Override
        public String toString() {
            return "MATCHING";
        }
    };

    /**
     * Marker value that should be used to indicate that none of the
     * property names/values matches one of the filter paths.
     */
    private static final FilterPathBasedFilter NO_MATCHING = new FilterPathBasedFilter() {
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
        this.filters = filters;
        this.inclusive = inclusive;
        this.matchFieldNamesWithDots = matchFieldNamesWithDots;
    }

    private FilterPathBasedFilter() {
        this.inclusive = true;
        this.filters = null;
        this.matchFieldNamesWithDots = false;
    }

    public FilterPathBasedFilter(Set<String> filters, boolean inclusive) {
        this(FilterPath.compile(filters), inclusive, false);
    }

    /**
     * Evaluates if a property name matches one of the given filter paths.
     */
    private FilterPathBasedFilter evaluate(String name) {
        assert matchFieldNamesWithDots == false || name.indexOf('.') < 0;

        if (filters != null) {
            List<FilterPath> nextFilters = new ArrayList<>();
            for (FilterPath filter : filters) {
                boolean matches = filter.matches(name, nextFilters);
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
        FilterPathBasedFilter filter = this;
        if (matchFieldNamesWithDots) {
            int dot = name.indexOf('.');
            while (dot > 0) {
                String first = name.substring(0, dot);
                filter = filter.evaluate(first);
                if (filter == MATCHING) {
                    return inclusive ? TokenFilter.INCLUDE_ALL : null;
                }
                if (filter == NO_MATCHING) {
                    return inclusive ? null : TokenFilter.INCLUDE_ALL;
                }
                name = name.substring(dot + 1);
                dot = name.indexOf('.');
            }
        }
        filter = filter.evaluate(name);
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
