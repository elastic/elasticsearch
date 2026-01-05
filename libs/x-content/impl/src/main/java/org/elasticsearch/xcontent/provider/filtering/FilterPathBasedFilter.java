/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

    /**
     * This is overridden in order to keep empty arrays in nested exclusions - see #109668.
     * <p>
     * If we are excluding contents, we only want to exclude based on property name - but empty arrays in themselves do not have a property
     * name. If the empty array were to be excluded, it should be done by excluding the parent.
     * <p>
     * Note though that the expected behavior seems to be ambiguous if contentsFiltered is true - that is, that the filter has pruned all
     * the contents of a given array, such that we are left with the empty array. The behavior below drops that array, for at the time of
     * writing, not doing so would cause assertions in JsonXContentFilteringTests to fail, which expect this behavior. Yet it is not obvious
     * if dropping the empty array in this case is correct. For example, one could expect this sort of behavior:
     * <ul>
     *     <li>Document: <pre>{ "myArray": [ { "myField": "myValue" } ]}</pre></li>
     *     <li>Filter: <pre>{ "exclude": "myArray.myField" }</pre></li>
     * </ul>
     * From the user's perspective, this could reasonably yield either of:
     * <ol>
     *     <li><pre>{ "myArray": []}</pre></li>
     *     <li>Removing {@code myArray} entirely.</li>
     * </ol>
     */
    @Override
    public boolean includeEmptyArray(boolean contentsFiltered) {
        return inclusive == false && contentsFiltered == false;
    }

    /**
     * This is overridden in order to keep empty objects in nested exclusions - see #109668.
     * <p>
     * The same logic applies to this as to {@link #includeEmptyArray(boolean)}, only for nested objects instead of nested arrays.
     */
    @Override
    public boolean includeEmptyObject(boolean contentsFiltered) {
        return inclusive == false && contentsFiltered == false;
    }

    @Override
    protected boolean _includeScalar() {
        return inclusive == false;
    }
}
