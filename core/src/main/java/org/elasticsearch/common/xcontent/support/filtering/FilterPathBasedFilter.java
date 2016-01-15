/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent.support.filtering;

import com.fasterxml.jackson.core.filter.TokenFilter;
import org.elasticsearch.common.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class FilterPathBasedFilter extends TokenFilter {

    /**
     * Marker value that should be used to indicate that a property name
     * or value matches one of the filter paths.
     */
    private static final TokenFilter MATCHING = new TokenFilter() {
    };

    /**
     * Marker value that should be used to indicate that none of the
     * property names/values matches one of the filter paths.
     */
    private static final TokenFilter NO_MATCHING = new TokenFilter() {
    };

    private final FilterPath[] filters;

    private final boolean inclusive;

    public FilterPathBasedFilter(boolean inclusive, FilterPath[] filters) {
        this.inclusive = inclusive;
        if (CollectionUtils.isEmpty(filters)) {
            throw new IllegalArgumentException("filters cannot be null or empty");
        }
        this.filters = filters;
    }

    public FilterPathBasedFilter(boolean inclusive, String[] filters) {
        this(inclusive, FilterPath.compile(filters));
    }

    /**
     * Evaluates if a property name matches one of the given filter paths.
     */
    private TokenFilter evaluate(String name, FilterPath[] filters) {
        if (filters != null) {
            List<FilterPath> nextFilters = null;

            for (FilterPath filter : filters) {
                FilterPath next = filter.matchProperty(name);
                if (next != null) {
                    if (next.matches()) {
                        return MATCHING;
                    } else {
                        if (nextFilters == null) {
                            nextFilters = new ArrayList<>();
                        }
                        if (filter.isDoubleWildcard()) {
                            nextFilters.add(filter);
                        }
                        nextFilters.add(next);
                    }
                }
            }

            if ((nextFilters != null) && (nextFilters.isEmpty() == false)) {
                return new FilterPathBasedFilter(inclusive, nextFilters.toArray(new FilterPath[nextFilters.size()]));
            }
        }
        return NO_MATCHING;
    }


    @Override
    public TokenFilter includeProperty(String name) {
        TokenFilter filter = evaluate(name, filters);
        if (inclusive) {
            if (filter == MATCHING) {
                return TokenFilter.INCLUDE_ALL;
            } else if (filter == NO_MATCHING) {
                return null;
            }
        } else {
            if (filter == MATCHING) {
                return null;
            } else if (filter == NO_MATCHING) {
                return TokenFilter.INCLUDE_ALL;
            }
        }
        return filter;
    }

    @Override
    protected boolean _includeScalar() {
        for (FilterPath filter : filters) {
            if (filter.matches()) {
                return inclusive;
            }
        }
        return !inclusive;
    }
}
