/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.provider.filtering.FilterPathBasedFilter;
import org.elasticsearch.xcontent.support.filtering.FilterPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class XContentParserConfigurationImpl implements XContentParserConfiguration {

    public static final XContentParserConfigurationImpl EMPTY = new XContentParserConfigurationImpl(
        NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        RestApiVersion.current(),
        null,
        null,
        false,
        true,
        false,
        false,
        false
    );

    final NamedXContentRegistry registry;
    final DeprecationHandler deprecationHandler;
    final RestApiVersion restApiVersion;
    final FilterPath[] includes;
    final FilterPath[] excludes;
    final boolean filtersMatchFieldNamesWithDots;
    final boolean includeSourceOnError;
    final boolean sourceFilterSemantics;
    final boolean wildcardExcludes;
    final boolean dottedPathExcludes;

    private XContentParserConfigurationImpl(
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        RestApiVersion restApiVersion,
        FilterPath[] includes,
        FilterPath[] excludes,
        boolean filtersMatchFieldNamesWithDots,
        boolean includeSourceOnError,
        boolean sourceFilterSemantics,
        boolean wildcardExcludes,
        boolean dottedPathExcludes
    ) {
        this.registry = registry;
        this.deprecationHandler = deprecationHandler;
        this.restApiVersion = restApiVersion;
        this.includes = includes;
        this.excludes = excludes;
        this.filtersMatchFieldNamesWithDots = filtersMatchFieldNamesWithDots;
        this.includeSourceOnError = includeSourceOnError;
        this.sourceFilterSemantics = sourceFilterSemantics;
        this.wildcardExcludes = wildcardExcludes;
        this.dottedPathExcludes = dottedPathExcludes;
    }

    private XContentParserConfigurationImpl copy(boolean includeSourceOnError) {
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includes,
            excludes,
            filtersMatchFieldNamesWithDots,
            includeSourceOnError,
            sourceFilterSemantics,
            wildcardExcludes,
            dottedPathExcludes
        );
    }

    @Override
    public boolean includeSourceOnError() {
        return includeSourceOnError;
    }

    @Override
    public XContentParserConfiguration withIncludeSourceOnError(boolean includeSourceOnError) {
        if (includeSourceOnError == this.includeSourceOnError) {
            return this;
        }
        return copy(includeSourceOnError);
    }

    @Override
    public XContentParserConfigurationImpl withRegistry(NamedXContentRegistry registry) {
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includes,
            excludes,
            filtersMatchFieldNamesWithDots,
            includeSourceOnError,
            sourceFilterSemantics,
            wildcardExcludes,
            dottedPathExcludes
        );
    }

    public NamedXContentRegistry registry() {
        return registry;
    }

    public XContentParserConfiguration withDeprecationHandler(DeprecationHandler deprecationHandler) {
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includes,
            excludes,
            filtersMatchFieldNamesWithDots,
            includeSourceOnError,
            sourceFilterSemantics,
            wildcardExcludes,
            dottedPathExcludes
        );
    }

    public DeprecationHandler deprecationHandler() {
        return deprecationHandler;
    }

    public XContentParserConfiguration withRestApiVersion(RestApiVersion restApiVersion) {
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includes,
            excludes,
            filtersMatchFieldNamesWithDots,
            includeSourceOnError,
            sourceFilterSemantics,
            wildcardExcludes,
            dottedPathExcludes
        );
    }

    public RestApiVersion restApiVersion() {
        return restApiVersion;
    }

    public XContentParserConfiguration withFiltering(
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots
    ) {
        return withFiltering(null, includeStrings, excludeStrings, filtersMatchFieldNamesWithDots);
    }

    public XContentParserConfiguration withFiltering(
        String prefixPath,
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots
    ) {
        return buildFilteringConfiguration(prefixPath, includeStrings, excludeStrings, filtersMatchFieldNamesWithDots, false);
    }

    @Override
    public XContentParserConfiguration withSourceFilterFiltering(
        String prefixPath,
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots
    ) {
        return buildFilteringConfiguration(prefixPath, includeStrings, excludeStrings, filtersMatchFieldNamesWithDots, true);
    }

    private XContentParserConfiguration buildFilteringConfiguration(
        String prefixPath,
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots,
        boolean sourceFilterSemantics
    ) {
        FilterPath[] includePaths = FilterPath.compile(includeStrings);
        FilterPath[] excludePaths = FilterPath.compile(excludeStrings);
        boolean wildcardExcludes = false;
        boolean dottedPathExcludes = false;
        if (sourceFilterSemantics && excludeStrings != null) {
            for (String exclude : excludeStrings) {
                if (exclude != null && exclude.contains("*")) {
                    wildcardExcludes = true;
                }
                if (exclude != null && exclude.contains(".")) {
                    dottedPathExcludes = true;
                }
            }
        }

        if (prefixPath != null) {
            if (includePaths != null) {
                List<FilterPath> includeFilters = new ArrayList<>();
                for (var incl : includePaths) {
                    incl.matches(prefixPath, includeFilters, true);
                }
                includePaths = includeFilters.isEmpty() ? null : includeFilters.toArray(FilterPath[]::new);
            }

            if (excludePaths != null) {
                List<FilterPath> excludeFilters = new ArrayList<>();
                for (var excl : excludePaths) {
                    excl.matches(prefixPath, excludeFilters, true);
                }
                excludePaths = excludeFilters.isEmpty() ? null : excludeFilters.toArray(FilterPath[]::new);
            }
        }
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includePaths,
            excludePaths,
            filtersMatchFieldNamesWithDots,
            includeSourceOnError,
            sourceFilterSemantics,
            wildcardExcludes,
            dottedPathExcludes
        );
    }

    public JsonParser filter(JsonParser parser) {
        JsonParser filtered = parser;
        if (excludes != null) {
            FilterPathBasedFilter excludeFilter;
            if (sourceFilterSemantics) {
                if (includes != null) {
                    excludeFilter = FilterPathBasedFilter.createSourceFilterExclusiveFilterWithIncludes(
                        excludes,
                        filtersMatchFieldNamesWithDots
                    );
                } else if (wildcardExcludes) {
                    excludeFilter = FilterPathBasedFilter.createSourceFilterExclusiveFilterExcludeOnlyWildcard(
                        excludes,
                        filtersMatchFieldNamesWithDots
                    );
                } else {
                    excludeFilter = FilterPathBasedFilter.createSourceFilterExclusiveFilterExcludeOnlyLiteral(
                        excludes,
                        filtersMatchFieldNamesWithDots,
                        dottedPathExcludes
                    );
                }
            } else {
                excludeFilter = new FilterPathBasedFilter(excludes, false, filtersMatchFieldNamesWithDots);
            }
            filtered = new FilteringParserDelegate(filtered, excludeFilter, true, true);
        }
        if (includes != null) {
            filtered = new FilteringParserDelegate(
                filtered,
                new FilterPathBasedFilter(includes, true, filtersMatchFieldNamesWithDots),
                true,
                true
            );
        }
        return filtered;
    }
}
