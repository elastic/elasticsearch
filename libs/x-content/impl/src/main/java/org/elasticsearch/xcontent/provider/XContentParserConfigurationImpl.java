/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
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
        true
    );

    final NamedXContentRegistry registry;
    final DeprecationHandler deprecationHandler;
    final RestApiVersion restApiVersion;
    final FilterPath[] includes;
    final FilterPath[] excludes;
    final boolean filtersMatchFieldNamesWithDots;
    final boolean includeSourceOnError;

    private XContentParserConfigurationImpl(
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        RestApiVersion restApiVersion,
        FilterPath[] includes,
        FilterPath[] excludes,
        boolean filtersMatchFieldNamesWithDots,
        boolean includeSourceOnError
    ) {
        this.registry = registry;
        this.deprecationHandler = deprecationHandler;
        this.restApiVersion = restApiVersion;
        this.includes = includes;
        this.excludes = excludes;
        this.filtersMatchFieldNamesWithDots = filtersMatchFieldNamesWithDots;
        this.includeSourceOnError = includeSourceOnError;
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
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includes,
            excludes,
            filtersMatchFieldNamesWithDots,
            includeSourceOnError
        );
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
            includeSourceOnError
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
            includeSourceOnError
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
            includeSourceOnError
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
        FilterPath[] includePaths = FilterPath.compile(includeStrings);
        FilterPath[] excludePaths = FilterPath.compile(excludeStrings);

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
            includeSourceOnError
        );
    }

    /**
     * The paths to include, or {@code null} if no include filtering is configured.
     */
    public FilterPath[] includes() {
        return includes;
    }

    /**
     * The paths to exclude, or {@code null} if no exclude filtering is configured.
     */
    public FilterPath[] excludes() {
        return excludes;
    }

    /**
     * Whether a dot in a filter path should be matched against a field name containing a dot, rather than
     * only being treated as a separator between path segments.
     */
    public boolean filtersMatchFieldNamesWithDots() {
        return filtersMatchFieldNamesWithDots;
    }
}
