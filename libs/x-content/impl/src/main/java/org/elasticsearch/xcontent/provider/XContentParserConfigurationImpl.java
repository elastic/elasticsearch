/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import java.util.Set;

public class XContentParserConfigurationImpl implements XContentParserConfiguration {

    public static final XContentParserConfigurationImpl EMPTY = new XContentParserConfigurationImpl(
        NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        RestApiVersion.current(),
        null,
        null,
        false,
        null
    );

    final NamedXContentRegistry registry;
    final DeprecationHandler deprecationHandler;
    final RestApiVersion restApiVersion;
    final Integer minClusterTransportVersion;
    final FilterPath[] includes;
    final FilterPath[] excludes;
    final boolean filtersMatchFieldNamesWithDots;

    private XContentParserConfigurationImpl(
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        RestApiVersion restApiVersion,
        FilterPath[] includes,
        FilterPath[] excludes,
        boolean filtersMatchFieldNamesWithDots,
        Integer minClusterTransportVersion
    ) {
        this.registry = registry;
        this.deprecationHandler = deprecationHandler;
        this.restApiVersion = restApiVersion;
        this.includes = includes;
        this.excludes = excludes;
        this.filtersMatchFieldNamesWithDots = filtersMatchFieldNamesWithDots;
        this.minClusterTransportVersion = minClusterTransportVersion;
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
            minClusterTransportVersion
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
            minClusterTransportVersion
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
            minClusterTransportVersion
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
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            FilterPath.compile(includeStrings),
            FilterPath.compile(excludeStrings),
            filtersMatchFieldNamesWithDots,
            minClusterTransportVersion
        );
    }

    @Override
    public XContentParserConfiguration withMinClusterTransportVersion(int minClusterTransportVersion) {
        return new XContentParserConfigurationImpl(
            registry,
            deprecationHandler,
            restApiVersion,
            includes,
            excludes,
            filtersMatchFieldNamesWithDots,
            minClusterTransportVersion
        );
    }

    @Override
    public Integer getMinClusterTransportVersion() {
        return minClusterTransportVersion;
    }

    public JsonParser filter(JsonParser parser) {
        JsonParser filtered = parser;
        if (excludes != null) {
            filtered = new FilteringParserDelegate(
                filtered,
                new FilterPathBasedFilter(excludes, false, filtersMatchFieldNamesWithDots),
                true,
                true
            );
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
