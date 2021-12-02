/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.filter.FilteringParserDelegate;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.support.filtering.FilterPath;
import org.elasticsearch.xcontent.support.filtering.FilterPathBasedFilter;

import java.util.Set;

/**
 * Configuration for {@link XContentParser}.
 */
public class XContentParserConfiguration {
    /**
     * Creates parsers that don't support {@link XContentParser#namedObject},
     * throw an exception when they see deprecated fields, that return the
     * {@link RestApiVersion#current() current version} from
     * {@link XContentParser#getRestApiVersion}, and do no filtering.
     */
    public static final XContentParserConfiguration EMPTY = new XContentParserConfiguration(
        NamedXContentRegistry.EMPTY,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        RestApiVersion.current(),
        null,
        null
    );

    final NamedXContentRegistry registry;
    final DeprecationHandler deprecationHandler;
    final RestApiVersion restApiVersion;
    final FilterPath[] includes;
    final FilterPath[] excludes;

    private XContentParserConfiguration(
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        RestApiVersion restApiVersion,
        FilterPath[] includes,
        FilterPath[] excludes
    ) {
        this.registry = registry;
        this.deprecationHandler = deprecationHandler;
        this.restApiVersion = restApiVersion;
        this.includes = includes;
        this.excludes = excludes;
    }

    /**
     * Replace the registry backing {@link XContentParser#namedObject}.
     */
    public XContentParserConfiguration withRegistry(NamedXContentRegistry registry) {
        return new XContentParserConfiguration(registry, deprecationHandler, restApiVersion, includes, excludes);
    }

    public NamedXContentRegistry registry() {
        return registry;
    }

    /**
     * Replace the behavior of {@link XContentParser} when it encounters
     * a deprecated field.
     */
    public XContentParserConfiguration withDeprecationHandler(DeprecationHandler deprecationHandler) {
        return new XContentParserConfiguration(registry, deprecationHandler, restApiVersion, includes, excludes);
    }

    public DeprecationHandler deprecationHandler() {
        return deprecationHandler;
    }

    /**
     * Replace the {@link XContentParser#getRestApiVersion() claimed}
     * {@link RestApiVersion}.
     */
    public XContentParserConfiguration withRestApiVersion(RestApiVersion restApiVersion) {
        return new XContentParserConfiguration(registry, deprecationHandler, restApiVersion, includes, excludes);
    }

    public RestApiVersion restApiVersion() {
        return restApiVersion;
    }

    /**
     * Replace the configured filtering.
     */
    public XContentParserConfiguration withFiltering(Set<String> includeStrings, Set<String> excludeStrings) {
        return new XContentParserConfiguration(
            registry,
            deprecationHandler,
            restApiVersion,
            FilterPath.compile(includeStrings),
            FilterPath.compile(excludeStrings)
        );
    }

    public JsonParser filter(JsonParser parser) {
        JsonParser filtered = parser;
        if (excludes != null) {
            for (FilterPath e : excludes) {
                if (e.hasDoubleWildcard()) {
                    // Fixed in Jackson 2.13 - https://github.com/FasterXML/jackson-core/issues/700
                    throw new UnsupportedOperationException("double wildcards are not supported in filtered excludes");
                }
            }
            filtered = new FilteringParserDelegate(filtered, new FilterPathBasedFilter(excludes, false), true, true);
        }
        if (includes != null) {
            filtered = new FilteringParserDelegate(filtered, new FilterPathBasedFilter(includes, true), true, true);
        }
        return filtered;
    }
}
