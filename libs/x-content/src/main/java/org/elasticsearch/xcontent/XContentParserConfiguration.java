/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.util.Set;

/**
 * Configuration for {@link XContentParser}.
 */
public interface XContentParserConfiguration {
    /**
     * Creates parsers that don't support {@link XContentParser#namedObject},
     * throw an exception when they see deprecated fields, that return the
     * {@link RestApiVersion#current() current version} from
     * {@link XContentParser#getRestApiVersion}, and do no filtering.
     */
    XContentParserConfiguration EMPTY = XContentProvider.provider().empty();

    /**
     * Replace the registry backing {@link XContentParser#namedObject}.
     */
    XContentParserConfiguration withRegistry(NamedXContentRegistry registry);

    NamedXContentRegistry registry();

    /**
     * Replace the behavior of {@link XContentParser} when it encounters
     * a deprecated field.
     */
    XContentParserConfiguration withDeprecationHandler(DeprecationHandler deprecationHandler);

    DeprecationHandler deprecationHandler();

    /**
     * Replace the {@link XContentParser#getRestApiVersion() claimed}
     * {@link RestApiVersion}.
     */
    XContentParserConfiguration withRestApiVersion(RestApiVersion restApiVersion);

    RestApiVersion restApiVersion();

    /**
     * Replace the configured filtering.
     */
    XContentParserConfiguration withFiltering(
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots
    );
}
