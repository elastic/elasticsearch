/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
     *  Disable to not include the source in case of parsing errors (defaults to true).
     */
    XContentParserConfiguration withIncludeSourceOnError(boolean includeSourceOnError);

    boolean includeSourceOnError();

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

    // TODO: Remove when serverless uses the new API
    XContentParserConfiguration withFiltering(
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots
    );

    /**
     * Replace the configured filtering.
     *
     * @param prefixPath                    The path to be prepended to each sub-path before applying the include/exclude rules.
     *                                      Specify {@code null} if parsing starts from the root.
     * @param includeStrings                A set of strings representing paths to include during filtering.
     *                                      If specified, only these paths will be included in parsing.
     * @param excludeStrings                A set of strings representing paths to exclude during filtering.
     *                                      If specified, these paths will be excluded from parsing.
     * @param filtersMatchFieldNamesWithDots Indicates whether filters should match field names containing dots ('.')
     *                                      as part of the field name.
     */
    XContentParserConfiguration withFiltering(
        String prefixPath,
        Set<String> includeStrings,
        Set<String> excludeStrings,
        boolean filtersMatchFieldNamesWithDots
    );
}
