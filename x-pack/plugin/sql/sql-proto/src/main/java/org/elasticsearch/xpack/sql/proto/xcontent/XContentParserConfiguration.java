/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.xpack.sql.proto.core.RestApiVersion;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
 *
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
        RestApiVersion.current()
    );

    final NamedXContentRegistry registry;
    final DeprecationHandler deprecationHandler;
    final RestApiVersion restApiVersion;

    private XContentParserConfiguration(
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        RestApiVersion restApiVersion
    ) {
        this.registry = registry;
        this.deprecationHandler = deprecationHandler;
        this.restApiVersion = restApiVersion;
    }

    /**
     * Replace the registry backing {@link XContentParser#namedObject}.
     */
    public XContentParserConfiguration withRegistry(NamedXContentRegistry registry) {
        return new XContentParserConfiguration(registry, deprecationHandler, restApiVersion);
    }

    public NamedXContentRegistry registry() {
        return registry;
    }

    /**
     * Replace the behavior of {@link XContentParser} when it encounters
     * a deprecated field.
     */
    public XContentParserConfiguration withDeprecationHandler(DeprecationHandler deprecationHandler) {
        return new XContentParserConfiguration(registry, deprecationHandler, restApiVersion);
    }

    public DeprecationHandler deprecationHandler() {
        return deprecationHandler;
    }

    /**
     * Replace the {@link XContentParser#getRestApiVersion() claimed}
     * {@link RestApiVersion}.
     */
    public XContentParserConfiguration withRestApiVersion(RestApiVersion restApiVersion) {
        return new XContentParserConfiguration(registry, deprecationHandler, restApiVersion);
    }

    public RestApiVersion restApiVersion() {
        return restApiVersion;
    }

    public JsonParser filter(JsonParser parser) {
        return parser;
    }
}
