/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent.api;

import org.elasticsearch.core.Nullable;

/**
 * A registry of named {@link UserAgentParser} instances.
 */
public interface UserAgentParserRegistry {

    String DEFAULT_PARSER_NAME = "_default_";

    /**
     * No-op registry that always returns {@code null}. Used as a fallback when no plugin provides a registry.
     * The processor factory handles null parsers with a clear configuration exception.
     */
    UserAgentParserRegistry NOOP = parserName -> null;

    /**
     * Returns the parser registered under the given name, or {@code null} if none.
     */
    @Nullable
    UserAgentParser getParser(String parserName);
}
