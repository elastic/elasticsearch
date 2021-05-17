/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

/**
 * Deprecation log messages are categorised so that consumers of the logs can easily aggregate them.
 * <p>
 * When categorising a message, you should consider the impact of the work required to mitigate the
 * deprecation. For example, a settings change would normally be categorised as {@link #SETTINGS},
 * but if the setting in question was related to security configuration, it may be more appropriate
 * to categorise the deprecation message as {@link #SECURITY}.
 */
public enum DeprecationCategory {
    AGGREGATIONS,
    ANALYSIS,
    API,
    COMPATIBLE_API,
    INDICES,
    MAPPINGS,
    OTHER,
    PARSING,
    PLUGINS,
    QUERIES,
    SCRIPTING,
    SECURITY,
    SETTINGS,
    TEMPLATES
}
