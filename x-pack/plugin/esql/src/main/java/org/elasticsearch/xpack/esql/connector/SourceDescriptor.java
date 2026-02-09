/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Map;

/**
 * Descriptor for an external data source reference.
 *
 * <p>The syntax is {@code where:what}, where {@code where} defines the data source
 * and {@code what} defines the query/expression.
 *
 * <h2>Data Source ({@code where})</h2>
 *
 * <p>Either a registered name or an inline definition:
 * <ul>
 *   <li>Registered: {@code s3_logs}, {@code my_postgres}</li>
 *   <li>Inline: {@code EXTERNAL({"type": "s3", "configuration": {...}, "settings": {...}})}</li>
 * </ul>
 *
 * <p>The JSON inside {@code EXTERNAL()} is a raw JSON literal with required structure:
 * <pre>
 * {"type": "...", "configuration": {...}, "settings": {...}}
 * </pre>
 *
 * <h2>Expression ({@code what})</h2>
 *
 * <p>Three forms, all connector-specific and opaque to ES|QL:
 * <ul>
 *   <li><b>Unquoted shorthand</b> (simple identifiers): {@code s3_logs:logs}, {@code my_postgres:users}</li>
 *   <li><b>Quoted string</b> (patterns, SQL, special chars): {@code s3_logs:"logs/*.parquet"}</li>
 *   <li><b>Query function</b> (equivalent to quoted): {@code my_postgres:query("SELECT * FROM users")}</li>
 * </ul>
 *
 * @param type The connector type (e.g., "s3", "postgres", "iceberg")
 * @param configuration Connector-specific configuration (connection, auth) - opaque to ES
 * @param settings ES-controlled settings for this data source
 * @param expression The expression to resolve (table, pattern, query) - opaque to ES
 * @param dataSourceName The registered data source name, or null if inline
 * @param source Source location for error reporting
 */
public record SourceDescriptor(
    String type,
    Map<String, Object> configuration,
    Map<String, Object> settings,
    String expression,
    String dataSourceName,
    Source source
) {

    /**
     * Create a source descriptor from a registered data source.
     *
     * <p>Example: {@code FROM s3_logs:logs} or {@code FROM s3_logs:"logs/*.parquet"}
     *
     * @param dataSourceName The registered data source name (e.g., "s3_logs")
     * @param type The connector type from the registration
     * @param configuration The configuration from the registration
     * @param settings The settings from the registration
     * @param expression The expression after {@code :} (e.g., "logs", "logs/*.parquet")
     * @param source Source location for error reporting
     */
    public static SourceDescriptor registered(
        String dataSourceName,
        String type,
        Map<String, Object> configuration,
        Map<String, Object> settings,
        String expression,
        Source source
    ) {
        return new SourceDescriptor(type, configuration, settings, expression, dataSourceName, source);
    }

    /**
     * Create a source descriptor from an inline definition.
     *
     * <p>Example: {@code FROM EXTERNAL({"type": "s3", "configuration": {...}, "settings": {...}}):logs}
     *
     * @param type The connector type (e.g., "s3")
     * @param configuration The inline configuration
     * @param settings The inline settings
     * @param expression The expression after {@code :} (e.g., "logs", "logs/*.parquet")
     * @param source Source location for error reporting
     */
    public static SourceDescriptor inline(
        String type,
        Map<String, Object> configuration,
        Map<String, Object> settings,
        String expression,
        Source source
    ) {
        return new SourceDescriptor(type, configuration, settings, expression, null, source);
    }

    /**
     * Whether this source was defined inline (vs registered).
     */
    public boolean isInline() {
        return dataSourceName == null;
    }

    /**
     * Whether this source references a registered data source.
     */
    public boolean isRegistered() {
        return dataSourceName != null;
    }

    /**
     * Get a configuration value with a default.
     *
     * @param key The config key
     * @param defaultValue Default value if not present
     * @param <T> The expected type
     * @return The config value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T config(String key, T defaultValue) {
        Object value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        return (T) value;
    }

    /**
     * Get a required configuration value.
     *
     * @param key The config key
     * @param <T> The expected type
     * @return The config value
     * @throws IllegalArgumentException if the key is not present
     */
    @SuppressWarnings("unchecked")
    public <T> T requireConfig(String key) {
        Object value = configuration.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing required configuration: " + key);
        }
        return (T) value;
    }

    /**
     * Get a setting value with a default.
     *
     * @param key The setting key
     * @param defaultValue Default value if not present
     * @param <T> The expected type
     * @return The setting value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T setting(String key, T defaultValue) {
        Object value = settings.get(key);
        if (value == null) {
            return defaultValue;
        }
        return (T) value;
    }

    /**
     * Human-readable description for error messages.
     */
    public String describe() {
        if (isRegistered()) {
            return dataSourceName + ":" + expression;
        } else {
            return "EXTERNAL(" + type + "):" + expression;
        }
    }
}
