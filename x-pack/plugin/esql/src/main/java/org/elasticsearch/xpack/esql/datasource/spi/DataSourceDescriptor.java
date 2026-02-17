/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Map;

/**
 * Descriptor for an external data source reference.
 *
 * <p>Two mutually exclusive forms:
 * <ul>
 *   <li>{@link Specified} — type, configuration, and settings specified directly
 *       (e.g., {@code SOURCE s3 "logs/*.parquet" WITH {...}})</li>
 *   <li>{@link Registered} — references a named data source whose type, configuration,
 *       and settings are looked up from the registry at resolution time</li>
 * </ul>
 *
 * <p>Both forms carry an {@link #expression()} that is data source-specific and opaque
 * to ES|QL (file pattern, table name, SQL query, etc.).
 */
public sealed interface DataSourceDescriptor permits DataSourceDescriptor.Specified, DataSourceDescriptor.Registered {

    /**
     * The expression to resolve (table, pattern, query) — opaque to ES|QL.
     */
    String expression();

    /**
     * Source location in the query for error reporting.
     */
    Source source();

    /**
     * Human-readable description for error messages.
     */
    String describe();

    /**
     * Fully specified data source with type, configuration, and settings provided directly.
     *
     * @param type The data source type (e.g., "s3", "postgres", "iceberg")
     * @param configuration DataSource-specific configuration (connection, auth) — opaque to ES
     * @param settings ES-controlled settings for this data source
     * @param expression The expression to resolve (table, pattern, query) — opaque to ES
     * @param source Source location for error reporting
     */
    record Specified(String type, Map<String, Object> configuration, Map<String, Object> settings, String expression, Source source)
        implements
            DataSourceDescriptor {

        @Override
        public String describe() {
            return "SOURCE(" + type + "):" + expression;
        }

        /**
         * Get a configuration value with a default.
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
         */
        @SuppressWarnings("unchecked")
        public <T> T setting(String key, T defaultValue) {
            Object value = settings.get(key);
            if (value == null) {
                return defaultValue;
            }
            return (T) value;
        }
    }

    /**
     * Reference to a named data source whose configuration is stored in the registry.
     *
     * <p>At resolution time, the pre-analyzer looks up the registration by name to obtain
     * the type, configuration, and settings, then passes them to the {@link DataSource}.
     *
     * @param name The registered data source name (e.g., "s3_logs", "my_postgres")
     * @param expression The expression to resolve (table, pattern, query) — opaque to ES
     * @param source Source location for error reporting
     */
    record Registered(String name, String expression, Source source) implements DataSourceDescriptor {

        @Override
        public String describe() {
            return name + ":" + expression;
        }
    }
}
