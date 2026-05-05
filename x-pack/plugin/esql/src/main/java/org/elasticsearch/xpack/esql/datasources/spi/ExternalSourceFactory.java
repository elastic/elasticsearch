/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Common interface for complete external data source factories.
 * Both API-based connectors (Flight, JDBC) and table-based catalogs (Iceberg)
 * implement this interface, enabling unified resolution and dispatch.
 *
 * Building-block factories (StorageProviderFactory, FormatReaderFactory) are NOT
 * part of this hierarchy — they are composed by the framework for file-based sources.
 */
public interface ExternalSourceFactory {

    String type();

    boolean canHandle(String location);

    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    /**
     * Reject WITH-clause keys this factory does not recognize for the given location, throwing
     * {@link IllegalArgumentException} naming the unknown keys plus the recognised options.
     * <p>
     * Implementations compose claimed-key sets across their own sub-components (storage backend,
     * format reader, catalog, connection, …) and call
     * {@link WithClauseValidator#check(Map, java.util.Collection)} with the union. The default is
     * a no-op so providers that don't yet implement validation keep working — silently accepting
     * any key as today.
     * <p>
     * Called once at planning time, before {@link #resolveMetadata}; idempotent against repeated
     * invocations on the same {@code (location, config)} pair.
     */
    default void validateConfig(String location, Map<String, Object> config) {}

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }

    default SplitProvider splitProvider() {
        return SplitProvider.SINGLE;
    }
}
