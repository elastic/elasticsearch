/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.util.Map;

/**
 * Factory for creating connectors to external data sources.
 * Each factory handles a specific protocol or source type (e.g. "flight", "jdbc").
 * Schema resolution lives here to avoid creating throwaway {@link Connector} instances.
 */
public interface ConnectorFactory extends ExternalSourceFactory {

    String type();

    boolean canHandle(String location);

    /**
     * @param config configuration map. Values are typed as {@code Object}: secret values may arrive
     * as {@link org.elasticsearch.common.settings.SecureString}, non-secrets retain their underlying
     * type ({@code String}, {@code Integer}, {@code Long}, {@code Boolean}). Use
     * {@code Objects.toString(config.get(key), null)} or pattern-match on type — casting directly
     * to {@code String} will throw {@link ClassCastException} on non-String values.
     */
    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    /**
     * @param config configuration map; see {@link #resolveMetadata(String, Map)} for typing rules.
     */
    Connector open(Map<String, Object> config);

    /**
     * Tests connectivity by opening and immediately closing a {@link Connector}. Connector implementations
     * that can perform a lighter-weight probe (e.g. a ping without allocating a full session) may override this.
     *
     * <p>Close failures are suppressed: if {@link #open} succeeds the connection was established, and a
     * subsequent teardown error does not indicate a connectivity problem.
     */
    @Override
    default void testConnection(Map<String, Object> config) throws IOException {
        Connector c = open(config);
        if (c == null) {
            throw new IllegalStateException("open() returned null — connector contract violation");
        }
        try {
            c.close();
        } catch (IOException | RuntimeException ignored) {}
    }
}
