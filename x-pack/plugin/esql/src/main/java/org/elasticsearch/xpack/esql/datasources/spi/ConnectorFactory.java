/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Map;

/**
 * Factory for creating connectors to external data sources.
 * Each factory handles a specific protocol or source type (e.g. "flight", "jdbc").
 * Schema resolution lives here to avoid creating throwaway {@link Connector} instances.
 */
public interface ConnectorFactory extends ExternalSourceFactory {

    String type();

    boolean canHandle(String location);

    SourceMetadata resolveMetadata(String location, Map<String, Object> config);

    Connector open(Map<String, Object> config);
}
