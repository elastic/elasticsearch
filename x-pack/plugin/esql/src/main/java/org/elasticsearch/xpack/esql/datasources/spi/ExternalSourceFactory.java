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

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default SourceOperatorFactoryProvider operatorFactory() {
        return null;
    }
}
