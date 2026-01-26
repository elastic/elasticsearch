/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

/**
 * Base interface for external data source metadata.
 * Represents resolved metadata from external sources like Iceberg tables or Parquet files.
 */
public interface ExternalSourceMetadata {

    /**
     * @return The path or identifier of the external source (e.g., S3 path)
     */
    String tablePath();

    /**
     * @return List of attributes representing the schema of the external source
     */
    List<Attribute> attributes();

    /**
     * @return The type of external source (e.g., "iceberg", "parquet")
     */
    String sourceType();
}
