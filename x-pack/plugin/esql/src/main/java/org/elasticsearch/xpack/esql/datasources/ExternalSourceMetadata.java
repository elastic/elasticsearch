/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;

import java.util.List;

/**
 * Extended interface for external data source metadata.
 * <p>
 * This interface extends {@link SourceMetadata} to provide a unified metadata type
 * for all external sources (Iceberg tables, Parquet files, etc.) while maintaining
 * backward compatibility with existing code that uses the legacy method names.
 * <p>
 * New implementations should prefer using {@link SourceMetadata} methods directly:
 * <ul>
 *   <li>{@link #location()} instead of {@link #tablePath()}</li>
 *   <li>{@link #schema()} instead of {@link #attributes()}</li>
 * </ul>
 * <p>
 * For table-based sources (Iceberg, Delta Lake), implementations should store
 * native schema and source-specific data in {@link #sourceMetadata()} to avoid
 * re-resolving the table during execution.
 */
public interface ExternalSourceMetadata extends SourceMetadata {

    /**
     * Returns the path or identifier of the external source (e.g., S3 path).
     *
     * @return the source path
     * @deprecated Use {@link #location()} instead
     */
    @Deprecated
    default String tablePath() {
        return location();
    }

    /**
     * Returns the list of attributes representing the schema of the external source.
     *
     * @return list of attributes
     * @deprecated Use {@link #schema()} instead
     */
    @Deprecated
    default List<Attribute> attributes() {
        return schema();
    }

    /**
     * Default implementation of {@link SourceMetadata#location()} that delegates
     * to {@link #tablePath()} for backward compatibility.
     * <p>
     * Implementations should override either this method or {@link #tablePath()}.
     */
    @Override
    default String location() {
        // This will be overridden by implementations
        throw new UnsupportedOperationException("Implementation must override either location() or tablePath()");
    }

    /**
     * Default implementation of {@link SourceMetadata#schema()} that delegates
     * to {@link #attributes()} for backward compatibility.
     * <p>
     * Implementations should override either this method or {@link #attributes()}.
     */
    @Override
    default List<Attribute> schema() {
        // This will be overridden by implementations
        throw new UnsupportedOperationException("Implementation must override either schema() or attributes()");
    }
}
