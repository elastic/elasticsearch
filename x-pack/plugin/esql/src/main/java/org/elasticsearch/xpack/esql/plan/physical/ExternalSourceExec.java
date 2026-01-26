/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * Abstract base class for physical plan nodes that read from external data sources
 * (e.g., Iceberg tables, Parquet files).
 * <p>
 * This mirrors the logical layer design where {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation}
 * is the base class for external source logical plan nodes.
 * <p>
 * Subclasses should implement source-specific functionality:
 * <ul>
 *   <li>{@link IcebergSourceExec} - for Iceberg tables and Parquet files</li>
 * </ul>
 */
public abstract class ExternalSourceExec extends LeafExec implements EstimatesRowSize {

    private final String sourcePath;
    private final List<Attribute> attributes;
    private final Integer estimatedRowSize;

    /**
     * Creates an ExternalSourceExec.
     *
     * @param source the source location in the query
     * @param sourcePath the path or identifier of the external source (e.g., S3 URI, local file path)
     * @param attributes the schema attributes
     * @param estimatedRowSize the estimated size of each row in bytes (nullable, set by optimizer)
     */
    protected ExternalSourceExec(Source source, String sourcePath, List<Attribute> attributes, Integer estimatedRowSize) {
        super(source);
        this.sourcePath = Objects.requireNonNull(sourcePath, "sourcePath must not be null");
        this.attributes = Objects.requireNonNull(attributes, "attributes must not be null");
        this.estimatedRowSize = estimatedRowSize;
    }

    /**
     * @return the path or identifier of the external source (e.g., S3 URI, local file path)
     */
    public String sourcePath() {
        return sourcePath;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    /**
     * @return the estimated size of each row in bytes, or null if not yet estimated
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    /**
     * @return a string identifying the type of external source (e.g., "iceberg", "parquet")
     */
    public abstract String sourceType();

    @Override
    public PhysicalPlan estimateRowSize(EstimatesRowSize.State state) {
        int size = state.consumeAllFields(false);
        state.add(false, attributes);
        return Objects.equals(this.estimatedRowSize, size) ? this : withEstimatedRowSize(size);
    }

    /**
     * Create a new instance with the given estimated row size.
     * Used by the EstimatesRowSize optimizer.
     *
     * @param newEstimatedRowSize the new estimated row size
     * @return a new instance with the updated estimated row size
     */
    protected abstract ExternalSourceExec withEstimatedRowSize(Integer newEstimatedRowSize);

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, attributes, estimatedRowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExternalSourceExec other = (ExternalSourceExec) obj;
        return Objects.equals(sourcePath, other.sourcePath)
            && Objects.equals(attributes, other.attributes)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }
}
