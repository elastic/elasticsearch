/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * Abstract base class for external data source relations (e.g., Iceberg table, Parquet file).
 * This plan node is executed on the coordinator only (no dispatch to data nodes).
 * <p>
 * Unlike EsRelation which wraps into FragmentExec for data node dispatch,
 * ExternalRelation and its subclasses map directly to physical source operators via LocalMapper,
 * similar to how LocalRelation works.
 * <p>
 * Subclasses should implement source-specific functionality:
 * <ul>
 *   <li>{@link IcebergRelation} - for Iceberg tables and Parquet files</li>
 * </ul>
 */
public abstract class ExternalRelation extends LeafPlan implements ExecutesOn.Coordinator {

    private final String sourcePath;
    private final List<Attribute> output;

    /**
     * Creates an ExternalRelation.
     *
     * @param source the source location in the query
     * @param sourcePath the path or identifier of the external source (e.g., S3 URI, local file path)
     * @param output the schema attributes
     */
    protected ExternalRelation(Source source, String sourcePath, List<Attribute> output) {
        super(source);
        this.sourcePath = Objects.requireNonNull(sourcePath, "sourcePath must not be null");
        this.output = Objects.requireNonNull(output, "output must not be null");
    }

    /**
     * @return the path or identifier of the external source (e.g., S3 URI, local file path)
     */
    public String sourcePath() {
        return sourcePath;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    /**
     * @return a string identifying the type of external source (e.g., "iceberg", "parquet")
     */
    public abstract String sourceType();

    /**
     * Creates the corresponding physical plan execution node for this external relation.
     * This is used by the mapper to convert logical plans to physical plans.
     *
     * @return the physical plan node that reads from this external source
     */
    public abstract org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec toPhysicalExec();

    @Override
    public int hashCode() {
        return Objects.hash(sourcePath, output);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExternalRelation other = (ExternalRelation) obj;
        return Objects.equals(sourcePath, other.sourcePath)
            && Objects.equals(output, other.output);
    }
}
