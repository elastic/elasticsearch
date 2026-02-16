/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lakehouse.spi;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasource.spi.DataSource;
import org.elasticsearch.xpack.esql.datasource.spi.DataSourcePlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Concrete plan node for lakehouse data sources.
 *
 * <p>Stores everything needed to execute a lakehouse query:
 * <ul>
 *   <li>Identity: owning {@link DataSource}, location, original expression</li>
 *   <li>Schema: projected output columns</li>
 *   <li>Format: format name for looking up the {@link FormatReader} at execution time</li>
 *   <li>Pushed operations: opaque native filter and limit</li>
 * </ul>
 *
 * <p>Immutable — all mutation methods ({@link #withNativeFilter}, {@link #withLimit})
 * return new instances.
 *
 * @see LakehouseDataSource
 */
public final class LakehousePlan extends DataSourcePlan {

    private final DataSource dataSource;
    private final String location;
    private final List<Attribute> output;
    private final String expression;
    private final String formatName;
    private final Object nativeFilter;
    private final Integer limit;

    public LakehousePlan(
        Source source,
        DataSource dataSource,
        String location,
        List<Attribute> output,
        String expression,
        String formatName,
        Object nativeFilter,
        Integer limit
    ) {
        super(source);
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.location = Objects.requireNonNull(location, "location");
        this.output = Objects.requireNonNull(output, "output");
        this.expression = Objects.requireNonNull(expression, "expression");
        this.formatName = Objects.requireNonNull(formatName, "formatName");
        this.nativeFilter = nativeFilter;
        this.limit = limit;
    }

    // =========================================================================
    // DataSourcePlan abstract implementations
    // =========================================================================

    @Override
    public DataSource dataSource() {
        return dataSource;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    // =========================================================================
    // Lakehouse-specific accessors
    // =========================================================================

    /**
     * The original expression from the query (path, pattern, or query string).
     */
    public String expression() {
        return expression;
    }

    /**
     * The format name for looking up the {@link FormatReader} at execution time.
     */
    public String formatName() {
        return formatName;
    }

    /**
     * The opaque source-native filter from {@link FilterPushdownSupport}, or null.
     */
    public Object nativeFilter() {
        return nativeFilter;
    }

    /**
     * The pushed limit, or null.
     */
    public Integer limit() {
        return limit;
    }

    public boolean hasFilter() {
        return nativeFilter != null;
    }

    public boolean hasLimit() {
        return limit != null;
    }

    // =========================================================================
    // Immutable copy methods
    // =========================================================================

    /**
     * Return a copy with the given native filter applied.
     */
    public LakehousePlan withNativeFilter(Object nativeFilter) {
        return new LakehousePlan(source(), dataSource, location, output, expression, formatName, nativeFilter, limit);
    }

    /**
     * Return a copy with the given limit applied.
     */
    public LakehousePlan withLimit(Integer limit) {
        return new LakehousePlan(source(), dataSource, location, output, expression, formatName, nativeFilter, limit);
    }

    // =========================================================================
    // Tree infrastructure
    // =========================================================================

    @Override
    protected NodeInfo<LakehousePlan> info() {
        return NodeInfo.create(this);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    // =========================================================================
    // Serialization (stub — Phase 2 will implement for distributed execution)
    // =========================================================================

    @Override
    public String getWriteableName() {
        return "esql.plan.lakehouse";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Serialization not yet implemented for lakehouse plans");
    }

    // =========================================================================
    // Object
    // =========================================================================

    @Override
    public int hashCode() {
        return Objects.hash(dataSource.type(), location, expression, formatName, nativeFilter, limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LakehousePlan other = (LakehousePlan) obj;
        return Objects.equals(dataSource.type(), other.dataSource.type())
            && Objects.equals(location, other.location)
            && Objects.equals(expression, other.expression)
            && Objects.equals(formatName, other.formatName)
            && Objects.equals(nativeFilter, other.nativeFilter)
            && Objects.equals(limit, other.limit);
    }
}
