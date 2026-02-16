/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan leaf node for data source sources.
 *
 * <p>This is the physical counterpart of a data source's logical {@link DataSourcePlan} node.
 * It carries the fully-optimized data source plan (with any pushed-down filters, limits,
 * etc.) and will be used by {@code LocalExecutionPlanner} to create the actual source
 * operator via {@link DataSource#createSourceOperator}.
 *
 * <p>Shared by all data source types (SQL, data lake, etc.) — the data source-specific
 * state lives inside the {@link DataSourcePlan}.
 *
 * <h2>Serialization</h2>
 *
 * <p>For distributed data sources, this node is serialized and sent to data nodes.
 * The {@link DataSourcePlan} must be serializable (implement {@code writeTo}).
 * For coordinator-only data sources (e.g., JDBC), serialization is not needed and
 * {@code writeTo}/{@code readFrom} throw {@link UnsupportedOperationException}.
 */
public class DataSourceExec extends LeafExec {

    private final DataSourcePlan plan;
    private final List<Attribute> attributes;

    /**
     * Create a data source physical plan node.
     *
     * @param source Source location for error messages (from the logical plan node)
     * @param plan The data source plan with all pushed-down operations
     */
    public DataSourceExec(Source source, DataSourcePlan plan) {
        super(source);
        this.plan = plan;
        this.attributes = plan.output();
    }

    /**
     * The data source plan with all pushed-down operations.
     */
    public DataSourcePlan plan() {
        return plan;
    }

    /**
     * The data source that owns this plan.
     */
    public DataSource dataSource() {
        return plan.dataSource();
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, DataSourceExec::new, plan);
    }

    // =========================================================================
    // SERIALIZATION
    // =========================================================================

    @Override
    public String getWriteableName() {
        return "esql.plan.datasource";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: implement for distributed data sources
        // Source.EMPTY.writeTo(out);
        // out.writeString(plan.dataSource().type());
        // plan.writeTo(out); // DataSourcePlan extends LeafPlan which is NamedWriteable
        throw new UnsupportedOperationException("Serialization not yet implemented for data source physical plans");
    }

    // =========================================================================
    // NODE
    // =========================================================================

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + plan.dataSource().type() + ":" + plan.location() + "]" + NodeUtils.toString(attributes, format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plan);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DataSourceExec other = (DataSourceExec) obj;
        return Objects.equals(plan, other.plan);
    }
}
