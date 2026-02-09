/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector;

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
 * Physical plan leaf node for connector sources.
 *
 * <p>This is the physical counterpart of a connector's logical {@link ConnectorPlan} node.
 * It carries the fully-optimized connector plan (with any pushed-down filters, limits,
 * etc.) and will be used by {@code LocalExecutionPlanner} to create the actual source
 * operator via {@link Connector#createSourceOperator}.
 *
 * <p>Shared by all connector types (SQL, data lake, etc.) — the connector-specific
 * state lives inside the {@link ConnectorPlan}.
 *
 * <h2>Serialization</h2>
 *
 * <p>For distributed connectors, this node is serialized and sent to data nodes.
 * The {@link ConnectorPlan} must be serializable (implement {@code writeTo}).
 * For coordinator-only connectors (e.g., JDBC), serialization is not needed and
 * {@code writeTo}/{@code readFrom} throw {@link UnsupportedOperationException}.
 */
public class ConnectorExec extends LeafExec {

    private final ConnectorPlan plan;
    private final List<Attribute> attributes;

    /**
     * Create a connector physical plan node.
     *
     * @param source Source location for error messages (from the logical plan node)
     * @param plan The connector plan with all pushed-down operations
     */
    public ConnectorExec(Source source, ConnectorPlan plan) {
        super(source);
        this.plan = plan;
        this.attributes = plan.output();
    }

    /**
     * The connector plan with all pushed-down operations.
     */
    public ConnectorPlan plan() {
        return plan;
    }

    /**
     * The connector that owns this plan.
     */
    public Connector connector() {
        return plan.connector();
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ConnectorExec::new, plan);
    }

    // =========================================================================
    // SERIALIZATION
    // =========================================================================

    @Override
    public String getWriteableName() {
        return "esql.plan.connector";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO: implement for distributed connectors
        // Source.EMPTY.writeTo(out);
        // out.writeString(plan.connector().type());
        // plan.writeTo(out); // ConnectorPlan extends LeafPlan which is NamedWriteable
        throw new UnsupportedOperationException("Serialization not yet implemented for connector physical plans");
    }

    // =========================================================================
    // NODE
    // =========================================================================

    @Override
    public String nodeString(NodeStringFormat format) {
        return nodeName() + "[" + plan.connector().type() + ":" + plan.location() + "]" + NodeUtils.toString(attributes, format);
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
        ConnectorExec other = (ConnectorExec) obj;
        return Objects.equals(plan, other.plan);
    }
}
