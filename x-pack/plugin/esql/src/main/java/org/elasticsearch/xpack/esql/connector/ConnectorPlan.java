/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

/**
 * Base class for connector-specific logical plan leaf nodes.
 *
 * <p>Each connector defines its own subclass storing whatever state it needs
 * (filters, SQL fragments, file lists, etc.) as type-safe fields. Extending
 * {@link LeafPlan} is enforced at compile time through this class hierarchy.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li><b>Creation:</b> {@link Connector#resolve} creates the initial node with schema</li>
 *   <li><b>Optimization:</b> Connector-provided {@link Connector#optimizationRules() rules}
 *       fold operations (Filter, Limit, etc.) into this node via immutable copy methods</li>
 *   <li><b>Physical Planning:</b> {@code Mapper} calls {@link Connector#createPhysicalPlan}</li>
 * </ol>
 *
 * <h2>Class Hierarchy</h2>
 *
 * <pre>
 * LeafPlan
 *   └── ConnectorPlan                    (this class)
 *         ├── LakehousePlan              (adds filter, limit)
 *         │     └── IcebergPlan, etc.
 *         └── SqlPlan                    (adds filter, limit, orderBy, aggregation)
 *               └── JdbcPlan, etc.
 * </pre>
 *
 * @see Connector
 * @see org.elasticsearch.xpack.esql.connector.lakehouse.LakehousePlan
 * @see org.elasticsearch.xpack.esql.connector.sql.SqlPlan
 */
public abstract class ConnectorPlan extends LeafPlan {

    protected ConnectorPlan(Source source) {
        super(source);
    }

    /**
     * The connector that owns this plan node.
     *
     * @return the owning connector instance
     */
    public abstract Connector connector();

    /**
     * The source location (URI/path) for error messages and identification.
     *
     * @return the source location string
     */
    public abstract String location();

    /**
     * The output columns (projected schema).
     *
     * <p>This is declared abstract here as a reminder — it's already abstract in
     * {@link org.elasticsearch.xpack.esql.plan.logical.LogicalPlan}, but connector
     * subclasses must provide it.
     */
    @Override
    public abstract List<Attribute> output();
}
