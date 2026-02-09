/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import org.elasticsearch.xpack.esql.connector.ConnectorPlan;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Base class for data lake connector plan nodes.
 *
 * <p>Data lake connectors typically support:
 * <ul>
 *   <li>Filter pushdown (partition pruning, predicate pushdown to format)</li>
 *   <li>Limit pushdown</li>
 *   <li>Column projection</li>
 * </ul>
 *
 * <p>They typically do NOT support ORDER BY or aggregation pushdown since
 * file formats don't guarantee sorted output or support server-side computation.
 *
 * <p>All methods that modify state return new instances (immutable pattern).
 *
 * @see DataLakeConnector
 */
public abstract class DataLakePlan extends ConnectorPlan {

    protected DataLakePlan(Source source) {
        super(source);
    }

    /**
     * The filter expression to apply, or null if none.
     */
    public abstract Expression filter();

    /**
     * The limit to apply, or null if none.
     */
    public abstract Integer limit();

    /**
     * Whether a filter is present.
     */
    public boolean hasFilter() {
        return filter() != null;
    }

    /**
     * Whether a limit is present.
     */
    public boolean hasLimit() {
        return limit() != null;
    }

    /**
     * Create a copy with the given filter.
     */
    public abstract DataLakePlan withFilter(Expression filter);

    /**
     * Create a copy with the given limit.
     */
    public abstract DataLakePlan withLimit(Integer limit);
}
