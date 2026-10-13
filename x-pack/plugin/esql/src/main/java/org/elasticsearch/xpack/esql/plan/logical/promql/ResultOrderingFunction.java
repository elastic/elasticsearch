/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

/**
 * A PromQL function whose only effect is to order the final result.
 */
public interface ResultOrderingFunction {
    /**
     * @param commandOutput {@code PromqlCommand.output()}, i.e. {@code [value, step] ++ promqlPlan.output()}
     */
    ResultOrdering resultOrdering(List<Attribute> commandOutput, Configuration configuration);

    /**
     * @param syntheticKeys aliases to place in an Eval below the OrderBy; all must be synthetic
     */
    record ResultOrdering(List<Alias> syntheticKeys, List<Order> orders) {}
}
