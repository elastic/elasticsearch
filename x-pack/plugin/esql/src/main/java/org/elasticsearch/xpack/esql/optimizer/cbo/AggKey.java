/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;

import java.util.List;

/** Identifies an aggregation across its logical and physical representations. */
public record AggKey(List<NameId> ids) {
    public AggKey {
        ids = List.copyOf(ids);
    }

    /** Builds a key shared by logical and physical aggregation representations. */
    public static AggKey of(List<? extends NamedExpression> aggregates) {
        return new AggKey(Aggregate.output(aggregates).stream().map(Attribute::id).toList());
    }
}
