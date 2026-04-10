/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.capabilities.TransportVersionAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Replace {@link TransportVersionAware}s with their backwards-compatible replacements.
 */
public final class SubstituteTransportVersionAwareExpressions extends OptimizerRules.OptimizerExpressionRule<Expression> {

    public SubstituteTransportVersionAwareExpressions() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(Expression e, LogicalOptimizerContext ctx) {
        return rule(e, ctx.minimumVersion());
    }

    public static Expression rule(Expression e, TransportVersion minTransportVersion) {
        if (e instanceof TransportVersionAware tva) {
            Expression replacement = tva.forTransportVersion(minTransportVersion);
            if (replacement != null) {
                return replacement;
            }
        }
        return e;
    }
}
