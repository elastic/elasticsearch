/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;

/**
 * Follows {@code EVAL} and {@code RENAME} copies of a column back to the expression that produced it.
 */
public final class AliasBindings {

    private AliasBindings() {}

    /**
     * {@code EVAL} and {@code RENAME} bindings under {@code plan}, keyed by the attribute each one defines. Call
     * {@code resolve(column, column)} on the result to follow {@code column} back to what produced it.
     * <p>
     * {@code FORK} copies the commands above it into every branch, and each copy binds the same ids again. A map built
     * over the whole plan can therefore follow a column into a different branch. To follow a branch's own copy, build
     * the map over that branch.
     */
    public static AttributeMap<Expression> of(LogicalPlan plan) {
        AttributeMap.Builder<Expression> bindings = AttributeMap.builder();
        plan.forEachDown(p -> {
            if (p instanceof Eval eval) {
                eval.fields().forEach(alias -> bindings.put(alias.toAttribute(), alias.child()));
            } else if (p instanceof Project project) {
                for (NamedExpression projection : project.projections()) {
                    if (projection instanceof Alias alias) {
                        bindings.put(alias.toAttribute(), alias.child());
                    }
                }
            }
        });
        return bindings.build();
    }
}
