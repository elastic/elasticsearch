/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code EVAL} and {@code RENAME} bindings under a plan, keyed by the attribute id they define, so a column can be
 * followed back to whatever produced it.
 * <p>
 * {@code FORK} copies the commands above it into every branch and binds those same ids again, each to that branch's
 * own copy. A map built over the whole plan can follow a column into a different branch. Build it over the branch
 * when you need that branch's copy.
 */
public final class AliasBindings {

    private final Map<NameId, Expression> bindings;

    private AliasBindings(Map<NameId, Expression> bindings) {
        this.bindings = bindings;
    }

    public static AliasBindings of(LogicalPlan plan) {
        Map<NameId, Expression> bindings = new HashMap<>();
        plan.forEachDown(p -> {
            if (p instanceof Eval eval) {
                for (Alias alias : eval.fields()) {
                    bindings.put(alias.id(), alias.child());
                }
            } else if (p instanceof Project project) {
                for (NamedExpression projection : project.projections()) {
                    if (projection instanceof Alias alias) {
                        bindings.put(alias.id(), alias.child());
                    }
                }
            }
        });
        return new AliasBindings(bindings);
    }

    /** Follows bindings from {@code expression} until it isn't a bound attribute anymore. */
    public Expression resolve(Expression expression) {
        Expression current = expression;
        // No binding points at its own key, so this finishes within the size of the map.
        for (int hops = bindings.size(); hops > 0 && current instanceof Attribute attribute; hops--) {
            Expression bound = bindings.get(attribute.id());
            if (bound == null) {
                break;
            }
            current = bound;
        }
        return current;
    }
}
