/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitOr;

/**
 * Combine disjunctions on the same field into an In expression.
 * This rule looks for both simple equalities:
 * 1. a == 1 OR a == 2 becomes a IN (1, 2)
 * and combinations of In
 * 2. a == 1 OR a IN (2) becomes a IN (1, 2)
 * 3. a IN (1) OR a IN (2) becomes a IN (1, 2)
 * <p>
 * This rule does NOT check for type compatibility as that phase has been
 * already be verified in the analyzer.
 */
public final class CombineDisjunctionsToIn extends OptimizerRules.OptimizerExpressionRule<Or> {
    public CombineDisjunctionsToIn() {
        super(OptimizerRules.TransformDirection.UP);
    }

    protected In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
        return new In(key.source(), key, values);
    }

    protected Equals createEquals(Expression k, Set<Expression> v, ZoneId finalZoneId) {
        return new Equals(k.source(), k, v.iterator().next(), finalZoneId);
    }

    @Override
    public Expression rule(Or or) {
        Expression e = or;
        // look only at equals and In
        List<Expression> exps = splitOr(e);

        Map<Expression, Set<Expression>> found = new LinkedHashMap<>();
        ZoneId zoneId = null;
        List<Expression> ors = new LinkedList<>();

        for (Expression exp : exps) {
            if (exp instanceof Equals eq) {
                // consider only equals against foldables
                if (eq.right().foldable()) {
                    found.computeIfAbsent(eq.left(), k -> new LinkedHashSet<>()).add(eq.right());
                } else {
                    ors.add(exp);
                }
                if (zoneId == null) {
                    zoneId = eq.zoneId();
                }
            } else if (exp instanceof In in) {
                found.computeIfAbsent(in.value(), k -> new LinkedHashSet<>()).addAll(in.list());
            } else {
                ors.add(exp);
            }
        }

        if (found.isEmpty() == false) {
            // combine equals alongside the existing ors
            final ZoneId finalZoneId = zoneId;
            found.forEach(
                (k, v) -> { ors.add(v.size() == 1 ? createEquals(k, v, finalZoneId) : createIn(k, new ArrayList<>(v), finalZoneId)); }
            );

            // TODO: this makes a QL `or`, not an ESQL `or`
            Expression combineOr = combineOr(ors);
            // check the result semantically since the result might different in order
            // but be actually the same which can trigger a loop
            // e.g. a == 1 OR a == 2 OR null --> null OR a in (1,2) --> literalsOnTheRight --> cycle
            if (e.semanticEquals(combineOr) == false) {
                e = combineOr;
            }
        }

        return e;
    }
}
