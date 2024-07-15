/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

// a IS NULL AND a IS NOT NULL -> FALSE
// a IS NULL AND a > 10 -> a IS NULL and FALSE
// can be extended to handle null conditions where available
public class PropagateNullable extends OptimizerRules.OptimizerExpressionRule<And> {

    public PropagateNullable() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public Expression rule(And and) {
        List<Expression> splits = Predicates.splitAnd(and);

        Set<Expression> nullExpressions = new LinkedHashSet<>();
        Set<Expression> notNullExpressions = new LinkedHashSet<>();
        List<Expression> others = new LinkedList<>();

        // first find isNull/isNotNull
        for (Expression ex : splits) {
            if (ex instanceof IsNull isn) {
                nullExpressions.add(isn.field());
            } else if (ex instanceof IsNotNull isnn) {
                notNullExpressions.add(isnn.field());
            }
            // the rest
            else {
                others.add(ex);
            }
        }

        // check for is isNull and isNotNull --> FALSE
        if (Sets.haveNonEmptyIntersection(nullExpressions, notNullExpressions)) {
            return Literal.of(and, Boolean.FALSE);
        }

        // apply nullability across relevant/matching expressions

        // first against all nullable expressions
        // followed by all not-nullable expressions
        boolean modified = replace(nullExpressions, others, splits, this::nullify);
        modified |= replace(notNullExpressions, others, splits, this::nonNullify);
        if (modified) {
            // reconstruct the expression
            return Predicates.combineAnd(splits);
        }
        return and;
    }

    /**
     * Replace the given 'pattern' expressions against the target expression.
     * If a match is found, the matching expression will be replaced by the replacer result
     * or removed if null is returned.
     */
    private static boolean replace(
        Iterable<Expression> pattern,
        List<Expression> target,
        List<Expression> originalExpressions,
        BiFunction<Expression, Expression, Expression> replacer
    ) {
        boolean modified = false;
        for (Expression s : pattern) {
            for (int i = 0; i < target.size(); i++) {
                Expression t = target.get(i);
                // identify matching expressions
                if (t.anyMatch(s::semanticEquals)) {
                    Expression replacement = replacer.apply(t, s);
                    // if the expression has changed, replace it
                    if (replacement != t) {
                        modified = true;
                        target.set(i, replacement);
                        originalExpressions.replaceAll(e -> t.semanticEquals(e) ? replacement : e);
                    }
                }
            }
        }
        return modified;
    }

    // placeholder for non-null
    protected Expression nonNullify(Expression exp, Expression nonNullExp) {
        return exp;
    }

    protected Expression nullify(Expression exp, Expression nullExp) {
        if (exp instanceof Coalesce) {
            List<Expression> newChildren = new ArrayList<>(exp.children());
            newChildren.removeIf(e -> e.semanticEquals(nullExp));
            if (newChildren.size() != exp.children().size() && newChildren.size() > 0) { // coalesce needs at least one input
                return exp.replaceChildren(newChildren);
            }
        }
        return Literal.of(exp, null);
    }
}
