/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

// a IS NULL AND a IS NOT NULL -> FALSE
// a IS NULL AND a > 10 -> a IS NULL AND null > 10 (FoldNull folds to null in the next pass)
// (a IS NOT NULL OR p) AND a IS NULL -> OR(false, p) AND a IS NULL (BooleanSimplification then yields p AND a IS NULL)
// (a IS NULL OR p) AND a IS NOT NULL -> OR(false, p) AND a IS NOT NULL (BooleanSimplification then yields p AND a IS NOT NULL)
public class PropagateNullable extends OptimizerRules.OptimizerExpressionRule<And> {

    public PropagateNullable() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public Expression rule(And and, LogicalOptimizerContext ctx) {
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

    protected Expression nonNullify(Expression exp, Expression nonNullExp) {
        return substituteNullability(exp, nonNullExp, false);
    }

    protected Expression nullify(Expression exp, Expression nullExp) {
        Expression base = exp;
        if (exp instanceof Coalesce) {
            // Remove direct COALESCE arguments that are exactly the null field; the remaining
            // arguments may still reference the field and are handled by substituteNullability below.
            List<Expression> newChildren = new ArrayList<>(exp.children());
            newChildren.removeIf(e -> e.semanticEquals(nullExp));
            if (newChildren.size() != exp.children().size() && newChildren.size() > 0) { // coalesce needs at least one input
                base = exp.replaceChildren(newChildren);
            }
        }
        return substituteNullability(base, nullExp, true);
    }

    /**
     * Substitutes {@code IS NULL} and {@code IS NOT NULL} predicates — and, when the field is known to be {@code NULL},
     * direct field references — throughout {@code exp}.
     * <p>
     * When {@code isNullResult} is {@code true} (field constrained to {@code IS NULL}):
     * direct field occurrences become {@code null}, {@code IS NULL(field)} becomes {@code true},
     * {@code IS NOT NULL(field)} becomes {@code false}.
     * <p>
     * When {@code isNullResult} is {@code false} (field constrained to {@code IS NOT NULL}):
     * only the predicate forms are substituted ({@code IS NULL} → {@code false},
     * {@code IS NOT NULL} → {@code true}); the field value itself is left intact because its
     * concrete value is unknown.
     * <p>
     * This preserves surviving OR branches instead of discarding them. For example:
     * {@code (a IS NOT NULL OR p) AND a IS NULL} → {@code OR(false, p) AND a IS NULL},
     * which {@code BooleanSimplification} then reduces to {@code p AND a IS NULL}.
     */
    private static Expression substituteNullability(Expression exp, Expression field, boolean isNullResult) {
        return exp.transformDown(e -> {
            if (isNullResult && e.semanticEquals(field)) {
                return Literal.of(e, null);
            }
            if (e instanceof IsNull isNull && isNull.field().semanticEquals(field)) {
                return Literal.of(e, isNullResult);
            }
            if (e instanceof IsNotNull isNotNull && isNotNull.field().semanticEquals(field)) {
                return Literal.of(e, isNullResult == false);
            }
            return e;
        });
    }
}
