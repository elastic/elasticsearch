/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class RuleUtils {

    private RuleUtils() {}

    /**
     * Returns a tuple of two lists:
     * 1. A list of aliases to null literals for those data types in the {@param outputAttributes} that {@param shouldBeReplaced}.
     * 2. A list of named expressions where attributes that match the predicate are replaced with their corresponding null alias.
     *
     * @param outputAttributes The original output attributes.
     * @param shouldBeReplaced A predicate to determine which attributes should be replaced with null aliases.
     */
    public static Tuple<List<Alias>, List<NamedExpression>> aliasedNulls(
        List<Attribute> outputAttributes,
        Predicate<Attribute> shouldBeReplaced
    ) {
        List<NamedExpression> newProjections = new ArrayList<>(outputAttributes.size());
        List<Alias> nullLiterals = new ArrayList<>(outputAttributes.size());
        for (Attribute attr : outputAttributes) {
            NamedExpression projection;
            if (shouldBeReplaced.test(attr)) {
                Alias alias = new Alias(attr.source(), attr.name(), Literal.of(attr, null), attr.id());
                nullLiterals.add(alias);
                projection = alias.toAttribute();
            } else {
                projection = attr;
            }
            newProjections.add(projection);
        }
        return new Tuple<>(nullLiterals, newProjections);
    }

    /**
     * Collects references to foldables from the given logical plan, returning an {@link AttributeMap} that maps
     * foldable aliases to their corresponding literal values.
     *
     * @param plan The logical plan to analyze.
     * @param ctx The optimizer context providing fold context.
     * @return An {@link AttributeMap} containing foldable references and their literal values.
     */
    public static AttributeMap<Expression> foldableReferences(LogicalPlan plan, LogicalOptimizerContext ctx) {
        AttributeMap.Builder<Expression> collectRefsBuilder = AttributeMap.builder();

        // collect aliases bottom-up
        plan.forEachExpressionUp(Alias.class, a -> {
            var c = a.child();
            boolean shouldCollect = c.foldable();
            // try to resolve the expression based on an existing foldables
            if (shouldCollect == false) {
                c = c.transformUp(ReferenceAttribute.class, r -> collectRefsBuilder.build().resolve(r, r));
                shouldCollect = c.foldable();
            }
            if (shouldCollect) {
                collectRefsBuilder.put(a.toAttribute(), Literal.of(ctx.foldCtx(), c));
            }
        });

        return collectRefsBuilder.build();
    }
}
