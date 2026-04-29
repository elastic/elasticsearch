/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public final class RuleUtils {

    private RuleUtils() {}

    /**
     * @return a tuple of two lists:
     * <ol>
     * <li>A list of aliases to null literals for those data types in the {@code outputAttributes} that {@code shouldBeReplaced}.</li>
     * <li>A list of named expressions where attributes that match the predicate are replaced with their corresponding null alias.</li>
     * </ol>
     *
     * @param outputAttributes The original output attributes.
     * @param shouldBeReplaced A predicate to determine which attributes should be replaced with null aliases.
     */
    public static Tuple<List<Alias>, List<NamedExpression>> aliasedNulls(
        List<Attribute> outputAttributes,
        Predicate<Attribute> shouldBeReplaced
    ) {
        Map<DataType, Alias> nullLiterals = Maps.newLinkedHashMapWithExpectedSize(DataType.types().size());
        List<NamedExpression> newProjections = new ArrayList<>(outputAttributes.size());
        for (Attribute attr : outputAttributes) {
            NamedExpression projection;
            if (shouldBeReplaced.test(attr)) {
                DataType dt = attr.dataType();
                Alias nullAlias = nullLiterals.get(dt);
                // save the first field as null (per datatype)
                if (nullAlias == null) {
                    // Keep the same id so downstream query plans don't need updating
                    // NOTE: THIS IS BRITTLE AND CAN LEAD TO BUGS.
                    // In case some optimizer rule or so inserts a plan node that requires the field BEFORE the Eval that we're adding
                    // on top of the EsRelation, this can trigger a field extraction in the physical optimizer phase, causing wrong
                    // layouts due to a duplicate name id.
                    // If someone reaches here AGAIN when debugging e.g. ClassCastExceptions NPEs from wrong layouts, we should probably
                    // give up on this approach and instead insert EvalExecs in InsertFieldExtraction.
                    Alias alias = new Alias(attr.source(), attr.name(), Literal.of(attr, null), attr.id());
                    nullLiterals.put(dt, alias);
                    projection = alias.toAttribute();
                }
                // otherwise point to it since this avoids creating field copies
                else {
                    projection = new Alias(attr.source(), attr.name(), nullAlias.toAttribute(), attr.id());
                }
            } else {
                projection = attr;
            }
            newProjections.add(projection);
        }

        return new Tuple<>(new ArrayList<>(nullLiterals.values()), newProjections);
    }

    /**
     * Collects references to foldables from the given logical plan, returning an {@link AttributeMap} that maps
     * foldable aliases to their corresponding literal values. Equivalent to calling
     * {@link #foldableReferences(LogicalPlan, LogicalOptimizerContext, boolean)} with {@code stopAtSchemaBoundaries=false}.
     *
     * @param plan The logical plan to analyze.
     * @param ctx The optimizer context providing fold context.
     * @return An {@link AttributeMap} containing foldable references and their literal values.
     */
    public static AttributeMap<Expression> foldableReferences(LogicalPlan plan, LogicalOptimizerContext ctx) {
        return foldableReferences(plan, ctx, false);
    }

    /**
     * Collects references to foldables from the given logical plan, returning an {@link AttributeMap} that maps
     * foldable aliases to their corresponding literal values.
     *
     * @param plan The logical plan to analyze.
     * @param ctx The optimizer context providing fold context.
     * @param stopAtSchemaBoundaries If {@code true}, stops traversal at nodes that redefine attribute semantics:
     *                               {@link Fork} (branch-local aliases must not be treated as globally constant) and
     *                               {@link Aggregate} (STATS BY re-uses input attribute IDs, so source literals must
     *                               not be mistaken for the per-group output values).
     * @return An {@link AttributeMap} containing foldable references and their literal values.
     */
    public static AttributeMap<Expression> foldableReferences(
        LogicalPlan plan,
        LogicalOptimizerContext ctx,
        boolean stopAtSchemaBoundaries
    ) {
        AttributeMap.Builder<Expression> refs = AttributeMap.builder();
        collectFoldableRefs(plan, refs, ctx, stopAtSchemaBoundaries);
        return refs.build();
    }

    private static void collectFoldableRefs(
        LogicalPlan plan,
        AttributeMap.Builder<Expression> refs,
        LogicalOptimizerContext ctx,
        boolean stopAtSchemaBoundaries
    ) {
        // Fork: branch-local aliases (e.g. null-padding Evals) must not be treated as globally constant.
        // Aggregate: STATS BY re-uses grouping attribute IDs from the input (e.g. a ROW alias color=["b","p","y"]
        // would make the SORT-by-color key appear foldable). Grouped output values are data-dependent.
        if (stopAtSchemaBoundaries && (plan instanceof Fork || plan instanceof Aggregate)) {
            return;
        }
        for (LogicalPlan child : plan.children()) {
            collectFoldableRefs(child, refs, ctx, stopAtSchemaBoundaries);
        }
        plan.forEachExpression(Alias.class, a -> {
            var c = a.child();
            boolean shouldCollect = c.foldable();
            if (shouldCollect == false) {
                c = c.transformUp(ReferenceAttribute.class, r -> refs.build().resolve(r, r));
                shouldCollect = c.foldable();
            }
            if (shouldCollect) {
                refs.put(a.toAttribute(), Literal.of(ctx.foldCtx(), c));
            }
        });
    }
}
