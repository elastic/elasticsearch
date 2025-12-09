/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Replaces {@link Expression}s that can be pushed to field loading with a field attribute
 * that calculates the expression during value loading. See {@link BlockLoaderExpression}
 * for more about how these loads are implemented and why we do this.
 * <p>
 *     This rule runs in one downward (aka output-to-read) pass, making four sorts
 *     of transformations:
 * </p>
 * <ul>
 *     <li>
 *         When we see a use of a <strong>new</strong> pushable function we build an
 *         attribute for the function, record that attribute, and discard it after use.
 *         For example, {@code EVAL l = LENGTH(message)} becomes
 *         {@code EVAL l = $$message$LENGTH$1324$$ | DROP $$message$LENGTH$1324$$ }.
 *         We need the {@code DROP} so we don't change the output schema.
 *     </li>
 *     <li>
 *         When we see a use of pushable function for which we already have an attribute
 *         we just use it. This looks like the {@code l} attribute in
 *         {@code EVAL l = LENGTH(message) | EVAL l2 = LENGTH(message)}
 *     </li>
 *     <li>
 *         When we see a PROJECT, add any new attributes to the projection so we can use
 *         them on previously visited nodes. So {@code KEEP foo | EVAL l = LENGTH(message)}
 *         becomes
 *         <pre>{@code
 *           | KEEP foo, $$message$LENGTH$1324$$
 *           | EVAL l = $$message$LENGTH$1324$$
 *           | DROP $$message$LENGTH$1324$$}
 *         }</pre>
 *     </li>
 *     <li>
 *         When we see a relation, add the attribute to it.
 *     </li>
 * </ul>
 */
public class PushExpressionsToFieldLoad extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        Rule rule = new Rule(context, plan);
        return plan.transformDown(LogicalPlan.class, rule::doRule);
    }

    private class Rule {
        private final Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs = new HashMap<>();

        private final LocalLogicalOptimizerContext context;
        private final LogicalPlan plan;

        /**
         * The primary indices, lazily initialized.
         */
        private List<EsRelation> primaries;
        private boolean addedNewAttribute = false;

        private Rule(LocalLogicalOptimizerContext context, LogicalPlan plan) {
            this.context = context;
            this.plan = plan;
        }

        private LogicalPlan doRule(LogicalPlan plan) {
            addedNewAttribute = false;
            if (plan instanceof Eval || plan instanceof Filter || plan instanceof Aggregate) {
                return transformPotentialInvocation(plan);
            }
            if (addedAttrs.isEmpty()) {
                return plan;
            }
            if (plan instanceof Project project) {
                return transformProject(project);
            }
            if (plan instanceof EsRelation rel) {
                return transformRelation(rel);
            }
            return plan;
        }

        private LogicalPlan transformPotentialInvocation(LogicalPlan plan) {
            LogicalPlan transformedPlan = plan.transformExpressionsOnly(Expression.class, e -> {
                if (e instanceof BlockLoaderExpression ble) {
                    return transformExpression(e, ble);
                }
                return e;
            });
            if (addedNewAttribute == false) {
                /*
                 * Either didn't see anything pushable or everything pushable already
                 * has a pushed attribute.
                 */
                return plan;
            }
            // Found a new pushable attribute, discard it *after* use so we don't modify the output.
            return new EsqlProject(Source.EMPTY, transformedPlan, transformedPlan.output());
        }

        private Expression transformExpression(Expression e, BlockLoaderExpression ble) {
            BlockLoaderExpression.PushedBlockLoaderExpression fuse = ble.tryPushToFieldLoading(context.searchStats());
            if (fuse == null) {
                return e;
            }
            if (anyPrimaryContains(fuse.field()) == false) {
                return e;
            }
            var preference = context.configuration().pragmas().fieldExtractPreference();
            if (context.searchStats().supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), preference) == false) {
                return e;
            }
            addedNewAttribute = true;
            return replaceFieldsForFieldTransformations(e, fuse);
        }

        private LogicalPlan transformProject(Project project) {
            // Preserve any pushed attributes so we can use them later
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.addAll(addedAttrs.values());
            return project.withProjections(projections);
        }

        private LogicalPlan transformRelation(EsRelation rel) {
            // Add the pushed attribute
            if (rel.indexMode() == IndexMode.LOOKUP) {
                return rel;
            }
            AttributeSet updatedOutput = rel.outputSet().combine(AttributeSet.of(addedAttrs.values()));
            return rel.withAttributes(updatedOutput.stream().toList());
        }

        private Expression replaceFieldsForFieldTransformations(Expression e, BlockLoaderExpression.PushedBlockLoaderExpression fuse) {
            // Change the expression to a reference of the pushed down function on the field
            FunctionEsField functionEsField = new FunctionEsField(fuse.field().field(), e.dataType(), fuse.config());
            var name = rawTemporaryName(fuse.field().name(), fuse.config().function().toString(), String.valueOf(fuse.config().hashCode()));
            var newFunctionAttr = new FieldAttribute(
                fuse.field().source(),
                fuse.field().parentName(),
                fuse.field().qualifier(),
                name,
                functionEsField,
                fuse.field().nullable(),
                new NameId(),
                true
            );
            Attribute.IdIgnoringWrapper key = newFunctionAttr.ignoreId();
            if (addedAttrs.containsKey(key)) {
                return addedAttrs.get(key);
            }

            addedAttrs.put(key, newFunctionAttr);
            return newFunctionAttr;
        }

        private List<EsRelation> primaries() {
            if (primaries == null) {
                primaries = new ArrayList<>(2);
                plan.forEachUp(EsRelation.class, r -> {
                    if (r.indexMode() != IndexMode.LOOKUP) {
                        primaries.add(r);
                    }
                });
            }
            return primaries;
        }

        private boolean anyPrimaryContains(FieldAttribute attr) {
            for (EsRelation primary : primaries()) {
                if (primary.outputSet().contains(attr)) {
                    return true;
                }
            }
            return false;
        }
    }
}
