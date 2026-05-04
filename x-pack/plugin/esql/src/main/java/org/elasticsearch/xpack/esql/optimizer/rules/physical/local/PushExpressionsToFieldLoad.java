/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.expression.function.blockloader.BlockLoaderExpression;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Replaces {@link Expression}s that can be pushed to field loading with a field attribute
 * that calculates the expression during value loading. See {@link BlockLoaderExpression}
 * for more about how these loads are implemented and why we do this.
 * <p>
 *     This rule runs in one downward (aka output-to-read) pass, making three sorts
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
 *         </pre>
 *     </li>
 * </ul>
 * <p>
 *     The actual loading of pushed attributes is handled by {@link InsertFieldExtraction},
 *     which runs after this rule. It sees the new {@link FieldAttribute} references (backed
 *     by {@link FunctionEsField}) and inserts {@link FieldExtractExec}
 *     nodes that create the appropriate block loaders.
 * </p>
 */
public class PushExpressionsToFieldLoad extends ParameterizedRule<PhysicalPlan, PhysicalPlan, LocalPhysicalOptimizerContext> {

    @Override
    public PhysicalPlan apply(PhysicalPlan plan, LocalPhysicalOptimizerContext context) {
        PushRule rule = new PushRule(context);
        return plan.transformDown(PhysicalPlan.class, rule::doRule);
    }

    /**
     * Lazily scans the plan to determine which nodes can push expressions to field loading.
     * A node can push only if its entire data lineage traces to exactly one {@link EsQueryExec}
     * (non-LOOKUP). This prevents incorrect pushes above join nodes, where a field reference
     * could resolve from the wrong side of the join.
     */
    private class Primaries {
        private static final int UNSUPPORTED = -1;
        private final Map<PhysicalPlan, Integer> counts = new IdentityHashMap<>();

        boolean canPush(PhysicalPlan plan) {
            return countFor(plan) == 1;
        }

        private int countFor(PhysicalPlan plan) {
            scanSubtree(plan);
            return counts.get(plan);
        }

        private void scanSubtree(PhysicalPlan plan) {
            if (counts.containsKey(plan)) {
                return;
            }
            if (plan.children().isEmpty()) {
                if (plan instanceof EsQueryExec exec && exec.indexMode() != IndexMode.LOOKUP) {
                    counts.put(plan, 1);
                } else {
                    counts.put(plan, UNSUPPORTED);
                }
            } else {
                int total = 0;
                for (PhysicalPlan child : plan.children()) {
                    scanSubtree(child);
                    int childCount = counts.get(child);
                    if (childCount == UNSUPPORTED) {
                        counts.put(plan, UNSUPPORTED);
                        return;
                    }
                    total += childCount;
                }
                counts.put(plan, total);
            }
        }
    }

    private class PushRule {
        private final Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs = new HashMap<>();

        private final LocalPhysicalOptimizerContext context;
        private final Primaries primaries = new Primaries();

        private boolean addedNewAttribute = false;

        private PushRule(LocalPhysicalOptimizerContext context) {
            this.context = context;
        }

        private PhysicalPlan doRule(PhysicalPlan plan) {
            addedNewAttribute = false;
            if (plan instanceof EvalExec || plan instanceof FilterExec || plan instanceof AggregateExec) {
                return transformPotentialInvocation(plan);
            }
            if (addedAttrs.isEmpty()) {
                return plan;
            }
            if (plan instanceof ProjectExec project) {
                return transformProject(project);
            }
            return plan;
        }

        private PhysicalPlan transformPotentialInvocation(PhysicalPlan plan) {
            PhysicalPlan transformedPlan = plan.transformExpressionsOnly(Expression.class, e -> {
                if (e instanceof BlockLoaderExpression ble) {
                    return transformExpression(plan, e, ble);
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
            // return new ProjectExec(Source.EMPTY, transformedPlan, transformedPlan.output());
            return transformedPlan;
        }

        private Expression transformExpression(PhysicalPlan nodeWithExpression, Expression e, BlockLoaderExpression ble) {
            BlockLoaderExpression.PushedBlockLoaderExpression fuse = ble.tryPushToFieldLoading(context.searchStats());
            if (fuse == null) {
                return e;
            }
            if (fuse.field().field() instanceof MultiTypeEsField) {
                return e;
            }
            if (primaries.canPush(nodeWithExpression) == false) {
                return e;
            }
            MappedFieldType.FieldExtractPreference preference = context.configuration().pragmas().fieldExtractPreference();
            if (context.searchStats().supportsLoaderConfig(fuse.field().fieldName(), fuse.config(), preference) == false) {
                return e;
            }
            addedNewAttribute = true;
            return replaceFieldsForFieldTransformations(e, fuse);
        }

        private PhysicalPlan transformProject(ProjectExec project) {
            List<NamedExpression> projections = new ArrayList<>(project.projections());
            projections.addAll(addedAttrs.values());
            return new ProjectExec(project.source(), project.child(), projections);
        }

        private Expression replaceFieldsForFieldTransformations(Expression e, BlockLoaderExpression.PushedBlockLoaderExpression fuse) {
            FunctionEsField functionEsField = new FunctionEsField(fuse.field().field(), e.dataType(), fuse.config());
            String name = rawTemporaryName(
                fuse.field().name(),
                fuse.config().function().toString(),
                String.valueOf(fuse.config().hashCode())
            );
            FieldAttribute newFunctionAttr = new FieldAttribute(
                e.source(),
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
    }
}
