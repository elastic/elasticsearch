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
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Replaces vector similarity functions with a field attribute that applies
 * the similarity function during value loading, when one side of the function is a literal.
 * It also adds the new field function attribute to the EsRelation output, and adds a projection after it to remove it from the output.
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
        private boolean planWasTransformed = false;

        private Rule(LocalLogicalOptimizerContext context, LogicalPlan plan) {
            this.context = context;
            this.plan = plan;
        }

        private LogicalPlan doRule(LogicalPlan plan) {
            planWasTransformed = false;
            if (plan instanceof Eval || plan instanceof Filter || plan instanceof Aggregate) {
                LogicalPlan transformedPlan = plan.transformExpressionsOnly(Expression.class, e -> {
                    if (e instanceof BlockLoaderExpression ble) {
                        return transformExpression(e, ble);
                    }
                    return e;
                });

                // TODO rebuild everything one time rather than after each find.
                if (planWasTransformed == false) {
                    return plan;
                }

                List<Attribute> previousAttrs = transformedPlan.output();
                // Transforms EsRelation to extract the new attributes
                List<Attribute> addedAttrsList = addedAttrs.values().stream().toList();
                transformedPlan = transformedPlan.transformDown(EsRelation.class, esRelation -> {
                    AttributeSet updatedOutput = esRelation.outputSet().combine(AttributeSet.of(addedAttrsList));
                    return esRelation.withAttributes(updatedOutput.stream().toList());
                });
                // Transforms Projects so the new attribute is not discarded
                transformedPlan = transformedPlan.transformDown(EsqlProject.class, esProject -> {
                    List<NamedExpression> projections = new ArrayList<>(esProject.projections());
                    projections.addAll(addedAttrsList);
                    return esProject.withProjections(projections);
                });

                return new EsqlProject(Source.EMPTY, transformedPlan, previousAttrs);
            }
            return plan;
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
            planWasTransformed = true;
            return replaceFieldsForFieldTransformations(e, fuse);
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
