/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
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
        Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs = new HashMap<>();
        return plan.transformDown(LogicalPlan.class, p -> doRule(p, context, addedAttrs));
    }

    private LogicalPlan doRule(
        LogicalPlan plan,
        LocalLogicalOptimizerContext context,
        Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs
    ) {
        Holder<Boolean> planWasTransformed = new Holder<>(false);
        if (plan instanceof Eval || plan instanceof Filter || plan instanceof Aggregate) {
            LogicalPlan transformedPlan = plan.transformExpressionsOnly(Expression.class, e -> {
                if (e instanceof BlockLoaderExpression ble) {
                    BlockLoaderExpression.PushedBlockLoaderExpression fuse = ble.tryPushToFieldLoading(context.searchStats());
                    if (fuse != null
                        && context.searchStats()
                            .supportsLoaderConfig(
                                fuse.field().fieldName(),
                                fuse.config(),
                                context.configuration().pragmas().fieldExtractPreference()
                            )) {
                        planWasTransformed.set(true);
                        return replaceFieldsForFieldTransformations(e, addedAttrs, fuse);
                    }
                }
                return e;
            });

            if (planWasTransformed.get() == false) {
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

    private static Expression replaceFieldsForFieldTransformations(
        Expression e,
        Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs,
        BlockLoaderExpression.PushedBlockLoaderExpression fuse
    ) {
        // Change the similarity function to a reference of a transformation on the field
        FunctionEsField functionEsField = new FunctionEsField(fuse.field().field(), e.dataType(), fuse.config());
        var name = rawTemporaryName(fuse.field().name(), fuse.config().function().toString(), String.valueOf(fuse.config().hashCode()));
        // TODO: Check if exists before adding, retrieve the previous one
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
}
