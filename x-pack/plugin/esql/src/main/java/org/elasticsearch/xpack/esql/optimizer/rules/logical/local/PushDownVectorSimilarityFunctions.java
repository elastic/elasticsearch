/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

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
public class PushDownVectorSimilarityFunctions extends ParameterizedRule<
    LogicalPlan,
    LogicalPlan,
    LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext context) {
        Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs = new HashMap<>();
        return plan.transformUp(LogicalPlan.class, p -> doRule(p, context.searchStats(), addedAttrs));
    }


    private LogicalPlan doRule(LogicalPlan plan, SearchStats searchStats, Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs) {
        // Collect field attributes from previous runs
        int originalAddedAttrsSize = addedAttrs.size();
        if (plan instanceof EsRelation rel) {
            addedAttrs.clear();
            for (Attribute attr : rel.output()) {
                if (attr instanceof FieldAttribute fa && fa.field() instanceof FunctionEsField) {
                    addedAttrs.put(fa.ignoreId(), fa);
                }
            }
        }

        if (plan instanceof Eval || plan instanceof Filter || plan instanceof Aggregate) {
            LogicalPlan transformedPlan = plan.transformExpressionsOnly(
                VectorSimilarityFunction.class,
                similarityFunction -> replaceFieldsForFieldTransformations(similarityFunction, addedAttrs, searchStats)
            );

            // No fields were added, return the original plan
            if (addedAttrs.size() == originalAddedAttrsSize) {
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
        VectorSimilarityFunction similarityFunction,
        Map<Attribute.IdIgnoringWrapper, Attribute> addedAttrs, SearchStats searchStats
    ) {
        // Only replace if it consists of a literal and the other a field attribute.
        // CanonicalizeVectorSimilarityFunctions ensures that if there is a literal, it will be on the right side.
        if (similarityFunction.left() instanceof FieldAttribute fieldAttr && similarityFunction.right() instanceof Literal) {

            // We can push down also for doc values, requires handling that case on the field mapper
            if (searchStats.isIndexed(fieldAttr.fieldName()) == false) {
                return similarityFunction;
            }

            // Change the similarity function to a reference of a transformation on the field
            MappedFieldType.BlockLoaderFunctionConfig blockLoaderFunctionConfig = similarityFunction.getBlockLoaderFunctionConfig();
            FunctionEsField functionEsField = new FunctionEsField(
                fieldAttr.field(),
                similarityFunction.dataType(),
                blockLoaderFunctionConfig
            );
            var name = rawTemporaryName(fieldAttr.name(), blockLoaderFunctionConfig.name());
            var newFunctionAttr = new FieldAttribute(
                fieldAttr.source(),
                fieldAttr.parentName(),
                fieldAttr.qualifier(),
                name,
                functionEsField,
                fieldAttr.nullable(),
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

        return similarityFunction;
    }
}
