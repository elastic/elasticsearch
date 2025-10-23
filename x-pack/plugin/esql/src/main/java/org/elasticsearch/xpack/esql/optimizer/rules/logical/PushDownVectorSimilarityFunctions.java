/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldFunctionAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

import java.util.List;

/**
 * Replaces vector similarity functions with a field function attribute that applies
 * the similarity function during value loading, when one side of the function is a literal.
 * It also adds the new field function attribute to the EsRelation output, and adds a projection after it to remove it from the output.
 */
public class PushDownVectorSimilarityFunctions extends OptimizerRules.OptimizerRule<Eval> {

    @Override
    protected LogicalPlan rule(Eval eval) {
        // TODO Extend to WHERE if necessary

        AttributeSet.Builder addedAttrs = AttributeSet.builder();
        Eval transformedEval = (Eval) eval.transformExpressionsDown(VectorSimilarityFunction.class, similarityFunction -> {
            if (similarityFunction.left() instanceof Literal ^ similarityFunction.right() instanceof Literal) {
                return replaceFieldsForFieldTransformations(similarityFunction, addedAttrs);
            }
            return similarityFunction;
        });

        if (addedAttrs.isEmpty()) {
            return eval;
        }

        List<Attribute> previousAttrs = transformedEval.output();
        transformedEval = (Eval) transformedEval.transformDown(
            EsRelation.class,
            esRelation -> esRelation.withAttributes(addedAttrs.build().combine(esRelation.outputSet()).stream().toList())
        );

        return new EsqlProject(Source.EMPTY, transformedEval, previousAttrs);
    }

    private static Expression replaceFieldsForFieldTransformations(
        VectorSimilarityFunction similarityFunction,
        AttributeSet.Builder addedAttrs
    ) {
        // Only replace if exactly one side is a literal and the other a field attribute
        if ((similarityFunction.left() instanceof Literal ^ similarityFunction.right() instanceof Literal) == false) {
            return similarityFunction;
        }

        Literal literal = (Literal) (similarityFunction.left() instanceof Literal ? similarityFunction.left() : similarityFunction.right());
        FieldAttribute fieldAttribute = null;
        if (similarityFunction.left() instanceof FieldAttribute fa) {
            fieldAttribute = fa;
        } else if (similarityFunction.right() instanceof FieldAttribute fa) {
            fieldAttribute = fa;
        }
        if (fieldAttribute == null) {
            return similarityFunction;
        }
        @SuppressWarnings("unchecked")
        List<Number> vectorList = (List<Number>) literal.value();
        float[] vectorArray = new float[vectorList.size()];
        for (int i = 0; i < vectorList.size(); i++) {
            vectorArray[i] = vectorList.get(i).floatValue();
        }

        // Change the similarity function to a reference of a transformation on the field
        FieldFunctionAttribute fieldFunctionAttribute = FieldFunctionAttribute.fromFieldAttribute(fieldAttribute, similarityFunction);
        addedAttrs.add(fieldFunctionAttribute);
        return fieldFunctionAttribute;
    }
}
