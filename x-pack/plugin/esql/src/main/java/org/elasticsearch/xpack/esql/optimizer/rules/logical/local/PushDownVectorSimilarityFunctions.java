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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;

import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.Attribute.rawTemporaryName;

/**
 * Replaces vector similarity functions with a field function attribute that applies
 * the similarity function during value loading, when one side of the function is a literal.
 * It also adds the new field function attribute to the EsRelation output, and adds a projection after it to remove it from the output.
 */
public class PushDownVectorSimilarityFunctions extends OptimizerRules.ParameterizedOptimizerRule<Eval, LocalLogicalOptimizerContext> {

    public PushDownVectorSimilarityFunctions() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Eval eval, LocalLogicalOptimizerContext context) {
        // TODO Extend to WHERE if necessary

        AttributeSet.Builder addedAttrs = AttributeSet.builder();
        Eval transformedEval = (Eval) eval.transformExpressionsDown(VectorSimilarityFunction.class, similarityFunction -> {
            if (similarityFunction.left() instanceof Literal ^ similarityFunction.right() instanceof Literal) {
                return replaceFieldsForFieldTransformations(similarityFunction, addedAttrs, context);
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
        AttributeSet.Builder addedAttrs,
        LocalLogicalOptimizerContext context
    ) {
        // Only replace if exactly one side is a literal and the other a field attribute
        if ((similarityFunction.left() instanceof Literal ^ similarityFunction.right() instanceof Literal) == false) {
            return similarityFunction;
        }

        Literal literal = (Literal) (similarityFunction.left() instanceof Literal ? similarityFunction.left() : similarityFunction.right());
        FieldAttribute fieldAttr = null;
        if (similarityFunction.left() instanceof FieldAttribute fa) {
            fieldAttr = fa;
        } else if (similarityFunction.right() instanceof FieldAttribute fa) {
            fieldAttr = fa;
        }
        // We can push down also for doc values, requires handling that case on the field mapper
        if (fieldAttr == null || context.searchStats().isIndexed(fieldAttr.fieldName()) == false) {
            return similarityFunction;
        }

        @SuppressWarnings("unchecked")
        List<Number> vectorList = (List<Number>) literal.value();
        float[] vectorArray = new float[vectorList.size()];
        for (int i = 0; i < vectorList.size(); i++) {
            vectorArray[i] = vectorList.get(i).floatValue();
        }

        // Change the similarity function to a reference of a transformation on the field
        FunctionEsField functionEsField = new FunctionEsField(
            fieldAttr.field(),
            similarityFunction,
            similarityFunction.getBlockLoaderFunctionConfig()
        );
        var nameId = new NameId();
        var name = rawTemporaryName(fieldAttr.name(), similarityFunction.nodeName(), nameId.toString());
        var functionAttr = new FieldAttribute(
            fieldAttr.source(),
            fieldAttr.parentName(),
            fieldAttr.qualifier(),
            name,
            functionEsField,
            fieldAttr.nullable(),
            nameId,
            fieldAttr.synthetic()
        );
        addedAttrs.add(functionAttr);
        return functionAttr;
    }
}
