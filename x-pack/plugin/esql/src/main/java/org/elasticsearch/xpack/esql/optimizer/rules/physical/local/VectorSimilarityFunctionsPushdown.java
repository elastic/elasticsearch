/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldTransformationAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorSimilarityFunctionsPushdown extends PhysicalOptimizerRules.OptimizerRule<EvalExec> {

    // TODO We could replace on filters as well

    @Override
    protected PhysicalPlan rule(EvalExec eval) {
        return replaceSimilarityFunctionsForFieldTransformations(eval);
    }

    @SuppressWarnings("unchecked")
    private EvalExec replaceSimilarityFunctionsForFieldTransformations(EvalExec eval) {
        Map<Attribute, Attribute> replacements = new HashMap<>();
        EvalExec resultEval = (EvalExec) eval.transformExpressionsDown(VectorSimilarityFunction.class, similarityFunction -> {
            if (similarityFunction.left() instanceof Literal ^ similarityFunction.right() instanceof Literal) {
                return replaceFieldsForFieldTransformations(similarityFunction, replacements);
            }
            return similarityFunction;
        });

        if (replacements.isEmpty()) {
            return eval;
        }

        resultEval = (EvalExec) resultEval.transformDown(FieldExtractExec.class, fieldEx -> {
            List<Attribute> attrs = fieldEx.attributesToExtract();
            assert attrs.stream().anyMatch(replacements::containsKey) : "Expected at least one attribute to be replaced";
            List<Attribute> replaceAttrs = attrs.stream().map(a -> replacements.getOrDefault(a, a)).toList();

            return fieldEx.withAttributesToExtract(replaceAttrs);
        });

        return resultEval;
    }

    private static Expression replaceFieldsForFieldTransformations(
        VectorSimilarityFunction similarityFunction,
        Map<Attribute, Attribute> replacements
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

        // Create a transformation that computes similarity between each value and the literal value
        DenseVectorFieldMapper.DenseVectorBlockValueLoader blockValueLoader =
            new DenseVectorFieldMapper.DenseVectorBlockValueLoader(similarityFunction.getSimilarityFunction(), vectorArray);

        // Change the similarity function to a reference of a transformation on the field
        var fieldTransformationAttribute = new FieldTransformationAttribute<>(fieldAttribute, blockValueLoader, DataType.DOUBLE);
        replacements.put(fieldAttribute, fieldTransformationAttribute);
        return fieldTransformationAttribute;
    }
}
