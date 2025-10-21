/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldFunctionAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.vector.VectorSimilarityFunction;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VectorSimilarityFunctionsPushdown extends PhysicalOptimizerRules.OptimizerRule<PhysicalPlan> {

    // TODO We could replace on filters as well

    @Override
    protected PhysicalPlan rule(PhysicalPlan plan) {
        return replaceSimilarityFunctions(plan);
    }

    private PhysicalPlan replaceSimilarityFunctions(PhysicalPlan plan) {

        Set<Attribute> denseVectorAttrsToKeep = findDenseVectorAttrsToKeep(plan);

        return plan.transformDown(EvalExec.class, eval -> replaceSimilarityFunctionsForFieldTransformations(eval, denseVectorAttrsToKeep));
    }

    private static Set<Attribute> findDenseVectorAttrsToKeep(PhysicalPlan plan) {
        Map<Attribute, Integer> nonFunctionUsages = new HashMap<>();

        plan.references().forEach(f -> {
            if (f instanceof FieldAttribute fieldAttr && fieldAttr.dataType() == DataType.DENSE_VECTOR) {
                nonFunctionUsages.compute(fieldAttr, (k, v) -> v == null ? 1 : v + 1);
            }
        });

        plan.forEachExpression(VectorSimilarityFunction.class, similarityFunction -> {
            if (similarityFunction.left() instanceof FieldAttribute fieldAttr) {
                assert nonFunctionUsages.containsKey(fieldAttr) : "Expected field attribute to be retrieved from plan references";
                nonFunctionUsages.computeIfPresent(fieldAttr, (k, v) -> v - 1);
            }
            if (similarityFunction.right() instanceof FieldAttribute fieldAttr) {
                assert nonFunctionUsages.containsKey(fieldAttr) : "Expected field attribute to be retrieved from plan references";
                nonFunctionUsages.computeIfPresent(fieldAttr, (k, v) -> v - 1);
            }
        });

        return nonFunctionUsages.keySet().stream().filter(k -> nonFunctionUsages.get(k) > 0).collect(Collectors.toSet());
    }

    private EvalExec replaceSimilarityFunctionsForFieldTransformations(EvalExec eval, Set<Attribute> denseVectorAttrsToKeep) {
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
            List<Attribute> replacedAttrs = new ArrayList<>();
            for (Attribute attr : attrs) {
                // Add the replacement attribute, and also the original attribute if it's being used in a non-similarity function context
                if (replacements.containsKey(attr)) {
                    replacedAttrs.add(replacements.get(attr));
                    if (denseVectorAttrsToKeep.contains(attr)) {
                        replacedAttrs.add(attr);
                    }
                } else {
                    replacedAttrs.add(attr);
                }
            }

            // TODO Check that we don't have to duplicate the field, or run a second optimization to prune out unused attrs
            return fieldEx.withAttributesToExtract(replacedAttrs).withFieldFunctionAttributes(new HashSet<>(replacements.values()));
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
        DenseVectorFieldMapper.DenseVectorBlockLoaderValueFunction blockValueLoader =
            new DenseVectorFieldMapper.DenseVectorBlockLoaderValueFunction(similarityFunction.getSimilarityFunction(), vectorArray);

        // Change the similarity function to a reference of a transformation on the field
        var fieldFunctionAttribute = new FieldFunctionAttribute(fieldAttribute, blockValueLoader, DataType.DOUBLE);
        replacements.put(fieldAttribute, fieldFunctionAttribute);
        return fieldFunctionAttribute;
    }
}
