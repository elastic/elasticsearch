/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.planner.ComparisonMapper.EQUALS;

class InMapper extends EvalMapper.ExpressionMapper<In> {

    public static final InMapper IN_MAPPER = new InMapper();

    private InMapper() {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected Supplier<EvalOperator.ExpressionEvaluator> map(In in, Layout layout) {
        List<Supplier<EvalOperator.ExpressionEvaluator>> listEvaluators = new ArrayList<>(in.list().size());
        in.list().forEach(e -> {
            Equals eq = new Equals(in.source(), in.value(), e);
            Supplier<EvalOperator.ExpressionEvaluator> eqEvaluator = ((EvalMapper.ExpressionMapper) EQUALS).map(eq, layout);
            listEvaluators.add(eqEvaluator);
        });
        return () -> new InExpressionEvaluator(listEvaluators.stream().map(Supplier::get).toList());
    }

    record InExpressionEvaluator(List<EvalOperator.ExpressionEvaluator> listEvaluators) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            int positionCount = page.getPositionCount();
            BooleanVector.Builder result = BooleanVector.newVectorBuilder(positionCount);
            for (int p = 0; p < positionCount; p++) {
                result.appendBoolean(evalPosition(p, page));
            }
            return result.build().asBlock();
        }

        private boolean evalPosition(int pos, Page page) {
            for (EvalOperator.ExpressionEvaluator evaluator : listEvaluators) {
                Block block = evaluator.eval(page);
                Vector vector = block.asVector();
                if (vector != null) {
                    BooleanVector booleanVector = (BooleanVector) vector;
                    if (booleanVector.getBoolean(pos)) {
                        return true;
                    }
                } else {
                    BooleanBlock boolBlock = (BooleanBlock) block;
                    if (boolBlock.isNull(pos) == false) {
                        int start = block.getFirstValueIndex(pos);
                        int end = start + block.getValueCount(pos);
                        for (int i = start; i < end; i++) {
                            if (((BooleanBlock) block).getBoolean(i)) {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }
    }
}
