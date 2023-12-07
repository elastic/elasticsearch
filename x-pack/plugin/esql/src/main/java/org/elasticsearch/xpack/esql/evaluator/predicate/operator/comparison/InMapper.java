/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanArrayBlock;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.evaluator.mapper.ExpressionMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.elasticsearch.xpack.esql.evaluator.predicate.operator.comparison.ComparisonMapper.EQUALS;

public class InMapper extends ExpressionMapper<In> {

    public static final InMapper IN_MAPPER = new InMapper();

    private InMapper() {}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public ExpressionEvaluator.Factory map(In in, Layout layout) {
        List<ExpressionEvaluator.Factory> listEvaluators = new ArrayList<>(in.list().size());
        in.list().forEach(e -> {
            Equals eq = new Equals(in.source(), in.value(), e);
            ExpressionEvaluator.Factory eqEvaluator = ((ExpressionMapper) EQUALS).map(eq, layout);
            listEvaluators.add(eqEvaluator);
        });
        return dvrCtx -> new InExpressionEvaluator(listEvaluators.stream().map(fac -> fac.get(dvrCtx)).toList());
    }

    record InExpressionEvaluator(List<EvalOperator.ExpressionEvaluator> listEvaluators) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Block eval(Page page) {
            int positionCount = page.getPositionCount();
            boolean[] values = new boolean[positionCount];
            BitSet nulls = new BitSet(positionCount); // at least one evaluation resulted in NULL on a row
            boolean nullInValues = false; // set when NULL's added in the values list: `field IN (valueA, null, valueB)`

            for (int i = 0; i < listEvaluators().size(); i++) {
                var evaluator = listEvaluators.get(i);
                try (BooleanBlock block = (BooleanBlock) evaluator.eval(page)) {
                    BooleanVector vector = block.asVector();
                    if (vector != null) {
                        updateValues(vector, values);
                    } else {
                        if (block.areAllValuesNull()) {
                            nullInValues = true;
                        } else {
                            updateValues(block, values, nulls);
                        }
                    }
                }
            }

            return evalWithNulls(values, nulls, nullInValues);
        }

        private static void updateValues(BooleanVector vector, boolean[] values) {
            for (int p = 0; p < values.length; p++) {
                values[p] |= vector.getBoolean(p);
            }
        }

        private static void updateValues(BooleanBlock block, boolean[] values, BitSet nulls) {
            for (int p = 0; p < values.length; p++) {
                if (block.isNull(p)) {
                    nulls.set(p);
                } else {
                    int start = block.getFirstValueIndex(p);
                    int end = start + block.getValueCount(p);
                    for (int i = start; i < end; i++) { // if MV_ANY is true, evaluation is true
                        if (block.getBoolean(i)) {
                            values[p] = true;
                            break;
                        }
                    }
                }
            }
        }

        private static Block evalWithNulls(boolean[] values, BitSet nulls, boolean nullInValues) {
            if (nulls.isEmpty() && nullInValues == false) {
                return new BooleanArrayVector(values, values.length).asBlock();
            } else {
                // 3VL: true trumps null; null trumps false.
                for (int i = 0; i < values.length; i++) {
                    if (values[i]) {
                        nulls.clear(i);
                    } else if (nullInValues) {
                        nulls.set(i);
                    } // else: leave nulls as is
                }
                if (nulls.isEmpty()) {
                    // no nulls and no multi-values means we must use a Vector
                    return new BooleanArrayVector(values, values.length).asBlock();
                } else {
                    return new BooleanArrayBlock(values, values.length, null, nulls, Block.MvOrdering.UNORDERED);
                }
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(() -> Releasables.close(listEvaluators));
        }
    }
}
