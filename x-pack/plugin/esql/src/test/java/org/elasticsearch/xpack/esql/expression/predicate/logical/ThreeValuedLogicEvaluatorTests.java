/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ThreeValuedLogicEvaluatorTests extends ComputeTestCase {
    private static final Boolean T = true;
    private static final Boolean F = false;
    private static final Boolean N = null;

    private static final List<Boolean> LEFT = Arrays.asList(T, T, T, F, F, F, N, N, N);
    private static final List<Boolean> RIGHT = Arrays.asList(T, F, N, T, F, N, T, F, N);

    private final ReferenceAttribute left = new ReferenceAttribute(Source.EMPTY, "left", DataType.BOOLEAN);
    private final ReferenceAttribute right = new ReferenceAttribute(Source.EMPTY, "right", DataType.BOOLEAN);

    public void testAndOverEveryCombinationOfTrueFalseAndNull() {
        assertThat(evaluate(new And(Source.EMPTY, left, right)), equalTo(Arrays.asList(T, F, N, F, F, F, N, F, N)));
    }

    public void testOrOverEveryCombinationOfTrueFalseAndNull() {
        assertThat(evaluate(new Or(Source.EMPTY, left, right)), equalTo(Arrays.asList(T, T, T, T, F, N, T, N, N)));
    }

    public void testNotKeepsNull() {
        assertThat(evaluate(new Not(Source.EMPTY, left)), equalTo(Arrays.asList(F, F, F, T, T, T, N, N, N)));
    }

    public void testAndNullStillDecidesFalseFromTheOtherSide() {
        assertThat(evaluate(new And(Source.EMPTY, new Not(Source.EMPTY, left), right)), equalTo(Arrays.asList(F, F, F, T, F, N, N, F, N)));
    }

    public void testNotOfAnOrKeepsNullWhereTheOrIsNull() {
        assertThat(evaluate(new Not(Source.EMPTY, new Or(Source.EMPTY, left, right))), equalTo(Arrays.asList(F, F, F, F, T, N, F, N, N)));
    }

    private List<Boolean> evaluate(Expression expression) {
        var blockFactory = blockFactory();
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        Layout layout = new Layout.Builder().append(left).append(right).build();
        List<Boolean> result = new ArrayList<>();
        try (
            ExpressionEvaluator evaluator = EvalMapper.toEvaluator(FoldContext.small(), expression, layout).get(driverContext);
            Page page = new Page(booleans(LEFT), booleans(RIGHT));
            Block block = evaluator.eval(page)
        ) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                result.add((Boolean) BlockUtils.toJavaObject(block, position));
            }
        }
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        return result;
    }

    private BooleanBlock booleans(List<Boolean> values) {
        try (BooleanBlock.Builder builder = blockFactory().newBooleanBlockBuilder(values.size())) {
            for (Boolean value : values) {
                if (value == null) {
                    builder.appendNull();
                } else {
                    builder.appendBoolean(value);
                }
            }
            return builder.build();
        }
    }
}
