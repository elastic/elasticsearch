/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * The values of {@link FieldExpression}s on edge cases. Index sorts persist these values as the order of segments, so a change
 * here means that segments sorted by one version would be out of order for the next.
 */
public class FieldExpressionValuesTests extends ESTestCase {
    private static final BlockFactory FACTORY = TestBlockFactory.getNonBreakingInstance();

    public void testNotOptimized() {
        // The optimizer rewrites -n > 0 to n < 0, which is true rather than null when -n overflows
        assertValues("-n > 0", Map.of("n", DataType.LONG), rows(Long.MIN_VALUE, -1L, 1L), Arrays.asList(null, true, false));
    }

    /** One row per value of a single field. */
    static List<List<Object>> rows(Object... values) {
        List<List<Object>> rows = new ArrayList<>();
        for (Object value : values) {
            rows.add(Arrays.asList(value));
        }
        return rows;
    }

    /**
     * Compiles {@code key} as an index sort does and checks its values on {@code rows}, whose columns are the fields in the order
     * of {@code types}. A multi-valued field or result is a {@link List}.
     */
    static void assertValues(String key, Map<String, DataType> types, List<List<Object>> rows, List<Object> expected) {
        Expression expression = ((Eval) FieldExpression.parser().parseQuery("ROW x = 1 | EVAL k = " + key)).fields().getFirst().child();
        Map<String, DataType> fieldTypes = new LinkedHashMap<>();
        List<String> order = new ArrayList<>(types.keySet());
        for (String field : FieldExpression.fields(expression)) {
            fieldTypes.put(field, types.get(field));
        }
        List<List<Object>> inputs = new ArrayList<>();
        for (List<Object> row : rows) {
            List<Object> input = new ArrayList<>();
            for (String field : fieldTypes.keySet()) {
                input.add(row.get(order.indexOf(field)));
            }
            inputs.add(input);
        }
        FieldExpression compiled = FieldExpression.compile(expression, fieldTypes);
        DriverContext context = new DriverContext(
            BigArrays.NON_RECYCLING_INSTANCE,
            FACTORY,
            null,
            "test",
            DriverContext.WarningsMode.IGNORE
        );
        Block[] blocks = BlockUtils.fromList(FACTORY, inputs);
        Page page = new Page(blocks);
        try (ExpressionEvaluator evaluator = compiled.evaluator().get(context); Block result = evaluator.eval(page)) {
            List<Object> actual = new ArrayList<>();
            for (int p = 0; p < result.getPositionCount(); p++) {
                actual.add(BlockUtils.toJavaObject(result, p));
            }
            assertThat(key, actual, equalTo(expected));
        } finally {
            Releasables.close(blocks);
        }
    }
}
