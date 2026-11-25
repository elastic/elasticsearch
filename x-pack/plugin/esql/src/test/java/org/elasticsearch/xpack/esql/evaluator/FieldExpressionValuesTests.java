/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.apache.lucene.util.BytesRef;
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

    public void testLongArithmetic() {
        Map<String, DataType> n = Map.of("n", DataType.LONG);
        assertValues("n + 1", n, rows(Long.MAX_VALUE, 1L, null, List.of(1L, 2L)), Arrays.asList(null, 2L, null, null));
        assertValues("n - 1", n, rows(Long.MIN_VALUE, 1L), Arrays.asList(null, 0L));
        assertValues("n * 2", n, rows(Long.MAX_VALUE, -3L), Arrays.asList(null, -6L));
        assertValues("n / 2", n, rows(7L, -7L), List.of(3L, -3L));
        assertValues("n / 0", n, rows(7L), Arrays.asList((Object) null));
        assertValues("n % 3", n, rows(7L, -7L), List.of(1L, -1L));
        assertValues("n % 0", n, rows(7L), Arrays.asList((Object) null));
        assertValues("-n", n, rows(Long.MIN_VALUE, 5L), Arrays.asList(null, -5L));
    }

    public void testIntegerArithmetic() {
        Map<String, DataType> i = Map.of("i", DataType.INTEGER);
        assertValues("i + 1", i, rows(Integer.MAX_VALUE, 1), Arrays.asList(null, 2));
        assertValues("i / 2", i, rows(-7), List.of(-3));
        // Mixed with a long, the integer widens first
        assertValues(
            "i + n",
            types("i", DataType.INTEGER, "n", DataType.LONG),
            List.of(List.of(Integer.MAX_VALUE, 1L)),
            List.of(2147483648L)
        );
    }

    public void testDoubleArithmetic() {
        Map<String, DataType> d = Map.of("d", DataType.DOUBLE);
        assertValues("d + 0.1", d, rows(0.2), List.of(0.30000000000000004));
        assertValues("d * 2", d, rows(1.5, -0.0), List.of(3.0, -0.0));
        assertValues("d / 0", d, rows(1.0), Arrays.asList((Object) null));
        assertValues("d + n", types("d", DataType.DOUBLE, "n", DataType.LONG), List.of(List.of(0.5, 1L)), List.of(1.5));
    }

    public void testComparisons() {
        Map<String, DataType> n = Map.of("n", DataType.LONG);
        assertValues("n > 0", n, rows(1L, 0L, null, List.of(1L, 2L)), Arrays.asList(true, false, null, null));
        assertValues("n == 0", n, rows(0L, 1L), List.of(true, false));
        assertValues("n != 0", n, rows(0L, 1L), List.of(false, true));
        assertValues("n <= 0", n, rows(0L, 1L), List.of(true, false));
        assertValues("n >= 1", n, rows(0L, 1L), List.of(false, true));
        assertValues("n < 1", n, rows(0L, 1L), List.of(true, false));
        assertValues("d == 0", Map.of("d", DataType.DOUBLE), rows(-0.0, 0.0), List.of(true, true));
    }

    public void testKeywordComparisons() {
        Map<String, DataType> k = Map.of("k", DataType.KEYWORD);
        assertValues("k == \"a\"", k, rows(bytes("a"), bytes("b"), List.of(bytes("a"), bytes("a"))), Arrays.asList(true, false, null));
        // Keywords compare as UTF-8 bytes
        assertValues("k < \"é\"", k, rows(bytes("z"), bytes("é")), List.of(true, false));
        assertValues("k > \"\"", k, rows(bytes(""), bytes("a")), List.of(false, true));
    }

    public void testDateComparisons() {
        assertValues(
            "t >= \"2026-01-01T00:00:00Z\"",
            Map.of("t", DataType.DATETIME),
            rows(1767225599999L, 1767225600000L),
            List.of(false, true)
        );
        assertValues(
            "t >= \"2026-01-01T00:00:00Z\"",
            Map.of("t", DataType.DATE_NANOS),
            rows(1767225599999999999L, 1767225600000000000L),
            List.of(false, true)
        );
    }

    public void testLogic() {
        Map<String, DataType> types = types("a", DataType.BOOLEAN, "b", DataType.BOOLEAN);
        List<List<Object>> rows = List.of(
            Arrays.asList(true, true),
            Arrays.asList(true, false),
            Arrays.asList(true, null),
            Arrays.asList(false, null),
            Arrays.asList(null, null)
        );
        assertValues("a AND b", types, rows, Arrays.asList(true, false, null, false, null));
        assertValues("a OR b", types, rows, Arrays.asList(true, true, true, null, null));
        assertValues("NOT a", types, rows, Arrays.asList(false, false, false, true, null));
    }

    public void testNulls() {
        Map<String, DataType> n = Map.of("n", DataType.LONG);
        assertValues("n IS NULL", n, rows(null, 1L, List.of(1L, 2L)), List.of(true, false, false));
        assertValues("n IS NOT NULL", n, rows(null, 1L, List.of(1L, 2L)), List.of(false, true, true));
        assertValues("COALESCE(n, 0)", n, rows(null, 5L, List.of(1L, 2L)), Arrays.asList(0L, 5L, List.of(1L, 2L)));
        assertValues(
            "CASE(b, n, -n)",
            types("b", DataType.BOOLEAN, "n", DataType.LONG),
            List.of(Arrays.asList(true, 5L), Arrays.asList(false, 5L), Arrays.asList(null, 5L)),
            List.of(5L, -5L, -5L)
        );
    }

    public void testMultiValues() {
        Map<String, DataType> n = Map.of("n", DataType.LONG);
        assertValues("MV_MIN(n)", n, rows(List.of(3L, 1L, 2L), 4L, null), Arrays.asList(1L, 4L, null));
        assertValues("MV_MAX(n)", n, rows(List.of(3L, 1L, 2L), 4L, null), Arrays.asList(3L, 4L, null));
        assertValues("MV_MAX(k) == \"b\"", Map.of("k", DataType.KEYWORD), rows(List.of(bytes("b"), bytes("a"))), List.of(true));
    }

    public void testDateTrunc() {
        Map<String, DataType> t = Map.of("t", DataType.DATETIME);
        long hour = 3_600_000L;
        assertValues("DATE_TRUNC(1 hour, t)", t, rows(hour + 1, hour - 1, -1L), List.of(hour, 0L, -hour));
        assertValues("DATE_TRUNC(15 minutes, t)", t, rows(16 * 60_000L), List.of(15 * 60_000L));
        // Calendar intervals in UTC: 2024-02-29T12:00:00Z, a leap day
        long leapDay = 1709208000000L;
        assertValues("DATE_TRUNC(1 day, t)", t, rows(leapDay), List.of(1709164800000L));
        assertValues("DATE_TRUNC(1 week, t)", t, rows(leapDay), List.of(1708905600000L));
        assertValues("DATE_TRUNC(1 month, t)", t, rows(leapDay), List.of(1706745600000L));
        assertValues("DATE_TRUNC(1 quarter, t)", t, rows(leapDay), List.of(1704067200000L));
        assertValues("DATE_TRUNC(1 year, t)", t, rows(leapDay), List.of(1704067200000L));
        assertValues("DATE_TRUNC(1 month, t)", t, rows(-1L), List.of(-2678400000L));
    }

    public void testDateTruncNanos() {
        Map<String, DataType> t = Map.of("t", DataType.DATE_NANOS);
        long hour = 3_600_000_000_000L;
        assertValues("DATE_TRUNC(1 hour, t)", t, rows(hour + 1, hour - 1), List.of(hour, 0L));
        // date_nanos can't hold buckets before 1970
        assertValues("DATE_TRUNC(1 hour, t)", t, rows(-1L), Arrays.asList((Object) null));
        assertValues("DATE_TRUNC(1 month, t)", t, rows(1709208000000000000L), List.of(1706745600000000000L));
    }

    private static Map<String, DataType> types(String f1, DataType t1, String f2, DataType t2) {
        Map<String, DataType> types = new LinkedHashMap<>();
        types.put(f1, t1);
        types.put(f2, t2);
        return types;
    }

    private static BytesRef bytes(String s) {
        return new BytesRef(s);
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
