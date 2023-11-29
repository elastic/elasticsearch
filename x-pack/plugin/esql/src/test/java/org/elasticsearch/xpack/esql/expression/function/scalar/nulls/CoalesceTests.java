/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nulls;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.VaragsTestCaseBuilder;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.hamcrest.Matchers.equalTo;

public class CoalesceTests extends AbstractFunctionTestCase {
    public CoalesceTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    /**
     * Generate the test cases for this test. The tests don't actually include
     * any nulls, but we insert those nulls in {@link #testSimpleWithNulls()}.
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        VaragsTestCaseBuilder builder = new VaragsTestCaseBuilder(type -> "Coalesce");
        builder.expectString(strings -> strings.filter(v -> v != null).findFirst());
        builder.expectLong(longs -> longs.filter(v -> v != null).findFirst());
        builder.expectInt(ints -> ints.filter(v -> v != null).findFirst());
        builder.expectBoolean(booleans -> booleans.filter(v -> v != null).findFirst());
        return parameterSuppliersFromTypedData(builder.suppliers());
    }

    @Override
    protected void assertSimpleWithNulls(List<Object> data, Block value, int nullBlock) {
        for (int i = 0; i < data.size(); i++) {
            if (nullBlock == i) {
                continue;
            }
            Object v = data.get(i);
            if (v == null) {
                continue;
            }
            if (v instanceof List<?> l && l.size() == 1) {
                v = l.get(0);
            }
            assertThat(toJavaObject(value, 0), equalTo(v));
            return;
        }
        assertThat(value.isNull(0), equalTo(true));
    }

    @Override
    protected Coalesce build(Source source, List<Expression> args) {
        return new Coalesce(Source.EMPTY, args.get(0), args.subList(1, args.size()));
    }

    public void testCoalesceIsLazy() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        FieldAttribute evil = new FieldAttribute(Source.EMPTY, "evil", new EsField("evil", sub.get(0).dataType(), Map.of(), true));
        sub.add(evil);
        Coalesce exp = build(Source.EMPTY, sub);
        Layout.Builder builder = new Layout.Builder();
        buildLayout(builder, exp);
        Layout layout = builder.build();
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> map = child -> {
            if (child == evil) {
                return dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                    @Override
                    public Block eval(Page page) {
                        throw new AssertionError("shouldn't be called");
                    }

                    @Override
                    public void close() {}
                };
            }
            return EvalMapper.toEvaluator(child, layout);
        };
        try (
            EvalOperator.ExpressionEvaluator eval = exp.toEvaluator(map).get(driverContext());
            Block block = eval.eval(row(testCase.getDataValues()))
        ) {
            assertThat(toJavaObject(block, 0), testCase.getMatcher());
        }
    }

    public void testCoalesceNullabilityIsUnknown() {
        assertThat(buildFieldExpression(testCase).nullable(), equalTo(Nullability.UNKNOWN));
    }

    public void testCoalesceKnownNullable() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        sub.add(between(0, sub.size()), new Literal(Source.EMPTY, null, sub.get(0).dataType()));
        Coalesce exp = build(Source.EMPTY, sub);
        // Still UNKNOWN - if it were TRUE then an optimizer would replace it with null
        assertThat(exp.nullable(), equalTo(Nullability.UNKNOWN));
    }

    public void testCoalesceNotNullable() {
        List<Expression> sub = new ArrayList<>(testCase.getDataAsFields());
        sub.add(between(0, sub.size()), randomLiteral(sub.get(0).dataType()));
        Coalesce exp = build(Source.EMPTY, sub);
        // Known not to be nullable because it contains a non-null literal
        assertThat(exp.nullable(), equalTo(Nullability.FALSE));
    }
}
