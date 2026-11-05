/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.expression.EvaluatorKernel;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTruncDatetimeEvaluator;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.LengthEvaluator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.AddIntsEvaluator;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.MulIntsEvaluator;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Checks the kernel metadata the code generator attaches to evaluators whose per-row function is simple
 * enough to be fused: the {@code KERNEL} descriptor matches the {@code process} forwarder, and the generated
 * {@code Factory} exposes its structure through {@link FusableEvaluatorFactory}.
 */
public class FusableEvaluatorFactoryTests extends ESTestCase {
    private static final FieldAttribute A = field("a", DataType.INTEGER);
    private static final FieldAttribute B = field("b", DataType.INTEGER);
    private static final FieldAttribute C = field("c", DataType.INTEGER);
    private static final FieldAttribute DATE = field("date", DataType.DATETIME);
    private static final FieldAttribute KEYWORD = field("keyword", DataType.KEYWORD);

    public void testKernelDescribesForwarder() throws Throwable {
        EvaluatorKernel kernel = MulIntsEvaluator.KERNEL;
        assertThat(kernel.evaluatorClass(), equalTo(MulIntsEvaluator.class));
        assertThat(kernel.resultType(), equalTo(int.class));
        assertThat(kernel.parameters(), contains(EvaluatorKernel.value(int.class), EvaluatorKernel.value(int.class)));
        assertThat(kernel.warnExceptions(), contains(ArithmeticException.class));

        int lhs = randomIntBetween(-1000, 1000);
        int rhs = randomIntBetween(-1000, 1000);
        assertThat((int) kernel.methodHandle().invokeExact(lhs, rhs), equalTo(lhs * rhs));
        // invokeExact is signature polymorphic: the cast is what makes the call site's type match the kernel's
        expectThrows(ArithmeticException.class, () -> { int ignored = (int) kernel.methodHandle().invokeExact(Integer.MAX_VALUE, 2); });
    }

    public void testFactoryExposesStructure() {
        Source source = new Source(1, 7, "a * b");
        ExpressionEvaluator.Factory factory = toEvaluator(new Mul(source, A, B));
        assertFactoryOf(MulIntsEvaluator.class, factory);
        FusableEvaluatorFactory fusable = (FusableEvaluatorFactory) factory;
        assertThat(fusable.kernel(), sameInstance(MulIntsEvaluator.KERNEL));
        assertThat(fusable.inputs(), contains(hasToString("Attribute[channel=0]"), hasToString("Attribute[channel=1]")));
        assertThat(fusable.fixedValues(), empty());
        assertThat(fusable.source(), sameInstance(source));
    }

    public void testNestedFactoriesAreFusable() {
        ExpressionEvaluator.Factory factory = toEvaluator(new Mul(Source.EMPTY, new Add(Source.EMPTY, A, B, EsqlTestUtils.TEST_CFG), C));
        FusableEvaluatorFactory mul = (FusableEvaluatorFactory) factory;
        assertThat(mul.kernel(), sameInstance(MulIntsEvaluator.KERNEL));
        assertThat(mul.inputs(), hasSize(2));
        FusableEvaluatorFactory add = (FusableEvaluatorFactory) mul.inputs().get(0);
        assertThat(add.kernel(), sameInstance(AddIntsEvaluator.KERNEL));
        assertThat(add.inputs(), contains(hasToString("Attribute[channel=0]"), hasToString("Attribute[channel=1]")));
        assertThat(mul.inputs().get(1), hasToString("Attribute[channel=2]"));
    }

    public void testFixedParameter() throws Throwable {
        Literal interval = new Literal(Source.EMPTY, Duration.ofHours(1), DataType.TIME_DURATION);
        ExpressionEvaluator.Factory factory = toEvaluator(new DateTrunc(Source.EMPTY, interval, DATE, EsqlTestUtils.TEST_CFG));
        assertFactoryOf(DateTruncDatetimeEvaluator.class, factory);
        FusableEvaluatorFactory fusable = (FusableEvaluatorFactory) factory;

        EvaluatorKernel kernel = fusable.kernel();
        assertThat(kernel.parameters(), contains(EvaluatorKernel.value(long.class), EvaluatorKernel.fixed(Rounding.Prepared.class)));
        assertThat(kernel.valueParameterCount(), equalTo(1));
        assertThat(fusable.inputs(), contains(hasToString("Attribute[channel=3]")));
        assertThat(fusable.fixedValues(), hasSize(1));
        Rounding.Prepared rounding = (Rounding.Prepared) fusable.fixedValues().get(0);

        long timestamp = randomLongBetween(0, 4_000_000_000_000L);
        assertThat((long) kernel.methodHandle().invokeExact(timestamp, rounding), equalTo(rounding.round(timestamp)));
    }

    public void testBytesRefEvaluatorIsNotFusable() {
        ExpressionEvaluator.Factory factory = toEvaluator(new Length(Source.EMPTY, KEYWORD));
        assertFactoryOf(LengthEvaluator.class, factory);
        assertThat(factory, not(instanceOf(FusableEvaluatorFactory.class)));
        expectThrows(NoSuchFieldException.class, () -> LengthEvaluator.class.getField("KERNEL"));
    }

    /**
     * The generated {@code Factory} classes are package private, so they are matched by name.
     */
    private static void assertFactoryOf(Class<? extends ExpressionEvaluator> evaluator, ExpressionEvaluator.Factory factory) {
        assertThat(factory.getClass().getName(), equalTo(evaluator.getName() + "$Factory"));
    }

    private static ExpressionEvaluator.Factory toEvaluator(Expression expression) {
        Layout layout = new Layout.Builder().append(List.of(A, B, C, DATE, KEYWORD)).build();
        return EvalMapper.toEvaluator(FoldContext.small(), expression, layout);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
