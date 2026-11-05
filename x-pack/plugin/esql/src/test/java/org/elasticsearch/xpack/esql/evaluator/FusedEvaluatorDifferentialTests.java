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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.FusableEvaluatorFactory;
import org.elasticsearch.compute.expression.FusedEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
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
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Length;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.planner.Layout;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Evaluates random expression trees both through the generated evaluators and through the
 * {@link FusedEvaluator} spun for them, and checks that the blocks and the warnings agree. The generated
 * evaluators are the specification; the spun code has to match them position by position, including
 * nulls, multi-values and the exceptions that turn into warnings.
 */
public class FusedEvaluatorDifferentialTests extends ESTestCase {
    private static final FieldAttribute INT_A = field("int_a", DataType.INTEGER);
    private static final FieldAttribute INT_B = field("int_b", DataType.INTEGER);
    private static final FieldAttribute INT_C = field("int_c", DataType.INTEGER);
    private static final FieldAttribute LONG_A = field("long_a", DataType.LONG);
    private static final FieldAttribute LONG_B = field("long_b", DataType.LONG);
    private static final FieldAttribute DOUBLE_A = field("double_a", DataType.DOUBLE);
    private static final FieldAttribute DOUBLE_B = field("double_b", DataType.DOUBLE);
    private static final FieldAttribute DATE = field("date", DataType.DATETIME);
    private static final FieldAttribute KEYWORD = field("keyword", DataType.KEYWORD);
    private static final List<FieldAttribute> FIELDS = List.of(INT_A, INT_B, INT_C, LONG_A, LONG_B, DOUBLE_A, DOUBLE_B, DATE, KEYWORD);
    private static final ElementType[] CHANNEL_TYPES = {
        ElementType.INT,
        ElementType.INT,
        ElementType.INT,
        ElementType.LONG,
        ElementType.LONG,
        ElementType.DOUBLE,
        ElementType.DOUBLE,
        ElementType.LONG,
        ElementType.BYTES_REF };

    private int nextLine = 1;

    public void testRandomArithmeticTrees() {
        for (int i = 0; i < 100; i++) {
            DataType type = randomFrom(DataType.INTEGER, DataType.LONG, DataType.DOUBLE);
            Expression expression = randomNumeric(type, 4);
            if (randomBoolean()) {
                expression = randomComparison(expression, randomNumeric(type, 2));
            }
            ExpressionEvaluator.Factory factory = EvalMapper.toEvaluator(FoldContext.small(), expression, layout());
            Optional<FusedEvaluator.Factory> fused = FusedEvaluator.Factory.tryFuse(factory);
            if (kernelCount(factory) < 2) {
                assertThat(factory.toString(), fused, equalTo(Optional.empty()));
                continue;
            }
            assertThat(factory.toString(), fused.isPresent(), equalTo(true));
            Page page = randomPage(randomIntBetween(0, 100));
            try {
                assertSameResult(factory, fused.get(), page);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * {@code DATE_TRUNC(1 hour, date + 1 hour)}: two kernels, both with fixed parameters.
     */
    public void testDateTruncOverDatetimeArithmetic() {
        Literal hour = new Literal(Source.EMPTY, Duration.ofHours(1), DataType.TIME_DURATION);
        Expression expression = new DateTrunc(
            source("date_trunc"),
            hour,
            new Add(source("add"), DATE, hour, EsqlTestUtils.TEST_CFG),
            EsqlTestUtils.TEST_CFG
        );
        ExpressionEvaluator.Factory factory = EvalMapper.toEvaluator(FoldContext.small(), expression, layout());
        assertThat(kernelCount(factory), equalTo(2));
        FusedEvaluator.Factory fused = FusedEvaluator.Factory.tryFuse(factory).orElseThrow();
        assertThat(fused.leaves(), hasSize(1));

        int positionCount = randomIntBetween(1, 100);
        Block[] blocks = new Block[CHANNEL_TYPES.length];
        for (int c = 0; c < blocks.length; c++) {
            if (c == 7) {
                LongBlock.Builder dates = TestBlockFactory.getNonBreakingInstance().newLongBlockBuilder(positionCount);
                for (int p = 0; p < positionCount; p++) {
                    switch (randomInt(3)) {
                        case 0 -> dates.appendNull();
                        case 1 -> dates.beginPositionEntry().appendLong(randomDate()).appendLong(randomDate()).endPositionEntry();
                        case 2, 3 -> dates.appendLong(randomDate());
                        default -> throw new AssertionError();
                    }
                }
                blocks[c] = dates.build();
            } else {
                blocks[c] = TestBlockFactory.getNonBreakingInstance().newConstantNullBlock(positionCount);
            }
        }
        Page page = new Page(positionCount, blocks);
        try {
            assertSameResult(factory, fused, page);
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * An input that isn't fusable, here {@code LENGTH} over a {@code BytesRef} block, stays a separate evaluator
     * whose block the fused tree reads.
     */
    public void testNonFusableInputStaysALeaf() {
        Expression expression = new Mul(
            source("mul"),
            new Add(source("add"), INT_A, new Length(source("length"), KEYWORD), EsqlTestUtils.TEST_CFG),
            INT_B
        );
        ExpressionEvaluator.Factory factory = EvalMapper.toEvaluator(FoldContext.small(), expression, layout());
        FusedEvaluator.Factory fused = FusedEvaluator.Factory.tryFuse(factory).orElseThrow();
        assertThat(fused.leaves(), hasSize(3));
        assertThat(fused.leaves().get(0), hasToString("Attribute[channel=0]"));
        assertThat(fused.leaves().get(1), not(instanceOf(FusableEvaluatorFactory.class)));
        assertThat(fused.leaves().get(1), hasToString("LengthEvaluator[val=Attribute[channel=8]]"));
        assertThat(fused.leaves().get(2), hasToString("Attribute[channel=1]"));
        assertThat(fused, hasToString(factory.toString()));

        Page page = randomPage(randomIntBetween(1, 100));
        try {
            assertSameResult(factory, fused, page);
        } finally {
            page.releaseBlocks();
        }
    }

    private static void assertSameResult(ExpressionEvaluator.Factory reference, ExpressionEvaluator.Factory fused, Page page) {
        DriverContext referenceContext = driverContext();
        DriverContext fusedContext = driverContext();
        try (
            ExpressionEvaluator referenceEvaluator = reference.get(referenceContext);
            ExpressionEvaluator fusedEvaluator = fused.get(fusedContext);
            Block expected = referenceEvaluator.eval(page);
            Block actual = fusedEvaluator.eval(page)
        ) {
            assertThat(fusedEvaluator, instanceOf(FusedEvaluator.class));
            assertThat(reference.toString(), actual, equalTo(expected));
        }
        referenceContext.finish();
        fusedContext.finish();
        Set<String> expectedWarnings = withoutArithmeticMessages(referenceContext.warnings());
        Set<String> actualWarnings = withoutArithmeticMessages(fusedContext.warnings());
        assertThat(reference.toString(), actualWarnings, equalTo(expectedWarnings));
    }

    /**
     * Once a compiled method has thrown enough overflow exceptions HotSpot throws a preallocated
     * {@link ArithmeticException} without a message ({@code -XX:+OmitStackTraceInFastThrow}), so whichever side of
     * the comparison is hotter can report {@code ArithmeticException: null} where the other reports
     * {@code ArithmeticException: integer overflow}. The message is the JVM's to drop either way.
     */
    private static Set<String> withoutArithmeticMessages(List<String> warnings) {
        Set<String> normalized = new HashSet<>();
        for (String warning : warnings) {
            int message = warning.indexOf("java.lang.ArithmeticException: ");
            normalized.add(message < 0 ? warning : warning.substring(0, message + "java.lang.ArithmeticException".length()));
        }
        return normalized;
    }

    private static DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, TestBlockFactory.getNonBreakingInstance(), null);
    }

    private static Page randomPage(int positionCount) {
        boolean allVectors = randomBoolean();
        Block[] blocks = new Block[CHANNEL_TYPES.length];
        for (int c = 0; c < blocks.length; c++) {
            if (CHANNEL_TYPES[c] == ElementType.BYTES_REF) {
                // LENGTH needs valid UTF-8, which random bytes aren't
                blocks[c] = randomKeywords(positionCount, allVectors);
                continue;
            }
            blocks[c] = RandomBlock.randomBlock(
                TestBlockFactory.getNonBreakingInstance(),
                CHANNEL_TYPES[c],
                positionCount,
                allVectors == false && randomBoolean(),
                1,
                allVectors ? 1 : randomIntBetween(1, 3),
                0,
                0
            ).block();
        }
        return new Page(positionCount, blocks);
    }

    private static BytesRefBlock randomKeywords(int positionCount, boolean allVectors) {
        try (BytesRefBlock.Builder builder = TestBlockFactory.getNonBreakingInstance().newBytesRefBlockBuilder(positionCount)) {
            for (int p = 0; p < positionCount; p++) {
                int values = allVectors ? 1 : randomIntBetween(0, 3);
                if (values == 0) {
                    builder.appendNull();
                    continue;
                }
                if (values > 1) {
                    builder.beginPositionEntry();
                }
                for (int v = 0; v < values; v++) {
                    builder.appendBytesRef(new BytesRef(randomAlphaOfLength(randomIntBetween(0, 10))));
                }
                if (values > 1) {
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    private static long randomDate() {
        return randomLongBetween(0, 4_000_000_000_000L);
    }

    private static int kernelCount(ExpressionEvaluator.Factory factory) {
        if (factory instanceof FusableEvaluatorFactory fusable) {
            int count = 1;
            for (ExpressionEvaluator.Factory input : fusable.inputs()) {
                count += kernelCount(input);
            }
            return count;
        }
        return 0;
    }

    private Expression randomNumeric(DataType type, int depth) {
        if (depth == 0 || randomInt(3) == 0) {
            return randomBoolean() ? randomField(type) : randomLiteral(type);
        }
        return switch (randomInt(6)) {
            case 0 -> new Add(source("add"), randomNumeric(type, depth - 1), randomNumeric(type, depth - 1), EsqlTestUtils.TEST_CFG);
            case 1 -> new Sub(source("sub"), randomNumeric(type, depth - 1), randomNumeric(type, depth - 1), EsqlTestUtils.TEST_CFG);
            case 2 -> new Mul(source("mul"), randomNumeric(type, depth - 1), randomNumeric(type, depth - 1));
            // a divisor is a field or a non-zero literal: a constant subtree folding to zero would be divided by at
            // planning time, which warns through the response headers rather than the driver
            case 3 -> new Div(source("div"), randomNumeric(type, depth - 1), randomNumeric(type, 0));
            case 4 -> new Mod(source("mod"), randomNumeric(type, depth - 1), randomNumeric(type, 0));
            case 5 -> new Neg(source("neg"), randomNumeric(type, depth - 1));
            case 6 -> new Abs(source("abs"), randomNumeric(type, depth - 1));
            default -> throw new AssertionError();
        };
    }

    private Expression randomComparison(Expression left, Expression right) {
        return switch (randomInt(2)) {
            case 0 -> new GreaterThan(source("gt"), left, right, null);
            case 1 -> new LessThan(source("lt"), left, right, null);
            case 2 -> new Equals(source("eq"), left, right, null);
            default -> throw new AssertionError();
        };
    }

    private static Expression randomField(DataType type) {
        return switch (type) {
            case INTEGER -> randomFrom(INT_A, INT_B, INT_C);
            case LONG -> randomFrom(LONG_A, LONG_B);
            case DOUBLE -> randomFrom(DOUBLE_A, DOUBLE_B);
            default -> throw new AssertionError(type);
        };
    }

    /**
     * Literals are non-zero so division by a constant doesn't fail at planning time.
     */
    private static Expression randomLiteral(DataType type) {
        return switch (type) {
            case INTEGER -> new Literal(Source.EMPTY, randomFrom(1, 2, 3, -7), DataType.INTEGER);
            case LONG -> new Literal(Source.EMPTY, randomFrom(1L, 2L, 3L, -7L), DataType.LONG);
            case DOUBLE -> new Literal(Source.EMPTY, randomFrom(1.0, 0.5, 2.5, -7.25), DataType.DOUBLE);
            default -> throw new AssertionError(type);
        };
    }

    private Source source(String text) {
        return new Source(nextLine++, 1, text);
    }

    private static Layout layout() {
        return new Layout.Builder().append(FIELDS).build();
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }
}
