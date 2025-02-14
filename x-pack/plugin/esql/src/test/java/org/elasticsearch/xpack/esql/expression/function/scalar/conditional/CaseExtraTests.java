/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Extra tests for {@code CASE} that don't fit into the parameterized
 * {@link CaseTests}.
 */
public class CaseExtraTests extends ESTestCase {
    public void testElseValueExplicit() {
        assertThat(
            new Case(
                Source.synthetic("case"),
                field("first_cond", DataType.BOOLEAN),
                List.of(field("v", DataType.LONG), field("e", DataType.LONG))
            ).children(),
            equalTo(List.of(field("first_cond", DataType.BOOLEAN), field("v", DataType.LONG), field("e", DataType.LONG)))
        );
    }

    public void testElseValueImplied() {
        assertThat(
            new Case(Source.synthetic("case"), field("first_cond", DataType.BOOLEAN), List.of(field("v", DataType.LONG))).children(),
            equalTo(List.of(field("first_cond", DataType.BOOLEAN), field("v", DataType.LONG)))
        );
    }

    public void testPartialFoldDropsFirstFalse() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last_cond", DataType.BOOLEAN), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(FoldContext.small()),
            equalTo(new Case(Source.synthetic("case"), field("last_cond", DataType.BOOLEAN), List.of(field("last", DataType.LONG))))
        );
    }

    public void testPartialFoldMv() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, List.of(true, true), DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last_cond", DataType.BOOLEAN), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(FoldContext.small()),
            equalTo(new Case(Source.synthetic("case"), field("last_cond", DataType.BOOLEAN), List.of(field("last", DataType.LONG))))
        );
    }

    public void testPartialFoldNoop() {
        Case c = new Case(
            Source.synthetic("case"),
            field("first_cond", DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(FoldContext.small()), sameInstance(c));
    }

    public void testPartialFoldFirst() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, true, DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(FoldContext.small()), equalTo(field("first", DataType.LONG)));
    }

    public void testPartialFoldFirstAfterKeepingUnknown() {
        Case c = new Case(
            Source.synthetic("case"),
            field("keep_me_cond", DataType.BOOLEAN),
            List.of(
                field("keep_me", DataType.LONG),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                field("first", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(FoldContext.small()),
            equalTo(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataType.BOOLEAN),
                    List.of(field("keep_me", DataType.LONG), field("first", DataType.LONG))
                )
            )
        );
    }

    public void testPartialFoldSecond() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                field("first", DataType.LONG),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                field("second", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(FoldContext.small()), equalTo(field("second", DataType.LONG)));
    }

    public void testPartialFoldSecondAfterDroppingFalse() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                field("first", DataType.LONG),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                field("second", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(FoldContext.small()), equalTo(field("second", DataType.LONG)));
    }

    public void testPartialFoldLast() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                field("first", DataType.LONG),
                new Literal(Source.EMPTY, false, DataType.BOOLEAN),
                field("second", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(FoldContext.small()), equalTo(field("last", DataType.LONG)));
    }

    public void testPartialFoldLastAfterKeepingUnknown() {
        Case c = new Case(
            Source.synthetic("case"),
            field("keep_me_cond", DataType.BOOLEAN),
            List.of(
                field("keep_me", DataType.LONG),
                new Literal(Source.EMPTY, false, DataType.BOOLEAN),
                field("first", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(FoldContext.small()),
            equalTo(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataType.BOOLEAN),
                    List.of(field("keep_me", DataType.LONG), field("last", DataType.LONG))
                )
            )
        );
    }

    public void testEvalCase() {
        testCase(caseExpr -> {
            DriverContext driverContext = driverContext();
            Page page = new Page(driverContext.blockFactory().newConstantIntBlockWith(0, 1));
            try (
                EvalOperator.ExpressionEvaluator eval = caseExpr.toEvaluator(AbstractFunctionTestCase.toEvaluator()).get(driverContext);
                Block block = eval.eval(page)
            ) {
                return toJavaObject(block, 0);
            } finally {
                page.releaseBlocks();
            }
        });
    }

    public void testFoldCase() {
        testCase(caseExpr -> {
            assertTrue(caseExpr.foldable());
            return caseExpr.fold(FoldContext.small());
        });
    }

    public void testCase(Function<Case, Object> toValue) {
        assertEquals(1, toValue.apply(caseExpr(true, 1)));
        assertNull(toValue.apply(caseExpr(false, 1)));
        assertEquals(2, toValue.apply(caseExpr(false, 1, 2)));
        assertEquals(1, toValue.apply(caseExpr(true, 1, true, 2)));
        assertEquals(2, toValue.apply(caseExpr(false, 1, true, 2)));
        assertNull(toValue.apply(caseExpr(false, 1, false, 2)));
        assertEquals(3, toValue.apply(caseExpr(false, 1, false, 2, 3)));
        assertNull(toValue.apply(caseExpr(true, null, 1)));
        assertEquals(1, toValue.apply(caseExpr(false, null, 1)));
        assertEquals(1, toValue.apply(caseExpr(false, field("ignored", DataType.INTEGER), 1)));
        assertEquals(1, toValue.apply(caseExpr(true, 1, field("ignored", DataType.INTEGER))));
    }

    public void testIgnoreLeadingNulls() {
        assertEquals(DataType.INTEGER, resolveType(false, null, 1));
        assertEquals(DataType.INTEGER, resolveType(false, null, false, null, false, 2, null));
        assertEquals(DataType.NULL, resolveType(false, null, null));
        assertEquals(DataType.BOOLEAN, resolveType(false, null, field("bool", DataType.BOOLEAN)));
    }

    public void testCaseWithInvalidCondition() {
        assertEquals("expected at least two arguments in [<case>] but got 1", resolveCase(1).message());
        assertEquals("first argument of [<case>] must be [boolean], found value [1] type [integer]", resolveCase(1, 2).message());
        assertEquals(
            "third argument of [<case>] must be [boolean], found value [3] type [integer]",
            resolveCase(true, 2, 3, 4, 5).message()
        );
    }

    public void testCaseWithIncompatibleTypes() {
        assertEquals("third argument of [<case>] must be [integer], found value [hi] type [keyword]", resolveCase(true, 1, "hi").message());
        assertEquals(
            "fourth argument of [<case>] must be [integer], found value [hi] type [keyword]",
            resolveCase(true, 1, false, "hi", 5).message()
        );
        assertEquals(
            "argument of [<case>] must be [integer], found value [hi] type [keyword]",
            resolveCase(true, 1, false, 2, true, 5, "hi").message()
        );
    }

    public void testCaseIsLazy() {
        Case caseExpr = caseExpr(true, 1, true, 2);
        DriverContext driveContext = driverContext();
        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public EvalOperator.ExpressionEvaluator.Factory apply(Expression expression) {
                Object value = expression.fold(FoldContext.small());
                if (value != null && value.equals(2)) {
                    return dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                        @Override
                        public Block eval(Page page) {
                            fail("Unexpected evaluation of 4th argument");
                            return null;
                        }

                        @Override
                        public void close() {}
                    };
                }
                return AbstractFunctionTestCase.evaluator(expression);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
        EvalOperator.ExpressionEvaluator evaluator = caseExpr.toEvaluator(toEvaluator).get(driveContext);
        Page page = new Page(driveContext.blockFactory().newConstantIntBlockWith(0, 1));
        try (Block block = evaluator.eval(page)) {
            assertEquals(1, toJavaObject(block, 0));
        } finally {
            page.releaseBlocks();
        }
    }

    private static Case caseExpr(Object... args) {
        List<Expression> exps = Stream.of(args).<Expression>map(arg -> {
            if (arg instanceof Expression e) {
                return e;
            }
            return new Literal(Source.synthetic(arg == null ? "null" : arg.toString()), arg, DataType.fromJava(arg));
        }).toList();
        return new Case(Source.synthetic("<case>"), exps.get(0), exps.subList(1, exps.size()));
    }

    private static Expression.TypeResolution resolveCase(Object... args) {
        return caseExpr(args).resolveType();
    }

    private static DataType resolveType(Object... args) {
        return caseExpr(args).dataType();
    }

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    protected final DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        CircuitBreaker breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        breakers.add(breaker);
        return new DriverContext(bigArrays, new BlockFactory(breaker, bigArrays));
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
