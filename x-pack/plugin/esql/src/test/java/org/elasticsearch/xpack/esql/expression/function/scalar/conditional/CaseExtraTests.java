/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.expression.LoadFromPageEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.junit.After;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
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
            equalToIgnoringIds(List.of(field("first_cond", DataType.BOOLEAN), field("v", DataType.LONG), field("e", DataType.LONG)))
        );
    }

    public void testElseValueImplied() {
        assertThat(
            new Case(Source.synthetic("case"), field("first_cond", DataType.BOOLEAN), List.of(field("v", DataType.LONG))).children(),
            equalToIgnoringIds(List.of(field("first_cond", DataType.BOOLEAN), field("v", DataType.LONG)))
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
            equalToIgnoringIds(
                new Case(Source.synthetic("case"), field("last_cond", DataType.BOOLEAN), List.of(field("last", DataType.LONG)))
            )
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
            equalToIgnoringIds(
                new Case(Source.synthetic("case"), field("last_cond", DataType.BOOLEAN), List.of(field("last", DataType.LONG)))
            )
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
        assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(field("first", DataType.LONG)));
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
            equalToIgnoringIds(
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
        assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(field("second", DataType.LONG)));
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
        assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(field("second", DataType.LONG)));
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
        assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(field("last", DataType.LONG)));
    }

    public void testPartialFoldTrailingTextLeadingKeyword() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(field("keyword_field", DataType.KEYWORD), field("text_field", DataType.TEXT))
        );
        assertThat(c.dataType(), equalTo(DataType.KEYWORD));
        Expression result = c.partiallyFold(FoldContext.small());
        assertThat(result, equalToIgnoringIds(field("text_field", DataType.TEXT)));
    }

    public void testPartialFoldTrueConditionTextValue() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, true, DataType.BOOLEAN),
            List.of(field("text_field", DataType.TEXT))
        );
        assertThat(c.dataType(), equalTo(DataType.KEYWORD));
        Expression result = c.partiallyFold(FoldContext.small());
        assertThat(result, equalToIgnoringIds(field("text_field", DataType.TEXT)));
    }

    public void testPartialFoldTrailingKeywordLeadingText() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(field("text_field", DataType.TEXT), field("keyword_field", DataType.KEYWORD))
        );
        assertThat(c.dataType(), equalTo(DataType.KEYWORD));
        Expression result = c.partiallyFold(FoldContext.small());
        assertThat(result, equalToIgnoringIds(field("keyword_field", DataType.KEYWORD)));
    }

    public void testPartialFoldExplicitNull() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(field("username", DataType.KEYWORD), new Literal(Source.EMPTY, null, DataType.NULL))
        );
        assertThat(c.dataType(), equalTo(DataType.KEYWORD));
        Expression result = c.partiallyFold(FoldContext.small());
        assertThat("partiallyFold must preserve the Case's declared type, not null[NULL]", result.dataType(), equalTo(DataType.KEYWORD));
    }

    public void testPartialFoldAllFalseExplicitNull() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                new Literal(Source.EMPTY, BytesRefs.toBytesRef("a"), DataType.KEYWORD),
                new Literal(Source.EMPTY, false, DataType.BOOLEAN),
                new Literal(Source.EMPTY, BytesRefs.toBytesRef("b"), DataType.KEYWORD),
                new Literal(Source.EMPTY, null, DataType.NULL)
            )
        );
        assertThat(c.dataType(), equalTo(DataType.KEYWORD));
        Expression result = c.partiallyFold(FoldContext.small());
        assertThat("partiallyFold must preserve the Case's declared type, not null[NULL]", result.dataType(), equalTo(DataType.KEYWORD));
    }

    public void testPartialFoldTrueBranchWithNullValue() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                new Literal(Source.EMPTY, BytesRefs.toBytesRef("a"), DataType.KEYWORD),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                new Literal(Source.EMPTY, null, DataType.NULL),
                new Literal(Source.EMPTY, BytesRefs.toBytesRef("b"), DataType.KEYWORD)
            )
        );
        assertThat(c.dataType(), equalTo(DataType.KEYWORD));
        Expression result = c.partiallyFold(FoldContext.small());
        assertThat("partiallyFold must preserve the Case's declared type, not null[NULL]", result.dataType(), equalTo(DataType.KEYWORD));
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
            equalToIgnoringIds(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataType.BOOLEAN),
                    List.of(field("keep_me", DataType.LONG), field("last", DataType.LONG))
                )
            )
        );
    }

    /**
     * A CASE whose only non-else branch has a literally-FALSE condition is foldable iff the
     * else value is foldable — the dead branch's value is irrelevant because it will never
     * be evaluated. The {@code else { continue; }} in {@link Case#foldable()} is load-bearing
     * here: without it, control falls through to the "value must be foldable" check and
     * returns {@code false} for the non-foldable field in the dead branch.
     */
    public void testFoldableWhenDeadBranchHasNonFoldableValue() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(field("dead_value", DataType.LONG), new Literal(Source.EMPTY, 42L, DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(true));
        assertThat(c.fold(FoldContext.small()), equalTo(42L));
    }

    public void testEvalCase() {
        testCase(caseExpr -> {
            DriverContext driverContext = driverContext();
            Page page = new Page(driverContext.blockFactory().newConstantIntBlockWith(0, 1));
            try (
                ExpressionEvaluator eval = caseExpr.toEvaluator(AbstractFunctionTestCase.toEvaluator()).get(driverContext);
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

    public void testFoldCaseWithTemporalAmount() {
        // TIME_DURATION
        Duration oneDay = Duration.ofDays(1);
        Duration fiveDays = Duration.ofDays(5);
        Duration tenDays = Duration.ofDays(10);

        Case caseExprTrue = caseExprWithTemporalAmount(true, oneDay, tenDays, DataType.TIME_DURATION);
        assertTrue(caseExprTrue.foldable());
        assertEquals(oneDay, caseExprTrue.fold(FoldContext.small()));

        Case caseExprFalse = caseExprWithTemporalAmount(false, oneDay, tenDays, DataType.TIME_DURATION);
        assertTrue(caseExprFalse.foldable());
        assertEquals(tenDays, caseExprFalse.fold(FoldContext.small()));

        // DATE_PERIOD
        Period oneMonth = Period.ofMonths(1);
        Period oneYear = Period.ofYears(1);

        Case caseExprPeriodTrue = caseExprWithTemporalAmount(true, oneMonth, oneYear, DataType.DATE_PERIOD);
        assertTrue(caseExprPeriodTrue.foldable());
        assertEquals(oneMonth, caseExprPeriodTrue.fold(FoldContext.small()));

        Case caseExprPeriodFalse = caseExprWithTemporalAmount(false, oneMonth, oneYear, DataType.DATE_PERIOD);
        assertTrue(caseExprPeriodFalse.foldable());
        assertEquals(oneYear, caseExprPeriodFalse.fold(FoldContext.small()));

        // Test multiple conditions
        Case caseExprMulti = new Case(
            Source.EMPTY,
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                new Literal(Source.EMPTY, oneDay, DataType.TIME_DURATION),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                new Literal(Source.EMPTY, fiveDays, DataType.TIME_DURATION),
                new Literal(Source.EMPTY, tenDays, DataType.TIME_DURATION)
            )
        );
        assertTrue(caseExprMulti.foldable());
        assertEquals(fiveDays, caseExprMulti.fold(FoldContext.small()));

        // Test multiple conditions with else
        Case caseExprMultiElse = new Case(
            Source.EMPTY,
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                new Literal(Source.EMPTY, oneDay, DataType.TIME_DURATION),
                new Literal(Source.EMPTY, false, DataType.BOOLEAN),
                new Literal(Source.EMPTY, fiveDays, DataType.TIME_DURATION),
                new Literal(Source.EMPTY, tenDays, DataType.TIME_DURATION)
            )
        );
        assertTrue(caseExprMultiElse.foldable());
        assertEquals(tenDays, caseExprMultiElse.fold(FoldContext.small()));
    }

    private static Case caseExprWithTemporalAmount(boolean condition, Object trueValue, Object elseValue, DataType dataType) {
        return new Case(
            Source.EMPTY,
            new Literal(Source.EMPTY, condition, DataType.BOOLEAN),
            List.of(new Literal(Source.EMPTY, trueValue, dataType), new Literal(Source.EMPTY, elseValue, dataType))
        );
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
            public ExpressionEvaluator.Factory apply(Expression expression) {
                Object value = expression.fold(FoldContext.small());
                if (value != null && value.equals(2)) {
                    return dvrCtx -> new ExpressionEvaluator() {
                        @Override
                        public Block eval(Page page) {
                            fail("Unexpected evaluation of 4th argument");
                            return null;
                        }

                        @Override
                        public long baseRamBytesUsed() {
                            return 0;
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
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(toEvaluator).get(driveContext);
        Page page = new Page(driveContext.blockFactory().newConstantIntBlockWith(0, 1));
        try (Block block = evaluator.eval(page)) {
            assertEquals(1, toJavaObject(block, 0));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * {@code CASE(c1, v1, c2, v2, e)} over a multi-row page where the arms interleave. Every child is
     * forced through the non-eager-safe path, so {@code CASE} must filter the page before evaluating
     * anything but the first condition. Checks that each child only ever sees the rows it is allowed
     * to see, that the results land in the right rows, and that multivalued conditions warn.
     */
    public void testCaseIsLazyPerArm() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        // Channels: 0=c1, 1=v1, 2=c2, 3=v2, 4=e, 5=id
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of(), seen)).get(driverContext);
        Page page = new Page(
            booleans(blockFactory, true, false, null, List.of(true, false), false, true, false, null),
            longs(blockFactory, 100, 8),
            booleans(blockFactory, true, true, false, true, null, false, List.of(true, true), true),
            longs(blockFactory, 200, 8),
            longs(blockFactory, 300, 8),
            ids(blockFactory, 8)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            // Expected arms: v1 at {0, 5}; v2 at {1, 3, 7}; e at {2, 4, 6}
            assertLongs(block, 100, 201, 302, 203, 304, 105, 306, 207);
            assertThat(seen.seen("c1"), equalTo(Set.of(0, 1, 2, 3, 4, 5, 6, 7)));
            assertThat(seen.seen("v1"), equalTo(Set.of(0, 5)));
            assertThat(seen.seen("c2"), equalTo(Set.of(1, 2, 3, 4, 6, 7)));
            assertThat(seen.seen("v2"), equalTo(Set.of(1, 3, 7)));
            assertThat(seen.seen("e"), equalTo(Set.of(2, 4, 6)));
        } finally {
            page.releaseBlocks();
        }
        driverContext.finish();
        // Both conditions saw a multivalued row, so both emit a header. The exception line is deduplicated.
        assertThat(driverContext.warnings(), hasSize(3));
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [c1] failed, treating result as false")));
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [c2] failed, treating result as false")));
        assertThat(
            driverContext.warnings(),
            hasItem(containsString("java.lang.IllegalArgumentException: CASE expects a single-valued boolean"))
        );
    }

    /**
     * An eager-safe condition is evaluated on the whole page even after the first arm resolved some
     * rows. Multivalued results on those already-resolved rows must not warn, because the row never
     * reaches this condition.
     */
    public void testCaseEagerSafeConditionDoesNotWarnForSkippedRows() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of("c2"), seen)).get(driverContext);
        Page page = new Page(
            booleans(blockFactory, true, false, true, false),
            longs(blockFactory, 100, 4),
            // Multivalued only where c1 is true
            booleans(blockFactory, List.of(true, true), true, List.of(false, true), false),
            longs(blockFactory, 200, 4),
            longs(blockFactory, 300, 4),
            ids(blockFactory, 4)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertLongs(block, 100, 201, 102, 303);
            // Eager-safe, so c2 is evaluated on the whole page
            assertThat(seen.seen("c2"), equalTo(Set.of(0, 1, 2, 3)));
            assertThat(seen.seen("v2"), equalTo(Set.of(1)));
            assertThat(seen.seen("e"), equalTo(Set.of(3)));
        } finally {
            page.releaseBlocks();
        }
        driverContext.finish();
        assertThat(driverContext.warnings(), equalTo(List.of()));
    }

    /**
     * The counterpart of {@link #testCaseEagerSafeConditionDoesNotWarnForSkippedRows}: a multivalued
     * result on a row that does reach the eager-safe condition warns, and the row falls through.
     */
    public void testCaseEagerSafeConditionWarnsForReachedRows() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of("c2"), seen)).get(driverContext);
        Page page = new Page(
            booleans(blockFactory, true, false, true, false),
            longs(blockFactory, 100, 4),
            booleans(blockFactory, true, List.of(true, true), true, false),
            longs(blockFactory, 200, 4),
            longs(blockFactory, 300, 4),
            ids(blockFactory, 4)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertLongs(block, 100, 301, 102, 303);
            assertThat(seen.seen("v2"), nullValue());
            assertThat(seen.seen("e"), equalTo(Set.of(1, 3)));
        } finally {
            page.releaseBlocks();
        }
        driverContext.finish();
        assertThat(driverContext.warnings(), hasSize(2));
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [c2] failed, treating result as false")));
        assertThat(
            driverContext.warnings(),
            hasItem(containsString("java.lang.IllegalArgumentException: CASE expects a single-valued boolean"))
        );
    }

    /**
     * When every row picks the same arm, {@code CASE} hands back that arm's block itself instead of
     * copying it. With an eager-safe field load that is the page's own block.
     */
    public void testCaseAllRowsFirstArmReturnsValueBlock() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of("v1", "e"), seen))
            .get(driverContext);
        Page page = new Page(
            booleans(blockFactory, true, true, true, true),
            longs(blockFactory, 100, 4),
            booleans(blockFactory, false, false, false, false),
            longs(blockFactory, 200, 4),
            longs(blockFactory, 300, 4),
            ids(blockFactory, 4)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertThat(block, sameInstance(page.getBlock(1)));
            assertThat(seen.seen("c2"), nullValue());
            assertThat(seen.seen("e"), nullValue());
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * When no condition matches any row, the else arm covers the whole page and its block is returned
     * directly. The multivalued {@code c1} row still warns.
     */
    public void testCaseNoRowsMatchReturnsElseBlock() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of("v1", "e"), seen))
            .get(driverContext);
        Page page = new Page(
            booleans(blockFactory, false, null, false, List.of(true, true)),
            longs(blockFactory, 100, 4),
            booleans(blockFactory, false, false, null, false),
            longs(blockFactory, 200, 4),
            longs(blockFactory, 300, 4),
            ids(blockFactory, 4)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertThat(block, sameInstance(page.getBlock(4)));
            assertThat(seen.seen("c2"), equalTo(Set.of(0, 1, 2, 3)));
            assertThat(seen.seen("v1"), nullValue());
            assertThat(seen.seen("v2"), nullValue());
        } finally {
            page.releaseBlocks();
        }
        driverContext.finish();
        assertThat(driverContext.warnings(), hasSize(2));
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [c1] failed, treating result as false")));
        assertThat(
            driverContext.warnings(),
            hasItem(containsString("java.lang.IllegalArgumentException: CASE expects a single-valued boolean"))
        );
    }

    /**
     * A page without rows evaluates no child at all and produces an empty block.
     */
    public void testCaseEmptyPage() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of(), seen)).get(driverContext);
        Page page = new Page(
            0,
            booleans(blockFactory),
            longs(blockFactory, 100, 0),
            booleans(blockFactory),
            longs(blockFactory, 200, 0),
            longs(blockFactory, 300, 0),
            ids(blockFactory, 0)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertThat(block.getPositionCount(), equalTo(0));
            assertThat(seen.seen, equalTo(Map.of()));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Like {@link #testCaseAllRowsFirstArmReturnsValueBlock} but for an arm that isn't the first one:
     * once the first condition rejected every row, the second arm still sees the whole page and can
     * hand its block back directly.
     */
    public void testCaseAllRowsNonFirstArmReturnsValueBlock() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "c2", "v2", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "c2", 2, "v2", 3, "e", 4);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 5, Set.of("v2"), seen)).get(driverContext);
        Page page = new Page(
            booleans(blockFactory, false, false, false, false),
            longs(blockFactory, 100, 4),
            booleans(blockFactory, true, true, true, true),
            longs(blockFactory, 200, 4),
            longs(blockFactory, 300, 4),
            ids(blockFactory, 4)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertThat(block, sameInstance(page.getBlock(3)));
            assertThat(seen.seen("c2"), equalTo(Set.of(0, 1, 2, 3)));
            assertThat(seen.seen("v2"), equalTo(Set.of(0, 1, 2, 3)));
            assertThat(seen.seen("v1"), nullValue());
            assertThat(seen.seen("e"), nullValue());
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * A condition whose evaluator produces a {@code ConstantNullBlock}, which has no vector, is
     * treated as false for every row without warning.
     */
    public void testCaseConstantNullCondition() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "e");
        // Channels: 0=v1, 1=e, 2=id; c1 is computed
        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                String name = ((FieldAttribute) expression).name();
                ExpressionEvaluator.Factory delegate = switch (name) {
                    case "c1" -> new ComputedFromIdsFactory(2, (bf, id) -> bf.newConstantNullBlock(id.getPositionCount()));
                    case "v1" -> new LoadFromPageEvaluator.Factory(0);
                    case "e" -> new LoadFromPageEvaluator.Factory(1);
                    default -> throw new AssertionError("unexpected child [" + name + "]");
                };
                return new RecordingFactory(name, delegate, false, 2, seen);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(toEvaluator).get(driverContext);
        Page page = new Page(longs(blockFactory, 100, 4), longs(blockFactory, 300, 4), ids(blockFactory, 4));
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertLongs(block, 300, 301, 302, 303);
            assertThat(seen.seen("c1"), equalTo(Set.of(0, 1, 2, 3)));
            assertThat(seen.seen("v1"), nullValue());
            assertThat(seen.seen("e"), equalTo(Set.of(0, 1, 2, 3)));
        } finally {
            page.releaseBlocks();
        }
        driverContext.finish();
        assertThat(driverContext.warnings(), equalTo(List.of()));
    }

    /**
     * Arms of different concrete block classes are scattered into one result: {@code v1} is an
     * {@link OrdinalBytesRefBlock} read on the whole page, the else arm is a plain array block read
     * from a filtered page.
     */
    public void testCaseBytesRefArmsMixOrdinalsAndArrays() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = new Case(
            Source.synthetic("<case>"),
            field("c1", DataType.BOOLEAN),
            List.of(field("v1", DataType.KEYWORD), field("e", DataType.KEYWORD))
        );
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "e", 2);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 3, Set.of("v1"), seen)).get(driverContext);
        BytesRefBlock ordinals;
        try (var ordinalsBuilder = blockFactory.newIntBlockBuilder(6); var dictionaryBuilder = blockFactory.newBytesRefVectorBuilder(2)) {
            for (int o : new int[] { 0, 1, 0, 1, 0, 1 }) {
                ordinalsBuilder.appendInt(o);
            }
            dictionaryBuilder.appendBytesRef(new BytesRef("a"));
            dictionaryBuilder.appendBytesRef(new BytesRef("b"));
            ordinals = new OrdinalBytesRefBlock(ordinalsBuilder.build(), dictionaryBuilder.build());
        }
        Page page = new Page(
            booleans(blockFactory, true, false, true, false, true, false),
            ordinals,
            bytesRefs(blockFactory, "x", "y", "z", "w", "u", "v"),
            ids(blockFactory, 6)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertThat(block.getPositionCount(), equalTo(6));
            String[] expected = { "a", "y", "a", "w", "a", "v" };
            for (int p = 0; p < expected.length; p++) {
                assertThat("position " + p, toJavaObject(block, p), equalTo(new BytesRef(expected[p])));
            }
            assertThat(seen.seen("v1"), equalTo(Set.of(0, 1, 2, 3, 4, 5)));
            assertThat(seen.seen("e"), equalTo(Set.of(1, 3, 5)));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Multivalued and null values flow through both an arm evaluated on the whole page (eager-safe
     * {@code v1}) and an arm evaluated on a filtered page (the else value).
     */
    public void testCaseMultivaluedValueArms() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "e", 2);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 3, Set.of("v1"), seen)).get(driverContext);
        Page page = new Page(
            booleans(blockFactory, true, false, true, false, true, false),
            longs(blockFactory, List.of(1L, 2L), 10L, null, 10L, 3L, 10L),
            longs(blockFactory, 9L, List.of(7L, 8L), 9L, null, 9L, 42L),
            ids(blockFactory, 6)
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertThat(toJavaObject(block, 0), equalTo(List.of(1L, 2L)));
            assertThat(toJavaObject(block, 1), equalTo(List.of(7L, 8L)));
            assertThat(toJavaObject(block, 2), nullValue());
            assertThat(toJavaObject(block, 3), nullValue());
            assertThat(toJavaObject(block, 4), equalTo(3L));
            assertThat(toJavaObject(block, 5), equalTo(42L));
            assertThat(seen.seen("v1"), equalTo(Set.of(0, 1, 2, 3, 4, 5)));
            assertThat(seen.seen("e"), equalTo(Set.of(1, 3, 5)));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Pages coming out of Lucene carry a {@link DocVector} that holds references to the shards it
     * points at. Filtering such a page for a non-eager-safe arm creates and releases more of those
     * references; make sure they all balance out.
     */
    public void testCaseWithDocBlockInPage() {
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        Case caseExpr = caseOfFields("c1", "v1", "e");
        Map<String, Integer> channels = Map.of("c1", 0, "v1", 1, "e", 2);
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(new RecordingToEvaluator(channels, 3, Set.of(), seen)).get(driverContext);
        AtomicInteger shardRefs = new AtomicInteger();
        RefCounted shardRefCounter = new RefCounted() {
            @Override
            public void incRef() {
                shardRefs.incrementAndGet();
            }

            @Override
            public boolean tryIncRef() {
                shardRefs.incrementAndGet();
                return true;
            }

            @Override
            public boolean decRef() {
                shardRefs.decrementAndGet();
                return false;
            }

            @Override
            public boolean hasReferences() {
                return shardRefs.get() > 0;
            }
        };
        int rows = 4;
        DocVector docs = new DocVector(
            new IndexedByShardIdFromSingleton<>(shardRefCounter),
            blockFactory.newConstantIntVector(0, rows),
            blockFactory.newConstantIntVector(0, rows),
            range(blockFactory, rows),
            DocVector.config()
        );
        assertThat(shardRefs.get(), equalTo(1));
        Page page = new Page(
            booleans(blockFactory, true, false, true, false),
            longs(blockFactory, 100, rows),
            longs(blockFactory, 300, rows),
            ids(blockFactory, rows),
            docs.asBlock()
        );
        try (evaluator; Block block = evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            assertLongs(block, 100, 301, 102, 303);
            assertThat(seen.seen("v1"), equalTo(Set.of(0, 2)));
            assertThat(seen.seen("e"), equalTo(Set.of(1, 3)));
            // The filtered pages have been released, only the original page still references the shard
            assertThat(shardRefs.get(), equalTo(1));
        } finally {
            page.releaseBlocks();
        }
        assertThat(shardRefs.get(), equalTo(0));
    }

    /**
     * Many arms over many rows, with every arm selecting some rows. Condition {@code k} may only see
     * the rows that conditions before it did not resolve.
     */
    public void testCaseManyArms() {
        int arms = 40;
        int rows = 1024;
        DriverContext driverContext = driverContext();
        BlockFactory blockFactory = driverContext.blockFactory();
        Recorder seen = new Recorder();
        List<Expression> children = new ArrayList<>();
        for (int k = 0; k < arms - 1; k++) {
            children.add(field("c" + k, DataType.BOOLEAN));
            children.add(field("v" + k, DataType.LONG));
        }
        children.add(field("e", DataType.LONG));
        Case caseExpr = new Case(Source.synthetic("<case>"), children.get(0), children.subList(1, children.size()));
        // c<k> is true for the rows where id % arms == k, v<k> is k * 1000 + id, e is -id
        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                String name = ((FieldAttribute) expression).name();
                BiFunction<BlockFactory, IntBlock, Block> fn;
                if (name.equals("e")) {
                    fn = (bf, id) -> mapIds(bf, id, i -> (long) -i);
                } else {
                    int k = Integer.parseInt(name.substring(1));
                    fn = name.charAt(0) == 'c' ? (bf, id) -> idsModEqual(bf, id, arms, k) : (bf, id) -> mapIds(bf, id, i -> k * 1000L + i);
                }
                return new RecordingFactory(name, new ComputedFromIdsFactory(0, fn), false, 0, seen);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
        ExpressionEvaluator evaluator = caseExpr.toEvaluator(toEvaluator).get(driverContext);
        Page page = new Page(ids(blockFactory, rows));
        try (evaluator; LongBlock block = (LongBlock) evaluator.eval(page)) {
            seen.assertEvaluatedAtMostOnce();
            for (int r = 0; r < rows; r++) {
                int arm = r % arms;
                long expected = arm == arms - 1 ? -r : arm * 1000L + r;
                assertThat("row " + r, block.getLong(r), equalTo(expected));
            }
            for (int k = 0; k < arms - 1; k++) {
                Set<Integer> expectedCondition = new TreeSet<>();
                Set<Integer> expectedValue = new TreeSet<>();
                for (int r = 0; r < rows; r++) {
                    if (r % arms >= k) {
                        expectedCondition.add(r);
                    }
                    if (r % arms == k) {
                        expectedValue.add(r);
                    }
                }
                assertThat("condition " + k, seen.seen("c" + k), equalTo(expectedCondition));
                assertThat("value " + k, seen.seen("v" + k), equalTo(expectedValue));
            }
            Set<Integer> expectedElse = new TreeSet<>();
            for (int r = 0; r < rows; r++) {
                if (r % arms == arms - 1) {
                    expectedElse.add(r);
                }
            }
            assertThat(seen.seen("e"), equalTo(expectedElse));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Builds a {@code CASE} whose children are all {@link FieldAttribute}s with the given names.
     */
    private static Case caseOfFields(String first, String... rest) {
        List<Expression> children = new ArrayList<>();
        for (String name : rest) {
            children.add(field(name, name.startsWith("c") ? DataType.BOOLEAN : DataType.LONG));
        }
        return new Case(Source.synthetic("<case>"), field(first, DataType.BOOLEAN), children);
    }

    /**
     * Records, per child evaluator name, which rows it was asked to evaluate and how often it was
     * called. Rows are identified by the value in the page's id channel so this works on filtered
     * pages too. Arm-at-a-time evaluation must call each child at most once per page.
     */
    private static class Recorder {
        final Map<String, Set<Integer>> seen = new HashMap<>();
        final Map<String, Integer> calls = new HashMap<>();

        void record(String name, IntBlock ids) {
            calls.merge(name, 1, Integer::sum);
            Set<Integer> rows = seen.computeIfAbsent(name, k -> new TreeSet<>());
            for (int p = 0; p < ids.getPositionCount(); p++) {
                rows.add(ids.getInt(ids.getFirstValueIndex(p)));
            }
        }

        Set<Integer> seen(String name) {
            return seen.get(name);
        }

        void assertEvaluatedAtMostOnce() {
            for (Map.Entry<String, Integer> e : calls.entrySet()) {
                assertThat("evaluations of [" + e.getKey() + "]", e.getValue(), lessThanOrEqualTo(1));
            }
        }
    }

    /**
     * Maps each {@link FieldAttribute} child of a {@code CASE} by name to a channel of the page and wraps
     * the evaluator so the test can see which rows each child was asked to evaluate. Rows are identified
     * by the value in {@code idChannel} so recording works on filtered pages too. Only the names in
     * {@code eagerSafe} report {@link ExpressionEvaluator.Factory#eagerEvalSafeInLazy()}; every other
     * child forces {@code CASE} to filter the page down before evaluating it.
     */
    private record RecordingToEvaluator(Map<String, Integer> channels, int idChannel, Set<String> eagerSafe, Recorder seen)
        implements
            EvaluatorMapper.ToEvaluator {
        @Override
        public ExpressionEvaluator.Factory apply(Expression expression) {
            String name = ((FieldAttribute) expression).name();
            return new RecordingFactory(
                name,
                new LoadFromPageEvaluator.Factory(channels.get(name)),
                eagerSafe.contains(name),
                idChannel,
                seen
            );
        }

        @Override
        public FoldContext foldCtx() {
            return FoldContext.small();
        }
    }

    /**
     * Wraps a factory so every page its evaluator sees is recorded, by row id, into {@code seen}.
     */
    private record RecordingFactory(String name, ExpressionEvaluator.Factory delegate, boolean eagerSafe, int idChannel, Recorder seen)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            ExpressionEvaluator delegateEvaluator = delegate.get(context);
            return new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    seen.record(name, page.getBlock(idChannel));
                    return delegateEvaluator.eval(page);
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {
                    delegateEvaluator.close();
                }
            };
        }

        @Override
        public boolean eagerEvalSafeInLazy() {
            return eagerSafe;
        }

        @Override
        public String toString() {
            return "Recording[" + name + "]";
        }
    }

    /**
     * An evaluator that computes its result from the row ids in {@code idChannel}, so tests with many
     * arms don't need a block per arm.
     */
    private record ComputedFromIdsFactory(int idChannel, BiFunction<BlockFactory, IntBlock, Block> fn)
        implements
            ExpressionEvaluator.Factory {
        @Override
        public ExpressionEvaluator get(DriverContext context) {
            return new ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    return fn.apply(context.blockFactory(), page.getBlock(idChannel));
                }

                @Override
                public long baseRamBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            };
        }
    }

    private static Block idsModEqual(BlockFactory blockFactory, IntBlock ids, int mod, int k) {
        try (var builder = blockFactory.newBooleanVectorFixedBuilder(ids.getPositionCount())) {
            for (int p = 0; p < ids.getPositionCount(); p++) {
                builder.appendBoolean(ids.getInt(ids.getFirstValueIndex(p)) % mod == k);
            }
            return builder.build().asBlock();
        }
    }

    private static Block mapIds(BlockFactory blockFactory, IntBlock ids, Function<Integer, Long> fn) {
        try (var builder = blockFactory.newLongVectorFixedBuilder(ids.getPositionCount())) {
            for (int p = 0; p < ids.getPositionCount(); p++) {
                builder.appendLong(fn.apply(ids.getInt(ids.getFirstValueIndex(p))));
            }
            return builder.build().asBlock();
        }
    }

    /**
     * Boolean block from {@code null}, {@link Boolean} or {@code List<Boolean>} (multivalued) entries.
     */
    private static BooleanBlock booleans(BlockFactory blockFactory, Object... values) {
        try (var builder = blockFactory.newBooleanBlockBuilder(values.length)) {
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else if (v instanceof Boolean b) {
                    builder.appendBoolean(b);
                } else {
                    builder.beginPositionEntry();
                    for (Object o : (List<?>) v) {
                        builder.appendBoolean((Boolean) o);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * Long block from {@code null}, {@link Long} or {@code List<Long>} (multivalued) entries.
     */
    private static LongBlock longs(BlockFactory blockFactory, Object... values) {
        try (var builder = blockFactory.newLongBlockBuilder(values.length)) {
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else if (v instanceof Long l) {
                    builder.appendLong(l);
                } else {
                    builder.beginPositionEntry();
                    for (Object o : (List<?>) v) {
                        builder.appendLong((Long) o);
                    }
                    builder.endPositionEntry();
                }
            }
            return builder.build();
        }
    }

    /**
     * Long block with {@code base + p} at every position so results can be traced back to an arm.
     */
    private static LongBlock longs(BlockFactory blockFactory, long base, int count) {
        try (var builder = blockFactory.newLongVectorFixedBuilder(count)) {
            for (int p = 0; p < count; p++) {
                builder.appendLong(base + p);
            }
            return builder.build().asBlock();
        }
    }

    private static BytesRefBlock bytesRefs(BlockFactory blockFactory, String... values) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(values.length)) {
            for (String v : values) {
                builder.appendBytesRef(new BytesRef(v));
            }
            return builder.build();
        }
    }

    private static IntBlock ids(BlockFactory blockFactory, int count) {
        return range(blockFactory, count).asBlock();
    }

    private static IntVector range(BlockFactory blockFactory, int count) {
        try (var builder = blockFactory.newIntVectorFixedBuilder(count)) {
            for (int p = 0; p < count; p++) {
                builder.appendInt(p);
            }
            return builder.build();
        }
    }

    private static void assertLongs(Block block, long... expected) {
        assertThat(block.getPositionCount(), equalTo(expected.length));
        LongBlock longs = (LongBlock) block;
        for (int p = 0; p < expected.length; p++) {
            assertThat("position " + p, longs.getLong(longs.getFirstValueIndex(p)), equalTo(expected[p]));
        }
    }

    private static Case caseExpr(Object... args) {
        List<Expression> exps = Stream.of(args).<Expression>map(arg -> {
            if (arg instanceof Expression e) {
                return e;
            }
            DataType dataType = DataType.fromJava(arg);
            if (arg instanceof String) {
                arg = BytesRefs.toBytesRef(arg);
            }
            return new Literal(Source.synthetic(arg == null ? "null" : BytesRefs.toString(arg)), arg, dataType);
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
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
