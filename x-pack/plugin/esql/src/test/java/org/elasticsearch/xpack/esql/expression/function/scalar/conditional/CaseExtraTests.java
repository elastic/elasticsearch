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
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.parser.ExpressionBuilder;
import org.junit.After;

import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
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
            listCondition(true, true),
            List.of(field("first", DataType.LONG), field("last_cond", DataType.BOOLEAN), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(FoldContext.small()),
            equalToIgnoringIds(
                new Case(Source.synthetic("case"), field("last_cond", DataType.BOOLEAN), List.of(field("last", DataType.LONG)))
            )
        );
        // Dropping the condition here is what the evaluator would have warned about.
        assertMultivalueConditionWarnings();
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

    /**
     * Nested {@code CASE} used to recurse in {@link Case#fold(FoldContext)} until
     * the JVM threw {@link StackOverflowError}. The type is randomized: types with no
     * evaluator have always been folded by hand, the rest used to fold through one, and
     * both recursed.
     */
    public void testDeeplyNestedFoldDoesNotStackOverflow() {
        boolean nestInTrueBranch = randomBoolean();
        FoldedCaseValues values = randomFoldedCaseValues();
        Expression nested = nestCases(10_000, values.expected, values.unused, nestInTrueBranch);
        assertTrue(nested.foldable());
        assertThat(nested.fold(FoldContext.small()), equalTo(values.expected.value()));
    }

    /**
     * Folding no longer builds an evaluator, and the evaluator was what warned about a
     * multivalued condition, so {@link Case#fold(FoldContext)} has to raise those
     * warnings itself.
     */
    public void testNestedIntegerFoldKeepsMultivalueConditionWarnings() {
        int taken = randomInt();
        int unused = randomValueOtherThan(taken, ESTestCase::randomInt);
        Case inner = resolvedCase(booleanLiteral(true), intLiteral(taken), intLiteral(unused));
        Case outer = resolvedCase(listCondition(true, true), inner, intLiteral(unused));
        assertTrue(outer.foldable());
        assertThat(outer.fold(FoldContext.small()), equalTo(unused));
        assertMultivalueConditionWarnings();
    }

    /**
     * Same warning requirement with the nested {@code CASE} in the else branch, which
     * is the arm a multivalued condition falls through to.
     */
    public void testNestedIntegerFoldInElseKeepsMultivalueConditionWarnings() {
        int taken = randomInt();
        int unused = randomValueOtherThan(taken, ESTestCase::randomInt);
        Case inner = resolvedCase(booleanLiteral(true), intLiteral(taken), intLiteral(unused));
        Case outer = resolvedCase(listCondition(true, true), intLiteral(unused), inner);
        assertTrue(outer.foldable());
        assertThat(outer.fold(FoldContext.small()), equalTo(taken));
        assertMultivalueConditionWarnings();
    }

    /**
     * A one value list is single valued, so the branch is taken. The evaluator reads the
     * value out of the Block and never sees a list, so it is the oracle here.
     */
    public void testSingleValuedListConditionMatchesEvaluator() {
        boolean condition = randomBoolean();
        int taken = randomInt();
        int unused = randomValueOtherThan(taken, ESTestCase::randomInt);
        Case c = resolvedCase(listCondition(condition), intLiteral(taken), intLiteral(unused));
        int expected = condition ? taken : unused;
        assertTrue(c.foldable());
        assertThat(evaluate(c).value(), equalTo(expected));
        assertThat(c.fold(FoldContext.small()), equalTo(expected));
    }

    /**
     * {@code foldable} has to agree with {@code fold} about a one value list. If it says a CASE
     * is foldable when the branch {@code fold} takes is not, the optimizer folds an expression
     * that {@code fold} then cannot handle.
     */
    public void testSingleValuedListConditionFoldableMatchesTakenBranch() {
        int value = randomInt();
        assertFalse(resolvedCase(listCondition(true), field("f", DataType.INTEGER), intLiteral(value)).foldable());

        Case takesTheLiteral = resolvedCase(listCondition(true), intLiteral(value), field("f", DataType.INTEGER));
        assertTrue(takesTheLiteral.foldable());
        assertThat(takesTheLiteral.fold(FoldContext.small()), equalTo(value));
    }

    public void testPartialFoldSingleValuedListCondition() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, List.of(true), DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(FoldContext.small()), equalToIgnoringIds(field("first", DataType.LONG)));
    }

    public void testMultivaluedListConditionMatchesEvaluator() {
        int taken = randomInt();
        int unused = randomValueOtherThan(taken, ESTestCase::randomInt);
        Case c = resolvedCase(listCondition(true, true), intLiteral(taken), intLiteral(unused));
        assertThat(evaluate(c).value(), equalTo(unused));
        assertThat(c.fold(FoldContext.small()), equalTo(unused));
        assertMultivalueConditionWarnings();
    }

    /**
     * The evaluator and {@code fold} are two implementations of the same rules for a
     * condition, so sweep every shape a folded condition can take and require they agree
     * on the value and on the warnings. The evaluator is the oracle.
     * <p>
     *     An empty list is left out because there is no oracle for it: {@code
     *     BlockUtils.fromListRow} reads {@code listVal.get(0)}, so the evaluator cannot
     *     answer. {@code fold} treats it as false.
     * </p>
     */
    public void testFoldMatchesEvaluatorForEveryConditionShape() {
        for (Object shape : Arrays.asList(
            null,
            true,
            false,
            List.of(true),
            List.of(false),
            List.of(true, true),
            List.of(false, false),
            List.of(true, false),
            List.of(false, true)
        )) {
            int taken = randomInt();
            int unused = randomValueOtherThan(taken, ESTestCase::randomInt);
            Case c = resolvedCase(new Literal(Source.synthetic("cond"), shape, DataType.BOOLEAN), intLiteral(taken), intLiteral(unused));
            EvaluatedCase evaluated = evaluate(c);
            String message = "condition [" + shape + "]";
            assertThat(message, c.fold(FoldContext.small()), equalTo(evaluated.value()));
            assertWarnings(evaluated.warnings().toArray(String[]::new));
        }
    }

    /**
     * A {@code CASE} whose type has no evaluator, {@code DATE_PERIOD} here, has never warned
     * about a multivalued condition. {@link ESTestCase} fails the test if a warning is raised
     * and not asserted, so not asserting one is the assertion.
     */
    public void testFoldWithoutEvaluatorDoesNotWarnOnMultivalueCondition() {
        Period taken = Period.ofDays(randomIntBetween(1, 20));
        Period unused = randomValueOtherThan(taken, () -> Period.ofDays(randomIntBetween(1, 20)));
        Case c = resolvedCase(
            listCondition(true, true),
            new Literal(Source.EMPTY, taken, DataType.DATE_PERIOD),
            new Literal(Source.EMPTY, unused, DataType.DATE_PERIOD)
        );
        assertTrue(c.foldable());
        assertThat(c.fold(FoldContext.small()), equalTo(unused));
    }

    /**
     * Parser-depth nested {@code CASE} with mixed true/false branches and conditions that are
     * plain booleans, foldable non-literals such as {@code 123 == 123}, and one value lists.
     */
    public void testNestedFoldAtMaxExpressionDepthWithMixedConditions() {
        FoldedCaseValues values = randomFoldedCaseValues();
        Expression nested = nestCasesWithMixedConditions(values);
        assertTrue(nested.foldable());
        assertThat(nested.fold(FoldContext.small()), equalTo(values.expected.value()));
    }

    /**
     * One {@code CASE} holding several nested {@code CASE} values is the only shape where the
     * walk has to keep more than one node pending at a time. The nests elsewhere in this class
     * are chains, so they would pass even if the walk could only follow one node.
     * <p>
     *     Conditions are {@code 1 == 2} rather than literals so that nothing is short-circuited
     *     away and every arm has to be looked at. An unfoldable leaf is planted in one arm,
     *     chosen at random including the else arm, so dropping any pending node shows up.
     * </p>
     */
    public void testSeveralNestedCaseValuesUnderOneCase() {
        int value = randomInt();
        int arms = randomIntBetween(2, 5);

        Case allFoldable = caseOverNestedArms(value, arms, -1);
        assertTrue(allFoldable.foldable());
        assertThat(allFoldable.foldable(), equalTo(foldableRecursively(allFoldable)));

        Case oneArmUnfoldable = caseOverNestedArms(value, arms, randomIntBetween(0, arms));
        assertFalse(oneArmUnfoldable.foldable());
        assertThat(oneArmUnfoldable.foldable(), equalTo(foldableRecursively(oneArmUnfoldable)));
    }

    /**
     * {@code CASE(1 == 2, CASE(..), 1 == 2, CASE(..), .., CASE(..))}, where the arm at
     * {@code unfoldableArm} holds a field. Pass {@code arms} to plant it in the else arm, or
     * -1 to leave every arm foldable.
     */
    private static Case caseOverNestedArms(int value, int arms, int unfoldableArm) {
        List<Expression> rest = new ArrayList<>();
        rest.add(nestedArm(value, unfoldableArm == 0));
        for (int i = 1; i < arms; i++) {
            rest.add(randomEquals(false));
            rest.add(nestedArm(value, unfoldableArm == i));
        }
        rest.add(nestedArm(value, unfoldableArm == arms));
        return resolvedCase(randomEquals(false), rest.toArray(Expression[]::new));
    }

    private static Case nestedArm(int value, boolean unfoldable) {
        Expression arm = unfoldable ? field("f", DataType.INTEGER) : intLiteral(value);
        return resolvedCase(randomEquals(randomBoolean()), arm, intLiteral(value));
    }

    /**
     * Cross-check the iterative {@code fold} and {@code foldable} against the recursive
     * implementations they replaced, which are safe at this depth.
     */
    public void testNestedFoldMatchesRecursiveImplementation() {
        FoldedCaseValues values = randomFoldedCaseValues();
        Expression nested = nestCasesWithMixedConditions(values);
        assertThat(nested.foldable(), equalTo(foldableRecursively(nested)));
        assertThat(nested.fold(FoldContext.small()), equalTo(foldRecursively(nested, FoldContext.small())));
    }

    /**
     * An unfoldable leaf makes the whole nest unfoldable, so the recursive cross-check
     * covers the {@code false} answer too.
     */
    public void testNestedUnfoldableMatchesRecursiveImplementation() {
        FoldedCaseValues values = randomFoldedCaseValues();
        Expression nested = nestCasesWithMixedConditions(values);
        Case withField = resolvedCase(randomEquals(randomBoolean()), nested, field("f", nested.dataType()));
        assertFalse(withField.foldable());
        assertFalse(foldableRecursively(withField));
    }

    /**
     * Mirrors {@code Case#isTrue}, so the recursive references below differ from {@link Case}
     * only in how they walk the tree, which is what they are here to check. Whether this rule
     * is the right one is {@link #testFoldMatchesEvaluatorForEveryConditionShape}'s job, and it
     * uses the evaluator rather than a copy of it.
     */
    private static boolean isTrueCondition(Object value) {
        if (value instanceof List<?> values) {
            return values.size() == 1 && Boolean.TRUE.equals(values.getFirst());
        }
        return Boolean.TRUE.equals(value);
    }

    /**
     * A recursive {@code foldable}, in the shape {@link Case} had before it was made iterative.
     */
    private static boolean foldableRecursively(Expression expression) {
        if (expression instanceof Case c) {
            List<Expression> children = c.children();
            for (int i = 0; i + 1 < children.size(); i += 2) {
                Expression condition = children.get(i);
                if (condition.foldable() == false) {
                    return false;
                }
                if (condition instanceof Literal literal) {
                    if (isTrueCondition(literal.value())) {
                        return foldableRecursively(children.get(i + 1));
                    }
                    continue;
                }
                if (foldableRecursively(children.get(i + 1)) == false) {
                    return false;
                }
            }
            // An implicit else is a NULL literal, which is foldable.
            return children.size() % 2 == 0 || foldableRecursively(children.getLast());
        }
        return expression.foldable();
    }

    /**
     * A recursive {@code fold}, in the shape {@link Case} had before it was made iterative.
     */
    private static Object foldRecursively(Expression expression, FoldContext ctx) {
        if (expression instanceof Case c) {
            List<Expression> children = c.children();
            for (int i = 0; i + 1 < children.size(); i += 2) {
                if (isTrueCondition(children.get(i).fold(ctx))) {
                    return foldRecursively(children.get(i + 1), ctx);
                }
            }
            return children.size() % 2 == 1 ? foldRecursively(children.getLast(), ctx) : null;
        }
        return expression.fold(ctx);
    }

    private record EvaluatedCase(Object value, List<String> warnings) {}

    private EvaluatedCase evaluate(Case caseExpr) {
        DriverContext driverContext = driverContext();
        EvaluatorMapper.ToEvaluator toEvaluator = new EvaluatorMapper.ToEvaluator() {
            @Override
            public ExpressionEvaluator.Factory apply(Expression expression) {
                return AbstractFunctionTestCase.evaluator(expression);
            }

            @Override
            public FoldContext foldCtx() {
                return FoldContext.small();
            }
        };
        Page page = new Page(driverContext.blockFactory().newConstantIntBlockWith(0, 1));
        Object value;
        try (ExpressionEvaluator evaluator = caseExpr.toEvaluator(toEvaluator).get(driverContext); Block block = evaluator.eval(page)) {
            value = toJavaObject(block, 0);
        } finally {
            page.releaseBlocks();
        }
        driverContext.finish();
        return new EvaluatedCase(value, driverContext.warnings());
    }

    private void assertMultivalueConditionWarnings() {
        assertWarnings(
            "Line -1:-1: evaluation of [case] failed, treating result as false. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: single-value function encountered multi-value"
        );
    }

    private static Expression nestCasesWithMixedConditions(FoldedCaseValues values) {
        Expression nested = values.expected;
        for (int i = 0; i < ExpressionBuilder.MAX_EXPRESSION_DEPTH; i++) {
            boolean nestInTrueBranch = randomBoolean();
            Expression condition = switch (randomInt(2)) {
                case 0 -> booleanLiteral(nestInTrueBranch);
                case 1 -> randomEquals(nestInTrueBranch);
                // Single valued, so it picks a branch like a plain boolean and raises no warning.
                case 2 -> listCondition(nestInTrueBranch);
                default -> throw new AssertionError("randomInt(2) returns 0 to 2");
            };
            nested = nestInTrueBranch ? resolvedCase(condition, nested, values.unused) : resolvedCase(condition, values.unused, nested);
        }
        return nested;
    }

    /**
     * Types are resolved one node at a time so the nesting tests target {@code fold},
     * not type resolution.
     */
    private static Case resolvedCase(Expression condition, Expression... rest) {
        // A synthetic source so multivalue warnings, which name the CASE, have text to report.
        Case c = new Case(Source.synthetic("case"), condition, List.of(rest));
        c.dataType();
        return c;
    }

    private static Literal intLiteral(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static Literal booleanLiteral(boolean value) {
        return new Literal(Source.EMPTY, value, DataType.BOOLEAN);
    }

    private static Literal listCondition(Boolean... values) {
        return new Literal(Source.synthetic("cond"), List.of(values), DataType.BOOLEAN);
    }

    private record FoldedCaseValues(Literal expected, Literal unused) {}

    private static FoldedCaseValues randomFoldedCaseValues() {
        DataType type = randomFrom(DataType.INTEGER, DataType.DATE_PERIOD, DataType.TIME_DURATION);
        return switch (type) {
            case INTEGER -> {
                int expected = randomInt();
                int unused = randomValueOtherThan(expected, ESTestCase::randomInt);
                yield new FoldedCaseValues(new Literal(Source.EMPTY, expected, type), new Literal(Source.EMPTY, unused, type));
            }
            case DATE_PERIOD -> {
                Period expected = Period.ofDays(randomIntBetween(1, 20));
                Period unused = randomValueOtherThan(expected, () -> Period.ofDays(randomIntBetween(1, 20)));
                yield new FoldedCaseValues(new Literal(Source.EMPTY, expected, type), new Literal(Source.EMPTY, unused, type));
            }
            case TIME_DURATION -> {
                Duration expected = Duration.ofHours(randomIntBetween(1, 20));
                Duration unused = randomValueOtherThan(expected, () -> Duration.ofHours(randomIntBetween(1, 20)));
                yield new FoldedCaseValues(new Literal(Source.EMPTY, expected, type), new Literal(Source.EMPTY, unused, type));
            }
            default -> throw new AssertionError("unexpected type " + type);
        };
    }

    private static Equals randomEquals(boolean shouldMatch) {
        int left = randomInt();
        int right = shouldMatch ? left : randomValueOtherThan(left, ESTestCase::randomInt);
        return new Equals(
            Source.EMPTY,
            new Literal(Source.EMPTY, left, DataType.INTEGER),
            new Literal(Source.EMPTY, right, DataType.INTEGER)
        );
    }

    private static Expression nestCases(int depth, Expression leaf, Expression unused, boolean nestInTrueBranch) {
        Literal condition = booleanLiteral(nestInTrueBranch);
        Expression nested = leaf;
        for (int i = 0; i < depth; i++) {
            nested = nestInTrueBranch ? resolvedCase(condition, nested, unused) : resolvedCase(condition, unused, nested);
        }
        return nested;
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
        // Both conditions saw a multivalued row, but multivalue warnings name the enclosing CASE rather than the
        // condition, so the header and the exception line are each deduplicated across the two conditions.
        assertThat(driverContext.warnings(), hasSize(2));
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [<case>] failed, treating result as false")));
        assertThat(
            driverContext.warnings(),
            hasItem(containsString("java.lang.IllegalArgumentException: single-value function encountered multi-value"))
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
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [<case>] failed, treating result as false")));
        assertThat(
            driverContext.warnings(),
            hasItem(containsString("java.lang.IllegalArgumentException: single-value function encountered multi-value"))
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
        assertThat(driverContext.warnings(), hasItem(containsString("evaluation of [<case>] failed, treating result as false")));
        assertThat(
            driverContext.warnings(),
            hasItem(containsString("java.lang.IllegalArgumentException: single-value function encountered multi-value"))
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
