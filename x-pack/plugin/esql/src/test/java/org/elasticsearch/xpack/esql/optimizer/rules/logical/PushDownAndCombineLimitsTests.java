/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizerTests.relation;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class PushDownAndCombineLimitsTests extends AbstractLogicalPlanOptimizerTests {

    private static class PushDownLimitTestCase<PlanType extends LogicalPlan> {
        private final Class<PlanType> clazz;
        private final BiFunction<LogicalPlan, Attribute, PlanType> planBuilder;
        private final BiConsumer<PlanType, PlanType> planChecker;

        PushDownLimitTestCase(
            Class<PlanType> clazz,
            BiFunction<LogicalPlan, Attribute, PlanType> planBuilder,
            BiConsumer<PlanType, PlanType> planChecker
        ) {
            this.clazz = clazz;
            this.planBuilder = planBuilder;
            this.planChecker = planChecker;
        }

        public PlanType buildPlan(LogicalPlan child, Attribute attr) {
            return planBuilder.apply(child, attr);
        }

        public void checkOptimizedPlan(LogicalPlan basePlan, LogicalPlan optimizedPlan) {
            planChecker.accept(as(basePlan, clazz), as(optimizedPlan, clazz));
        }
    }

    private static final List<PushDownLimitTestCase<? extends UnaryPlan>> PUSHABLE_LIMIT_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Eval.class,
            (plan, attr) -> new Eval(EMPTY, plan, List.of(new Alias(EMPTY, "y", new ToInteger(EMPTY, attr)))),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.fields(), optimizedPlan.fields());
            }
        ),
        new PushDownLimitTestCase<>(
            Completion.class,
            (plan, attr) -> new Completion(EMPTY, plan, randomLiteral(KEYWORD), randomLiteral(KEYWORD), attr),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.inferenceId(), optimizedPlan.inferenceId());
                assertEquals(basePlan.prompt(), optimizedPlan.prompt());
                assertEquals(basePlan.targetField(), optimizedPlan.targetField());
            }
        ),
        new PushDownLimitTestCase<>(
            Rerank.class,
            (plan, attr) -> new Rerank(
                EMPTY,
                plan,
                randomLiteral(KEYWORD),
                randomLiteral(KEYWORD),
                randomList(1, 10, () -> new Alias(EMPTY, randomIdentifier(), randomLiteral(KEYWORD))),
                attr
            ),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.inferenceId(), optimizedPlan.inferenceId());
                assertEquals(basePlan.queryText(), optimizedPlan.queryText());
                assertEquals(basePlan.rerankFields(), optimizedPlan.rerankFields());
                assertEquals(basePlan.scoreAttribute(), optimizedPlan.scoreAttribute());
            }
        ),
        new PushDownLimitTestCase<>(
            Enrich.class,
            (plan, attr) -> new Enrich(
                EMPTY,
                plan,
                randomFrom(Enrich.Mode.ANY, Enrich.Mode.COORDINATOR),
                randomLiteral(KEYWORD),
                attr,
                null,
                Map.of(),
                List.of()
            ),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.mode(), optimizedPlan.mode());
                assertEquals(basePlan.policyName(), optimizedPlan.policyName());
                assertEquals(basePlan.matchField(), optimizedPlan.matchField());
            }
        )
    );

    private static final List<PushDownLimitTestCase<? extends UnaryPlan>> NON_PUSHABLE_LIMIT_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Filter.class,
            (plan, attr) -> new Filter(EMPTY, plan, new Equals(EMPTY, attr, new Literal(EMPTY, BytesRefs.toBytesRef("right"), TEXT))),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.condition(), optimizedPlan.condition());
            }
        ),
        new PushDownLimitTestCase<>(
            OrderBy.class,
            (plan, attr) -> new OrderBy(EMPTY, plan, List.of(new Order(EMPTY, attr, Order.OrderDirection.DESC, null))),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.order(), optimizedPlan.order());
            }
        )
    );

    public void testPushableLimit() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation().withAttributes(List.of(a, b));

        for (PushDownLimitTestCase<? extends UnaryPlan> pushableLimitTestCase : PUSHABLE_LIMIT_TEST_CASES) {
            int precedingLimitValue = randomIntBetween(1, 10_000);
            Limit precedingLimit = new Limit(EMPTY, new Literal(EMPTY, precedingLimitValue, INTEGER), relation);

            LogicalPlan pushableLimitTestPlan = pushableLimitTestCase.buildPlan(precedingLimit, a);

            int pushableLimitValue = randomIntBetween(1, 10_000);
            Limit pushableLimit = new Limit(EMPTY, new Literal(EMPTY, pushableLimitValue, INTEGER), pushableLimitTestPlan);

            LogicalPlan optimizedPlan = optimizePlan(pushableLimit);

            pushableLimitTestCase.checkOptimizedPlan(pushableLimitTestPlan, optimizedPlan);

            assertEquals(
                as(optimizedPlan, UnaryPlan.class).child(),
                new Limit(EMPTY, new Literal(EMPTY, Math.min(pushableLimitValue, precedingLimitValue), INTEGER), relation)
            );
        }
    }

    public void testNonPushableLimit() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation().withAttributes(List.of(a, b));

        for (PushDownLimitTestCase<? extends UnaryPlan> nonPushableLimitTestCase : NON_PUSHABLE_LIMIT_TEST_CASES) {
            int precedingLimitValue = randomIntBetween(1, 10_000);
            Limit precedingLimit = new Limit(EMPTY, new Literal(EMPTY, precedingLimitValue, INTEGER), relation);
            UnaryPlan nonPushableLimitTestPlan = nonPushableLimitTestCase.buildPlan(precedingLimit, a);
            int nonPushableLimitValue = randomIntBetween(1, 10_000);
            Limit nonPushableLimit = new Limit(EMPTY, new Literal(EMPTY, nonPushableLimitValue, INTEGER), nonPushableLimitTestPlan);
            Limit optimizedPlan = as(optimizePlan(nonPushableLimit), Limit.class);
            nonPushableLimitTestCase.checkOptimizedPlan(nonPushableLimitTestPlan, optimizedPlan.child());
            assertEquals(
                optimizedPlan,
                new Limit(
                    EMPTY,
                    new Literal(EMPTY, Math.min(nonPushableLimitValue, precedingLimitValue), INTEGER),
                    nonPushableLimitTestPlan
                )
            );
            assertEquals(as(optimizedPlan.child(), UnaryPlan.class).child(), nonPushableLimitTestPlan.child());
        }
    }

    private static final List<PushDownLimitTestCase<? extends LogicalPlan>> DUPLICATING_TEST_CASES = List.of(
        new PushDownLimitTestCase<>(
            Enrich.class,
            (plan, attr) -> new Enrich(EMPTY, plan, Enrich.Mode.REMOTE, randomLiteral(KEYWORD), attr, null, Map.of(), List.of()),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                assertEquals(basePlan.mode(), optimizedPlan.mode());
                assertEquals(basePlan.policyName(), optimizedPlan.policyName());
                assertEquals(basePlan.matchField(), optimizedPlan.matchField());
                var limit = as(optimizedPlan.child(), Limit.class);
                assertTrue(limit.local());
                assertFalse(limit.duplicated());
            }
        ),
        new PushDownLimitTestCase<>(MvExpand.class, (plan, attr) -> new MvExpand(EMPTY, plan, attr, attr), (basePlan, optimizedPlan) -> {
            assertEquals(basePlan.source(), optimizedPlan.source());
            assertEquals(basePlan.expanded(), optimizedPlan.expanded());
            var limit = as(optimizedPlan.child(), Limit.class);
            assertFalse(limit.local());
            assertFalse(limit.duplicated());
        }),
        new PushDownLimitTestCase<>(
            Join.class,
            (plan, attr) -> new Join(EMPTY, plan, plan, new JoinConfig(JoinTypes.LEFT, List.of(), List.of(), attr)),
            (basePlan, optimizedPlan) -> {
                assertEquals(basePlan.source(), optimizedPlan.source());
                var limit = as(optimizedPlan.left(), Limit.class);
                assertFalse(limit.local());
                assertFalse(limit.duplicated());
            }
        )

    );

    public void testPushableLimitDuplicate() {
        FieldAttribute a = getFieldAttribute("a");
        FieldAttribute b = getFieldAttribute("b");
        EsRelation relation = relation().withAttributes(List.of(a, b));

        for (PushDownLimitTestCase<? extends LogicalPlan> duplicatingTestCase : DUPLICATING_TEST_CASES) {
            int precedingLimitValue = randomIntBetween(1, 10_000);
            Limit precedingLimit = new Limit(EMPTY, new Literal(EMPTY, precedingLimitValue, INTEGER), relation);
            LogicalPlan duplicatingLimitTestPlan = duplicatingTestCase.buildPlan(precedingLimit, a);
            // Explicitly raise the probability of equal limits, to test for https://github.com/elastic/elasticsearch/issues/139250
            int upperLimitValue = randomBoolean() ? precedingLimitValue : randomIntBetween(1, precedingLimitValue);
            Limit upperLimit = new Limit(EMPTY, new Literal(EMPTY, upperLimitValue, INTEGER), duplicatingLimitTestPlan);
            Limit optimizedPlan = as(optimizePlan(upperLimit), Limit.class);
            duplicatingTestCase.checkOptimizedPlan(duplicatingLimitTestPlan, optimizedPlan.child());
            assertTrue(optimizedPlan.duplicated());
            assertFalse(optimizedPlan.local());
        }
    }

    private LogicalPlan optimizePlan(LogicalPlan plan) {
        return new PushDownAndCombineLimits().apply(plan, unboundLogicalOptimizerContext());
    }

    /**
     * <pre>{@code
     * Limit[10[INTEGER],false,false]
     * \_Fork[[_meta_field{r}#30, emp_no{r}#31, first_name{r}#32, gender{r}#33, hire_date{r}#34, job{r}#35, job.raw{r}#36, l
     * anguages{r}#37, last_name{r}#38, long_noidx{r}#39, salary{r}#40, _fork{r}#41]]
     *   |_Project[[_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, gender{f}#10, hire_date{f}#15, job{f}#16, job.raw{f}#17, lan
     * guages{f}#11, last_name{f}#12, long_noidx{f}#18, salary{f}#13, _fork{r}#5]]
     *   | \_TopN[[Order[salary{f}#13,ASC,LAST]],10[INTEGER],false]
     *   |   \_Eval[[fork1[KEYWORD] AS _fork#5]]
     *   |     \_Filter[emp_no{f}#8 > 100[INTEGER]]
     *   |       \_EsRelation[employees][_meta_field{f}#14, emp_no{f}#8, first_name{f}#9, ge..]
     *   \_Project[[_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, gender{f}#21, hire_date{f}#26, job{f}#27, job.raw{f}#28, l
     * anguages{f}#22, last_name{f}#23, long_noidx{f}#29, salary{f}#24, _fork{r}#5]]
     *     \_TopN[[Order[emp_no{f}#19,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork2[KEYWORD] AS _fork#5]]
     *         \_Filter[emp_no{f}#19 < 10[INTEGER]]
     *           \_EsRelation[employees][_meta_field{f}#25, emp_no{f}#19, first_name{f}#20, ..]
     * }</pre>
     */
    public void testPushDownLimitIntoForkWithUnboundedOrderBy() {
        var query = """
            from employees
             | fork (where emp_no > 100 | SORT salary)
                    (where emp_no < 10 | SORT emp_no)
             | LIMIT 10
            """;
        var plan = planWithoutForkImplicitLimit(query);
        var limit = as(plan, Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(10));

        var fork = as(limit.child(), Fork.class);
        assertEquals(2, fork.children().size());

        for (LogicalPlan child : fork.children()) {
            var project = as(child, Project.class);
            var topN = as(project.child(), TopN.class);
            assertThat(((Literal) topN.limit()).value(), equalTo(10));
            var eval = as(topN.child(), Eval.class);
            var filter = as(eval.child(), Filter.class);
            assertThat(filter.child(), instanceOf(EsRelation.class));
        }
    }

    /**
     * <pre>{@code
     * Limit[10[INTEGER],false,false]
     * \_Fork[[a{r}#18, b{r}#19, _fork{r}#20]]
     *   |_Project[[a{r}#14, b{r}#15, _fork{r}#12]]
     *   | \_TopN[[Order[a{r}#14,ASC,LAST]],10[INTEGER],false]
     *   |   \_Eval[[fork1[KEYWORD] AS _fork#12]]
     *   |     \_MvExpand[b{r}#6,b{r}#15]
     *   |       \_MvExpand[a{r}#4,a{r}#14]
     *   |         \_LocalRelation[[a{r}#4, b{r}#6],Page{...}]
     *   \_Project[[a{r}#16, b{r}#17, _fork{r}#12]]
     *     \_TopN[[Order[b{r}#17,ASC,LAST]],10[INTEGER],false]
     *       \_Eval[[fork2[KEYWORD] AS _fork#12]]
     *         \_MvExpand[b{r}#6,b{r}#17]
     *           \_MvExpand[a{r}#4,a{r}#16]
     *             \_LocalRelation[[a{r}#4, b{r}#6],Page{...}]
     * }</pre>
     */
    public void testPushDownLimitIntoForkWithRowAndUnboundedOrderBy() {
        var query = """
            ROW a = [1, 2, 3], b = [4, 5, 6]
            | MV_EXPAND a
            | MV_EXPAND b
            | FORK (SORT a)
                   (SORT b)
            | LIMIT 10
            """;
        var plan = planWithoutForkImplicitLimit(query);

        var limit = as(plan, Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(10));

        var fork = as(limit.child(), Fork.class);
        assertEquals(2, fork.children().size());

        for (LogicalPlan child : fork.children()) {
            var project = as(child, Project.class);
            var topN = as(project.child(), TopN.class);
            assertThat(((Literal) topN.limit()).value(), equalTo(10));
            var eval = as(topN.child(), Eval.class);

            var mvExpand = as(eval.child(), MvExpand.class);
            mvExpand = as(mvExpand.child(), MvExpand.class);

            assertThat(mvExpand.child(), instanceOf(LocalRelation.class));
        }
    }

    /**
     * <pre>{@code
     * Limit[10000[INTEGER],[],false,false]
     * \_Limit[1[INTEGER],[emp_no{f}#6],false,false]
     *   \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testLimitByPruneIdenticalLimits() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            | LIMIT 2 BY emp_no
            | LIMIT 1 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(10000));
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * Limit[10000[INTEGER],[],false,false]
     * \_Limit[1[INTEGER],[first_name{f}#6],false,false]
     *   \_Limit[1[INTEGER],[emp_no{f}#5],false,false]
     *     \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }</pre>
     */
    public void testLimitByKeepDifferentGroupings() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            | LIMIT 1 BY first_name
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(10000));
        var limit1 = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit1.limit()).value(), equalTo(1));
        assertThat(limit1.groupings().size(), equalTo(1));
        assertThat(Expressions.names(limit1.groupings()), contains("first_name"));
        var limit2 = as(limit1.child(), Limit.class);
        assertThat(((Literal) limit2.limit()).value(), equalTo(1));
        assertThat(limit2.groupings().size(), equalTo(1));
        assertThat(Expressions.names(limit2.groupings()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * Limit[2[INTEGER],[],false,false]
     * \_Limit[2[INTEGER],[emp_no{f}#5],false,false]
     *   \_Limit[2[INTEGER],[],false,false]
     *     \_Limit[1[INTEGER],[emp_no{f}#5],false,false]
     *       \_EsRelation[test][_meta_field{f}#11, emp_no{f}#5, first_name{f}#6, ge..]
     * }</pre>
     */
    public void testLimitByNotCombinedWhenSeparatedByPlainLimit() {
        var plan = plan("""
            FROM test
            | LIMIT 1 BY emp_no
            | LIMIT 2
            | LIMIT 2 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(((Literal) defaultLimit.limit()).value(), equalTo(2));
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(2));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        var limit2 = as(limit.child(), Limit.class);
        assertThat(((Literal) limit2.limit()).value(), equalTo(2));
        assertThat(limit2.groupings(), empty());
        var limit3 = as(limit2.child(), Limit.class);
        assertThat(((Literal) limit3.limit()).value(), equalTo(1));
        assertThat(Expressions.names(limit3.groupings()), contains("emp_no"));
    }

    /**
     * <pre>{@code
     * TopN[[Order[languages{f}#12,ASC,LAST]],10000[INTEGER],false]
     * \_Aggregate[[languages{f}#12],[COUNT(*[KEYWORD],true[BOOLEAN],PT0S[TIME_DURATION]) AS c, languages{f}#12]]
     *   \_Limit[2[INTEGER],[languages{f}#12],false,false]
     *     \_TopN[[Order[emp_no{f}#9,ASC,LAST]],1000[INTEGER],false]
     *       \_EsRelation[test][_meta_field{f}#15, emp_no{f}#9, first_name{f}#10, g..]
     * }</pre>
     */
    public void testLimitByNotCombinedWithTopN() {
        var plan = plan("""
            FROM test
            | SORT emp_no
            | LIMIT 1000
            | LIMIT 2 BY languages
            | STATS c = COUNT(*) BY languages
            | SORT languages ASC NULLS LAST
            """);

        var topN = as(plan, TopN.class);
        assertThat(topN.limit().fold(FoldContext.small()), equalTo(10000));
        assertThat(orderNames(topN), contains("languages"));
        var agg = as(topN.child(), Aggregate.class);
        assertThat(Expressions.names(agg.groupings()), contains("languages"));
        var limit = as(agg.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(2));
        assertThat(Expressions.names(limit.groupings()), contains("languages"));
        var innerTopN = as(limit.child(), TopN.class);
        assertThat(innerTopN.limit().fold(FoldContext.small()), equalTo(1000));
        assertThat(orderNames(innerTopN), contains("emp_no"));
        as(innerTopN.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references a field introduced by a local Enrich must not be
     * pushed below the Enrich, because the field would not exist there.
     * <pre>{@code
     * Limit[10000[INTEGER],[],false,false]
     * \_Limit[5[INTEGER],[language_name{f}#N],false,false]
     *   \_Enrich[ANY,languages_idx,first_name{f}#N,...]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByNotPushedBelowLocalEnrichWhenGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH languages_idx ON first_name
            | LIMIT 5 BY language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references a field introduced by a remote Enrich must not be
     * duplicated below the Enrich, because the field would not exist there.
     * <pre>{@code
     * Limit[10000[INTEGER],[],false,false]
     * \_Limit[5[INTEGER],[language_name{f}#N],false,false]
     *   \_Enrich[REMOTE,languages_remote,first_name{f}#N,...]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByNotDuplicatedBelowRemoteEnrichWhenGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH _remote:languages_remote ON first_name
            | LIMIT 5 BY language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references only source fields should still be pushed below
     * a local Enrich, since the field is available in the Enrich's child.
     * <pre>{@code
     * Limit[10000[INTEGER],[],false,false]
     * \_Limit[5[INTEGER],[emp_no{f}#6, language_name{r}#20],false,false]
     *   \_Enrich[ANY,languages_idx[KEYWORD],first_name{f}#7,{"match":{"indices":[],"match_field":"id","enrich_fields":["language_c
     * ode","language_name"]}},{=languages_idx},[language_code{r}#19, language_name{r}#20]]
     *     \_EsRelation[test][_meta_field{f}#12, emp_no{f}#6, first_name{f}#7, ge..]
     * }</pre>
     */
    public void testLimitByNotDuplicatedBelowRemoteEnrichWhenSomeGroupingReferencesEnrichField() {
        var plan = plan("""
            FROM test
            | ENRICH languages_idx ON first_name
            | LIMIT 5 BY emp_no, language_name
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no", "language_name"));
        var enrich = as(limit.child(), Enrich.class);
        as(enrich.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) whose grouping references only source fields should still be pushed below
     * a local Enrich, since the field is available in the Enrich's child.
     * <pre>{@code
     * Enrich[ANY,languages_idx,first_name{f}#N,...]
     * \_Limit[10000[INTEGER],[],false,false]
     *   \_Limit[5[INTEGER],[emp_no{f}#N],false,false]
     *     \_EsRelation[test][...]
     * }</pre>
     */
    public void testLimitByPushedBelowLocalEnrichWhenGroupingOnSourceField() {
        var plan = plan("""
            FROM test
            | ENRICH languages_idx ON first_name
            | LIMIT 5 BY emp_no
            """);

        var enrich = as(plan, Enrich.class);
        var defaultLimit = as(enrich.child(), Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        as(limit.child(), EsRelation.class);
    }

    /**
     * A grouped LIMIT (LIMIT BY) above a Fork must not be pushed into the fork branches.
     */
    public void testLimitByNotPushedIntoForkBranches() {
        var plan = plan("""
            FROM test
            | FORK (WHERE emp_no > 100) (WHERE emp_no < 10)
            | LIMIT 5 BY emp_no
            """);

        var defaultLimit = as(plan, Limit.class);
        assertThat(defaultLimit.groupings(), empty());
        var limit = as(defaultLimit.child(), Limit.class);
        assertThat(((Literal) limit.limit()).value(), equalTo(5));
        assertThat(Expressions.names(limit.groupings()), contains("emp_no"));
        var fork = as(limit.child(), Fork.class);
        for (LogicalPlan branch : fork.children()) {
            var project = as(branch, Project.class);
            var eval = as(project.child(), Eval.class);
            var branchLimit = as(eval.child(), Limit.class);
            assertThat(branchLimit.groupings(), empty());
            as(branchLimit.child(), Filter.class);
        }
    }

    private static List<String> orderNames(TopN topN) {
        return topN.order().stream().map(o -> as(o.child(), NamedExpression.class).name()).toList();
    }
}
