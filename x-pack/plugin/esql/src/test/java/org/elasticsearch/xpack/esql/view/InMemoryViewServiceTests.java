/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class InMemoryViewServiceTests extends AbstractStatementParserTests {
    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    static InMemoryViewService viewService;
    static InMemoryViewResolver viewResolver;

    @BeforeClass
    public static void setup() {
        viewService = InMemoryViewService.makeViewService();
        viewResolver = viewService.getViewResolver();
    }

    @AfterClass
    public static void afterTearDown() {
        viewService.close();
    }

    @Before
    public void setupTest() {
        viewService.clearAllViews();
        viewResolver.clear();
    }

    PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
    QueryParams queryParams = new QueryParams();
    ProjectId projectId = ProjectId.fromId("1");

    public void testPutGet() {
        addView("view1", "FROM emp");
        addView("view2", "FROM view1");
        addView("view3", "FROM view2");
        assertThat(viewService.get(projectId, "view1").query(), equalTo("FROM emp"));
        assertThat(viewService.get(projectId, "view2").query(), equalTo("FROM view1"));
        assertThat(viewService.get(projectId, "view3").query(), equalTo("FROM view2"));
    }

    public void testReplaceView() {
        addView("view1", "FROM emp");
        addView("view2", "FROM view1");
        addView("view3", "FROM view2");
        LogicalPlan plan = query("FROM view3");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp")));
    }

    public void testReplaceViewPlans() {
        addView("view1", "FROM emp | WHERE emp.age > 30");
        addView("view2", "FROM view1 | WHERE emp.age < 40");
        addView("view3", "FROM view2 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view3");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp | WHERE emp.age > 30 | WHERE emp.age < 40 | WHERE emp.salary > 50000")));
    }

    public void testReplaceViews() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view1, view2, view3");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp1, emp2, emp3")));
    }

    public void testReplaceViewsPlans() {
        addView("view1", "FROM emp1 | WHERE emp.age > 30");
        addView("view2", "FROM emp2 | WHERE emp.age < 40");
        addView("view3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view1, view2, view3");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        // We cannot express the expected plan easily, so we check its structure instead
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(3));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2 | WHERE emp.age < 40")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000"))
            )
        );
    }

    public void testReplaceViewsWildcard() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp1, emp2, emp3")));
    }

    public void testReplaceViewsWildcardWithIndex() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM view*, emp1, emp2, emp3")));
    }

    public void testReplaceViewsPlanWildcard() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        // We cannot express the expected plan easily, so we check its structure instead
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(3));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2 | WHERE emp.age < 40")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000"))
            )
        );
    }

    public void testReplaceViewsPlanWildcardWithIndex() {
        addIndex("viewX");
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        // We cannot express the expected plan easily, so we check its structure instead
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(4));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM view*")),
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2 | WHERE emp.age < 40")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000"))
            )
        );
    }

    public void testReplaceViewsNestedWildcard() {
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp1,emp3,emp1,emp2")));
    }

    public void testReplaceViewsNestedWildcardWithIndex() {
        addIndex("view_1_X");
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM view_1_*,emp1,emp3,emp1,emp2")));
    }

    public void testReplaceViewsNestedWildcards() {
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        addView("view_2_1", "FROM view_2, view_1");
        addView("view_2_3", "FROM view_2, view_3");
        addView("view_3_1", "FROM view_3, view_1");
        addView("view_3_2", "FROM view_3, view_2");
        LogicalPlan plan = query("FROM view_1_*, view_2_*, view_3_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp1,emp3,emp1,emp2,emp2,emp1,emp2,emp3,emp3,emp1,emp3,emp2")));
    }

    public void testReplaceViewsNestedWildcardsWithIndex() {
        addIndex("view_2_X");
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        addView("view_2_1", "FROM view_2, view_1");
        addView("view_2_3", "FROM view_2, view_3");
        addView("view_3_1", "FROM view_3, view_1");
        addView("view_3_2", "FROM view_3, view_2");
        LogicalPlan plan = query("FROM view_1_*, view_2_*, view_3_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM view_2_*,emp1,emp3,emp1,emp2,emp2,emp1,emp2,emp3,emp3,emp1,emp3,emp2")));
    }

    public void testReplaceViewsNestedWildcardsWithIndexes() {
        addIndex("view_1_X");
        addIndex("view_2_X");
        addIndex("view_3_X");
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        addView("view_2_1", "FROM view_2, view_1");
        addView("view_2_3", "FROM view_2, view_3");
        addView("view_3_1", "FROM view_3, view_1");
        addView("view_3_2", "FROM view_3, view_2");
        LogicalPlan plan = query("FROM view_1_*, view_2_*, view_3_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        assertThat(
            rewritten,
            matchesPlan(query("FROM view_1_*,view_2_*,view_3_*,emp1,emp3,emp1,emp2,emp2,emp1,emp2,emp3,emp3,emp1,emp3,emp2"))
        );
    }

    public void testReplaceViewsNestedPlansWildcard() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        // We cannot express the expected plan easily, so we check its structure instead
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(2));
        for (LogicalPlan child : subqueries) {
            child = (child instanceof Subquery subquery) ? subquery.child() : child;
            assertThat(child, instanceOf(UnionAll.class));
            List<LogicalPlan> subchildren = child.children();
            assertThat(subchildren.size(), equalTo(2));
            assertThat(
                subchildren,
                matchesAnyXOf(
                    2,
                    query("FROM emp1 | WHERE emp.age > 30"),
                    query("FROM emp2 | WHERE emp.age < 40"),
                    query("FROM emp3 | WHERE emp.salary > 50000")
                )
            );
        }
    }

    public void testReplaceViewsNestedPlansWildcardWithIndex() {
        addIndex("view_1_X");
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        // We cannot express the expected plan easily, so we check its structure instead
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(3));
        assertThat(subqueries.getFirst(), matchesPlan(query("FROM view_1_*")));
        for (LogicalPlan child : subqueries.subList(1, 3)) {
            child = (child instanceof Subquery subquery) ? subquery.child() : child;
            assertThat(child, instanceOf(UnionAll.class));
            List<LogicalPlan> subchildren = child.children();
            assertThat(subchildren.size(), equalTo(2));
            assertThat(
                subchildren,
                matchesAnyXOf(
                    2,
                    query("FROM emp1 | WHERE emp.age > 30"),
                    query("FROM emp2 | WHERE emp.age < 40"),
                    query("FROM emp3 | WHERE emp.salary > 50000")
                )
            );
        }
    }

    public void testReplaceViewsNestedPlansWildcards() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        addView("view_2_1", "FROM view_2, view_1");
        addView("view_2_3", "FROM view_2, view_3");
        addView("view_3_1", "FROM view_3, view_1");
        addView("view_3_2", "FROM view_3, view_2");
        LogicalPlan plan = query("FROM view_1_*, view_2_*, view_3_*");
        LogicalPlan rewritten = viewResolver.replaceViews(plan, this::parse).plan();
        // We cannot express the expected plan easily, so we check its structure instead
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(6));
        for (LogicalPlan child : subqueries) {
            child = (child instanceof Subquery subquery) ? subquery.child() : child;
            assertThat(child, instanceOf(UnionAll.class));
            List<LogicalPlan> subchildren = child.children();
            assertThat(subchildren.size(), equalTo(2));
            assertThat(
                subchildren,
                matchesAnyXOf(
                    2,
                    query("FROM emp1 | WHERE emp.age > 30"),
                    query("FROM emp2 | WHERE emp.age < 40"),
                    query("FROM emp3 | WHERE emp.salary > 50000")
                )
            );
        }
    }

    public void testViewDepthExceeded() {
        addView("view1", "FROM emp");
        addView("view2", "FROM view1");
        addView("view3", "FROM view2");
        addView("view4", "FROM view3");
        addView("view5", "FROM view4");
        addView("view6", "FROM view5");
        addView("view7", "FROM view6");
        addView("view8", "FROM view7");
        addView("view9", "FROM view8");
        addView("view10", "FROM view9");
        addView("view11", "FROM view10");

        // FROM view11 should fail
        Exception e = expectThrows(VerificationException.class, () -> viewResolver.replaceViews(query("FROM view11"), this::parse));
        assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 10 has been exceeded"));

        // But FROM view10 should work
        LogicalPlan rewritten = viewResolver.replaceViews(query("FROM view10"), this::parse).plan();
        assertThat(rewritten, matchesPlan(query("FROM emp")));
    }

    public void testModifiedViewDepth() {
        try (
            InMemoryViewService customViewService = viewService.withSettings(
                Settings.builder().put(ViewResolver.MAX_VIEW_DEPTH_SETTING.getKey(), 1).build()
            )
        ) {
            addView("view1", "FROM emp", customViewService);
            addView("view2", "FROM view1", customViewService);
            addView("view3", "FROM view2", customViewService);

            InMemoryViewResolver customViewResolver = customViewService.getViewResolver();

            // FROM view2 should fail
            Exception e = expectThrows(
                VerificationException.class,
                () -> customViewResolver.replaceViews(query("FROM view2"), this::parse)
            );
            assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 1 has been exceeded"));

            // But FROM view1 should work
            LogicalPlan rewritten = customViewResolver.replaceViews(query("FROM view1"), this::parse).plan();
            assertThat(rewritten, matchesPlan(query("FROM emp")));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testViewCountExceeded() {
        for (int i = 0; i < ViewService.MAX_VIEWS_COUNT_SETTING.getDefault(Settings.EMPTY); i++) {
            addView("view" + i, "FROM emp");
        }

        // FROM view11 should fail
        Exception e = expectThrows(Exception.class, () -> addView("viewx", "FROM emp"));
        assertThat(e.getMessage(), containsString("cannot add view, the maximum number of views is reached: 100"));
    }

    public void testModifiedViewCount() {
        try (
            InMemoryViewService customViewService = viewService.withSettings(
                Settings.builder().put(ViewService.MAX_VIEWS_COUNT_SETTING.getKey(), 1).build()
            )
        ) {
            addView("view1", "FROM emp", customViewService);

            // View2 should fail
            Exception e = expectThrows(Exception.class, () -> addView("view2", "FROM emp", customViewService));
            assertThat(e.getMessage(), containsString("cannot add view, the maximum number of views is reached: 1"));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testViewLengthExceeded() {
        addView("view1", "FROM short");

        // Long view definition should fail
        Exception e = expectThrows(
            Exception.class,
            () -> addView("viewx", "FROM " + "a".repeat(Math.max(0, ViewService.MAX_VIEW_LENGTH_SETTING.getDefault(Settings.EMPTY))))
        );
        assertThat(e.getMessage(), containsString("view query is too large: 10005 characters, the maximum allowed is 10000"));
    }

    public void testModifiedViewLength() {
        try (
            InMemoryViewService customViewService = viewService.withSettings(
                Settings.builder().put(ViewService.MAX_VIEW_LENGTH_SETTING.getKey(), 6).build()
            )
        ) {
            addView("view1", "FROM a", customViewService);

            // Just one character longer should fail
            Exception e = expectThrows(Exception.class, () -> addView("view2", "FROM aa", customViewService));
            assertThat(e.getMessage(), containsString("view query is too large: 7 characters, the maximum allowed is 6"));
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
    }

    public void testSerializationSubqueryWithSourceFromViewQuery() {
        // This test verifies that view sources are correctly tagged with their view name
        // and that the Configuration contains the view queries, allowing proper deserialization.
        //
        // For example, if a view is defined as "FROM employees | EVAL x = ABS(salary)" and the
        // outer query is "FROM v", the expressions from the view will have Source positions
        // that exceed the length of "FROM v". Without the view name tagging and view queries
        // in Configuration, this would cause deserialization to fail.

        String viewName = "my_view";
        String viewQuery = "FROM employees | EVAL x = ABS(salary)";
        String shortOuterQuery = "FROM v";

        // "FROM employees | EVAL x = " is 26 characters (0-indexed 0-25)
        // "ABS(salary)" starts at index 26 (0-indexed), column 27 (1-indexed)
        // The Source constructor takes (line, charPositionInLine, text) where charPositionInLine is 0-indexed
        Source sourceFromView = new Source(1, 26, "ABS(salary)");

        // Create an expression with this source - Abs writes source().writeTo(out)
        Literal literalArg = new Literal(Source.EMPTY, 42, DataType.INTEGER);
        Abs absExpr = new Abs(sourceFromView, literalArg);

        // Wrap in an Eval plan to make it serializable
        LogicalPlan child = EsRelationSerializationTests.randomEsRelation();
        Alias alias = new Alias(Source.EMPTY, "x", absExpr);
        Eval eval = new Eval(Source.EMPTY, child, List.of(alias));

        // Test 1: Without view name tagging, this should fail
        Configuration configWithoutViewQueries = ConfigurationTestUtils.randomConfiguration(shortOuterQuery);
        Exception e = expectThrows(
            Exception.class,
            () -> SerializationTestUtils.serializeDeserialize(
                eval,
                PlanStreamOutput::writeNamedWriteable,
                in -> in.readNamedWriteable(LogicalPlan.class),
                configWithoutViewQueries
            )
        );
        assertThat(e.getMessage(), containsString("overrun query size"));

        // Test 2: With view name tagging AND view queries in Configuration, this should work
        Source taggedSource = sourceFromView.withViewName(viewName);
        Abs taggedAbsExpr = new Abs(taggedSource, literalArg);
        Alias taggedAlias = new Alias(Source.EMPTY, "x", taggedAbsExpr);
        Eval taggedEval = new Eval(Source.EMPTY, child, List.of(taggedAlias));

        Configuration configWithViewQueries = ConfigurationTestUtils.randomConfiguration(shortOuterQuery)
            .withViewQueries(Map.of(viewName, viewQuery));
        SerializationTestUtils.serializeDeserialize(
            taggedEval,
            PlanStreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(LogicalPlan.class),
            configWithViewQueries
        );
    }

    private void addIndex(String name) {
        viewResolver.addIndex(name);
    }

    private void addView(String name, String query) {
        addView(name, query, viewService);
    }

    private void addView(String name, String query, ViewService viewService) {
        PutViewAction.Request request = new PutViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new View(name, query));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> err = new AtomicReference<>(null);
        viewService.putView(projectId, request, ActionListener.wrap(r -> latch.countDown(), e -> {
            err.set(e);
            latch.countDown();
        }));
        try {
            // In-memory puts are synchronous, so we should never wait here
            assert latch.await(1, TimeUnit.MILLISECONDS) : "should never timeout";
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (err.get() != null) {
            throw new RuntimeException(err.get());
        }
    }

    private LogicalPlan parse(String query, String viewName) {
        return parser.parseView(
            query,
            queryParams,
            new SettingsValidationContext(false, false),
            telemetry,
            EMPTY_INFERENCE_SETTINGS,
            viewName
        ).plan();
    }

    private static Matcher<LogicalPlan> matchesPlan(LogicalPlan plan) {
        return new LogicalPlanEqualTo(plan);
    }

    private static class LogicalPlanEqualTo extends BaseMatcher<LogicalPlan> {

        private final LogicalPlan plan;

        private LogicalPlanEqualTo(LogicalPlan plan) {
            this.plan = (plan instanceof Subquery subquery) ? subquery.child() : plan;
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof LogicalPlan other) {
                LogicalPlan otherPlan = (other instanceof Subquery subquery) ? subquery.child() : other;
                return plan.toString().equals(otherPlan.toString());
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            if (plan != null) {
                description.appendText(plan.toString());
            } else {
                description.appendText("null");
            }
        }
    }

    /**
     * Matches if the iterable contains exactly {@code x} items from {@code plans} in any order.
     * For example, {@code matchesAnyXOf(2, a, b, c)} is equivalent to:
     * {@code anyOf(containsInAnyOrder(a, b), containsInAnyOrder(a, c), containsInAnyOrder(b, c))}
     */
    private static Matcher<Iterable<? extends LogicalPlan>> matchesAnyXOf(int x, LogicalPlan... plans) {
        if (x < 1) {
            throw new IllegalArgumentException("x must be >= 1");
        }
        List<Matcher<? super LogicalPlan>> matchers = Arrays.stream(plans)
            .<Matcher<? super LogicalPlan>>map(InMemoryViewServiceTests::matchesPlan)
            .toList();
        if (x >= matchers.size()) {
            return containsInAnyOrder(matchers);
        }
        List<Matcher<Iterable<? extends LogicalPlan>>> combinations = new ArrayList<>();
        generateCombinations(matchers, x, 0, new ArrayList<>(), combinations);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Matcher<Iterable<? extends LogicalPlan>>[] combinationsArray = combinations.toArray(new Matcher[0]);
        return anyOf(combinationsArray);
    }

    private static void generateCombinations(
        List<Matcher<? super LogicalPlan>> matchers,
        int size,
        int start,
        List<Matcher<? super LogicalPlan>> current,
        List<Matcher<Iterable<? extends LogicalPlan>>> result
    ) {
        if (current.size() == size) {
            Collection<Matcher<? super LogicalPlan>> combination = new ArrayList<>(current);
            result.add(containsInAnyOrder(combination));
            return;
        }
        for (int i = start; i < matchers.size(); i++) {
            current.add(matchers.get(i));
            generateCombinations(matchers, size, i + 1, current, result);
            current.remove(current.size() - 1);
        }
    }
}
