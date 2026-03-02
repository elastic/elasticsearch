/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class InMemoryViewServiceTests extends AbstractStatementParserTests {
    protected final EsqlParser parser = EsqlParser.INSTANCE;

    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    static InMemoryViewService viewService;
    static InMemoryViewResolver viewResolver;
    PlanTelemetry telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
    QueryParams queryParams = new QueryParams();
    ProjectId projectId = ProjectId.DEFAULT;

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
        viewService.clearAllViewsAndIndices();
        for (String idx : List.of("emp", "emp1", "emp2", "emp3", "logs")) {
            addIndex(idx);
        }
    }

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
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp")));
    }

    public void testViewExclusion() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        LogicalPlan plan = query("FROM view*, -view2");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1")));
    }

    public void testExclusionWithRemainingIndexMatch() {
        addView("logs-nginx", "FROM logs-1 | WHERE logs.type == nginx");
        addIndex("logs-1");
        LogicalPlan plan = query("FROM logs*, -logs-nginx");
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs*, -logs-nginx")));
    }

    public void testExclusionWithNoRemainingIndexMatch() {
        addView("logs-nginx", "FROM logs | WHERE logs.type == nginx");
        LogicalPlan plan = query("FROM logs*, -logs-nginx");
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs*, -logs-nginx")));
    }

    public void testExclusionPreservedForIndexResolution() {
        addView("logs1", "FROM logs2");
        addIndex("logs2");
        addIndex("logs3");
        LogicalPlan plan = query("FROM logs*,-logs3");
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs*,-logs3,logs2")));
    }

    public void testExclusionMultipleViews() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*, -view1, -view3");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp2")));
    }

    public void testExclusionAllViews() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        LogicalPlan plan = query("FROM view*, -view1, -view2");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*, -view1, -view2")));
    }

    public void testExclusionKeepingViewWithPipeBody() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2 | WHERE emp.age > 30");
        LogicalPlan plan = query("FROM view*, -view1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp2 | WHERE emp.age > 30")));
    }

    public void testExclusionWithWildcardExclusionPattern() {
        addView("view_a1", "FROM emp1");
        addView("view_a2", "FROM emp2");
        addView("view_b1", "FROM emp3");
        LogicalPlan plan = query("FROM view_*, -view_a*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp3")));
    }

    public void testExclusionPreservesNestedViewReference() {
        addView("view_inner", "FROM emp1");
        addView("view_outer", "FROM view_inner");
        LogicalPlan plan = query("FROM view_*, -view_inner");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1")));
    }

    public void testExclusionWithMultiplePipeBodies() {
        addView("view1", "FROM emp1 | WHERE emp.age > 30");
        addView("view2", "FROM emp2 | WHERE emp.age < 40");
        addView("view3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view*, -view2");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(2));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000"))
            )
        );
    }

    public void testExclusionWithMatchingIndexAndViewExclusion() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        LogicalPlan plan = query("FROM view*, -view2");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*,emp1")));
    }

    public void testExclusionAllViewsWithIndex() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        LogicalPlan plan = query("FROM view*, -view1, -view2");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*, -view1, -view2")));
    }

    public void testExclusionNonExistingResource() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM view*, -donotexist");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*,-donotexist,emp1")));
    }

    public void testFailureSelector() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        LogicalPlan plan = query("FROM view*::failures");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*::failures")));
    }

    public void testConcreteFailureSelector() {
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM view1::failures");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view1::failures")));
    }

    public void testDataSelector() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addIndex("view3");
        LogicalPlan plan = query("FROM view*::data");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*::data,emp1,emp2")));
    }

    public void testConcreteDataSelector() {
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM view1::data");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1")));
    }

    public void testCCSRemoteWildcardNotResolvedAsView() {
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM *:view1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM *:view1")));
    }

    public void testCCSExpressionNotResolvedAsView() {
        addIndex("remote:view1");
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM remote:view1, view1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM remote:view1,emp1")));
    }

    public void testCCSWildcardNotResolvedAsView() {
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM remote:view*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM remote:view*")));
    }

    public void testReplaceViewPlans() {
        addView("view1", "FROM emp | WHERE emp.age > 30");
        addView("view2", "FROM view1 | WHERE emp.age < 40");
        addView("view3", "FROM view2 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view3");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp | WHERE emp.age > 30 | WHERE emp.age < 40 | WHERE emp.salary > 50000")));
    }

    public void testReplaceViews() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view1, view2, view3");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1, emp2, emp3")));
    }

    public void testReplaceViewsPlans() {
        addView("view1", "FROM emp1 | WHERE emp.age > 30");
        addView("view2", "FROM emp2 | WHERE emp.age < 40");
        addView("view3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view1, view2, view3");
        LogicalPlan rewritten = replaceViews(plan);
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

    public void testReplaceViewsInForkMultipleBranches() {
        addView("view1", "FROM emp | WHERE emp.age > 25");
        LogicalPlan plan = query("FROM view1 | FORK (WHERE emp.age < 50) (WHERE emp.age > 35) (STATS count = COUNT(*))");
        Fork fork = (Fork) replaceViews(plan);
        List<LogicalPlan> children = fork.children();
        assertThat(children.size(), equalTo(3));

        assertThat(
            as(children.get(0), Eval.class).child(),
            equalToIgnoringIds(query("FROM emp | WHERE emp.age > 25 | WHERE emp.age < 50"))
        );
        assertThat(
            as(children.get(1), Eval.class).child(),
            equalToIgnoringIds(query("FROM emp | WHERE emp.age > 25 | WHERE emp.age > 35"))
        );
        assertThat(
            as(children.get(2), Eval.class).child(),
            equalToIgnoringIds(query("FROM emp | WHERE emp.age > 25 | STATS count = COUNT(*)"))
        );
    }

    public void testReplaceViewsWildcard() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1, emp2, emp3")));
    }

    public void testReplaceViewsWildcardWithIndex() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*, emp1, emp2, emp3")));
    }

    public void testMixedViewAndIndexMergedUnresolvedRelation() {
        addView("view1", "FROM emp");
        addIndex("index1");
        LogicalPlan plan = query("FROM view1, index1");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(rewritten, instanceOf(UnresolvedRelation.class));
        assertThat(as(rewritten, UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("index1,emp"));
    }

    public void testMissingIndexPreservedWhenMixedWithView() {
        addView("view1", "FROM emp");
        LogicalPlan plan = query("FROM view1, missing-index");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(as(rewritten, UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("missing-index,emp"));
    }

    public void testMissingIndexPreservedWhenMixedWithViewWithPipes() {
        addView("view1", "FROM emp | WHERE emp.age > 30");
        LogicalPlan plan = query("FROM view1, missing-index");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(rewritten, instanceOf(UnionAll.class));
    }

    public void testReplaceViewsPlanWildcard() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view*");
        LogicalPlan rewritten = replaceViews(plan);
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
        LogicalPlan rewritten = replaceViews(plan);
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp3,emp1,emp2")));
    }

    public void testReplaceViewsNestedWildcardWithIndex() {
        addIndex("view_1_X");
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view_1_*,emp1,emp3,emp1,emp2")));
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp3,emp1,emp2,emp2,emp1,emp2,emp3,emp3,emp1,emp3,emp2")));
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM view_2_*,emp1,emp3,emp1,emp2,emp2,emp1,emp2,emp3,emp3,emp1,emp3,emp2")));
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
        LogicalPlan rewritten = replaceViews(plan);
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
        LogicalPlan rewritten = replaceViews(plan);
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
        LogicalPlan rewritten = replaceViews(plan);
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
        LogicalPlan rewritten = replaceViews(plan);
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
        {
            Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM view11")));
            assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 10 has been exceeded"));
        }
        // But FROM view10 should work
        {
            LogicalPlan rewritten = replaceViews(query("FROM view10"));
            assertThat(rewritten, matchesPlan(query("FROM emp")));
        }
    }

    public void testCircularViewSelfReference() {
        addView("view_a", "FROM view_a");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM view_a")));
        assertThat(e.getMessage(), containsString("circular view reference 'view_a'"));
    }

    public void testCircularViewMutualReference() {
        addView("view_a", "FROM view_b");
        addView("view_b", "FROM view_a");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM view_a")));
        assertThat(e.getMessage(), containsString("circular view reference 'view_a'"));
        assertThat(e.getMessage(), containsString("view_a -> view_b"));
    }

    public void testCircularViewChain() {
        addView("chain_a", "FROM chain_b");
        addView("chain_b", "FROM chain_c");
        addView("chain_c", "FROM chain_a");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM chain_a")));
        assertThat(e.getMessage(), containsString("circular view reference 'chain_a'"));
        assertThat(e.getMessage(), containsString("chain_a -> chain_b -> chain_c"));
    }

    public void testCircularViewViaWildcard() {
        addView("v_1", "FROM v_*");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM v_*")));
        assertThat(e.getMessage(), containsString("circular view reference 'v_1'"));
    }

    public void testCircularViewViaWildcardWithIndex() {
        addIndex("v_idx");
        addView("v_1", "FROM v_*");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM v_*")));
        assertThat(e.getMessage(), containsString("circular view reference 'v_1'"));
    }

    public void testCircularViewInMultiSource() {
        addView("view_a", "FROM emp");
        addView("view_b", "FROM view_c");
        addView("view_c", "FROM view_a");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM view_a, view_b")));
        assertThat(e.getMessage(), containsString("circular view reference 'view_a'"));
    }

    public void testCircularViewWithPipes() {
        addView("view_a", "FROM view_b | WHERE emp.age > 30");
        addView("view_b", "FROM view_a | WHERE emp.salary > 50000");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM view_a")));
        assertThat(e.getMessage(), containsString("circular view reference 'view_a'"));
        assertThat(e.getMessage(), containsString("view_a -> view_b"));
    }

    public void testCircularViewInFork() {
        addView("view_a", "FROM view_b");
        addView("view_b", "FROM view_a");
        Exception e = expectThrows(
            VerificationException.class,
            () -> replaceViews(query("FROM view_a | FORK (WHERE emp.age > 30) (WHERE emp.age < 50)"))
        );
        assertThat(e.getMessage(), containsString("circular view reference 'view_a'"));
    }

    public void testModifiedViewDepth() {
        try (
            InMemoryViewService customViewService = viewService.withSettings(
                Settings.builder().put(ViewResolver.MAX_VIEW_DEPTH_SETTING.getKey(), 1).build()
            )
        ) {
            customViewService.addIndex(projectId, "emp");
            addView("view1", "FROM emp", customViewService);
            addView("view2", "FROM view1", customViewService);
            addView("view3", "FROM view2", customViewService);

            InMemoryViewResolver customViewResolver = customViewService.getViewResolver();
            {
                PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
                customViewResolver.replaceViews(query("FROM view2"), this::parse, future);
                // FROM view2 should fail
                Exception e = expectThrows(VerificationException.class, future::actionGet);
                assertThat(e.getMessage(), startsWith("The maximum allowed view depth of 1 has been exceeded"));
            }
            // But FROM view1 should work
            {
                PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
                customViewResolver.replaceViews(query("FROM view1"), this::parse, future);
                LogicalPlan rewritten = future.actionGet().plan();
                assertThat(rewritten, matchesPlan(query("FROM emp")));
            }
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

    public void testViewWithDateMathInBody() {
        addDateMathIndex("logs-");
        addView("view1", "FROM <logs-{now/d}>");
        LogicalPlan plan = query("FROM view1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM <logs-{now/d}>")));
    }

    public void testNestedViewWithDateMathInBody() {
        addDateMathIndex("logs-");
        addView("view1", "FROM <logs-{now/d}>");
        addView("view2", "FROM view1");
        LogicalPlan plan = query("FROM view2");
        assertThat(replaceViews(plan), matchesPlan(query("FROM <logs-{now/d}>")));
    }

    public void testViewWithDateMathAndPipeInBody() {
        addDateMathIndex("logs-");
        addView("view1", "FROM <logs-{now/d}> | WHERE log.level == \"error\"");
        LogicalPlan plan = query("FROM view1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM <logs-{now/d}> | WHERE log.level == \"error\"")));
    }

    public void testDateMathResolvesToViewName() {
        var date = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        var resolvedName = DateTimeFormatter.ofPattern("'view-'yyyy.MM.dd", Locale.ROOT).format(date);
        addView(resolvedName, "FROM emp");
        try {
            LogicalPlan plan = query("FROM <view-{now/d{yyyy.MM.dd}}>");
            assertThat(replaceViews(plan), matchesPlan(query("FROM emp")));
        } catch (AssertionError e) {
            assumeTrue("Date must stay the same during the test", Objects.equals(date, LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC)));
            throw e;
        }
    }

    public void testDateMathAlongsideConcreteView() {
        var date = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        var resolvedName = DateTimeFormatter.ofPattern("'logs-'yyyy.MM.dd", Locale.ROOT).format(date);
        addIndex(resolvedName);
        addView("view1", "FROM emp");
        try {
            LogicalPlan plan = query("FROM <logs-{now/d{yyyy.MM.dd}}>, view1");
            assertThat(replaceViews(plan), matchesPlan(query("FROM <logs-{now/d{yyyy.MM.dd}}>,emp")));
        } catch (AssertionError e) {
            assumeTrue("Date must stay the same during the test", Objects.equals(date, LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC)));
            throw e;
        }
    }

    public void testDateMathAlongsideViewWildcard() {
        var date = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        var resolvedName = DateTimeFormatter.ofPattern("'logs-'yyyy.MM.dd", Locale.ROOT).format(date);
        addIndex(resolvedName);
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        try {
            LogicalPlan plan = query("FROM <logs-{now/d{yyyy.MM.dd}}>, view*");
            assertThat(replaceViews(plan), matchesPlan(query("FROM <logs-{now/d{yyyy.MM.dd}}>,emp1,emp2")));
        } catch (AssertionError e) {
            assumeTrue("Date must stay the same during the test", Objects.equals(date, LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC)));
            throw e;
        }
    }

    public void testDateMathAlongsideViewWithPipeBody() {
        var date = LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC);
        var resolvedName = DateTimeFormatter.ofPattern("'logs-'yyyy.MM.dd", Locale.ROOT).format(date);
        addIndex(resolvedName);
        addView("view1", "FROM emp | WHERE emp.age > 30");
        try {
            LogicalPlan plan = query("FROM <logs-{now/d{yyyy.MM.dd}}>, view1");
            LogicalPlan rewritten = replaceViews(plan);
            assertThat(rewritten, instanceOf(UnionAll.class));
            List<LogicalPlan> subqueries = rewritten.children();
            assertThat(subqueries.size(), equalTo(2));
            assertThat(subqueries.getFirst(), matchesPlan(query("FROM <logs-{now/d{yyyy.MM.dd}}>")));
            assertThat(subqueries.get(1), matchesPlan(query("FROM emp | WHERE emp.age > 30")));
        } catch (AssertionError e) {
            assumeTrue("Date must stay the same during the test", Objects.equals(date, LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC)));
            throw e;
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

    private LogicalPlan replaceViews(LogicalPlan plan) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        viewResolver.replaceViews(plan, this::parse, future);
        return future.actionGet().plan();
    }

    private void addIndex(String name) {
        viewService.addIndex(projectId, name);
    }

    private void addDateMathIndex(String prefix) {
        addIndex(
            prefix + LocalDate.ofInstant(Instant.now(), ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT))
        );
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

    protected LogicalPlan query(String e) {
        return query(e, new QueryParams());
    }

    LogicalPlan query(String e, QueryParams params) {
        return parser.parseQuery(e, params);
    }
}
