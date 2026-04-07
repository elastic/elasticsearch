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
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.esql.ConfigurationTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Abs;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.LogicalVerifier;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelationSerializationTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.session.Configuration;
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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.equalToIgnoringIds;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class InMemoryViewServiceTests extends AbstractStatementParserTests {
    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    static InMemoryViewService viewService;
    static InMemoryViewResolver viewResolver;
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

    @SuppressWarnings("DataFlowIssue")
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs*")));
    }

    public void testExclusionWithDuplicateViewWildcard() {
        addView("logs-1", "FROM emp | WHERE logs.type == nginx");
        LogicalPlan plan = query("FROM logs-*,-logs-1,logs-*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp | WHERE logs.type == nginx")));
    }

    public void testExclusionWithDuplicateViewWildcardAndRemainingIndex() {
        addView("logs-1", "FROM emp | WHERE logs.type == nginx");
        addIndex("logs-2");
        LogicalPlan plan = query("FROM logs-*,-logs-1,logs-*");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(rewritten, instanceOf(UnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(2));
        assertThat(
            subqueries,
            containsInAnyOrder(matchesPlan(query("FROM logs-*,logs-*")), matchesPlan(query("FROM emp | WHERE logs.type == nginx")))
        );
    }

    public void testViewBodyWithExclusionCombined() {
        addView("safe-logs", "FROM logs*,-logs-secret");
        addIndex("logs-public");
        addIndex("logs-secret");
        LogicalPlan plan = query("FROM safe-logs,logs-secret");
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs*,-logs-secret,logs-secret")));
    }

    public void testExclusionWithNoRemainingIndexMatch() {
        addView("logs-nginx", "FROM logs | WHERE logs.type == nginx");
        LogicalPlan plan = query("FROM logs*, -logs-nginx");
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs*")));
    }

    public void testExclusionPreservedForIndexResolution() {
        addView("logs1", "FROM logs2");
        addIndex("logs2");
        addIndex("logs3");
        LogicalPlan plan = query("FROM logs*,-logs3");
        assertThat(replaceViews(plan), matchesPlan(query("FROM logs2,logs*,-logs3")));
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*")));
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,view*")));
    }

    public void testExclusionAllViewsWithIndex() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        LogicalPlan plan = query("FROM view*, -view1, -view2");
        assertThat(replaceViews(plan), matchesPlan(query("FROM view*")));
    }

    public void testExclusionNonExistingResource() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM view*, -donotexist");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,view*,-donotexist")));
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
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp2,view*::data")));
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
        assertThat(rewritten, matchesPlan(query("""
            FROM
            (FROM emp1 | WHERE emp.age > 30),
            (FROM emp2 | WHERE emp.age < 40),
            (FROM emp3 | WHERE emp.salary > 50000)""")));
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

    public void testReplaceViewsWildcardAll() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM *");
        // The * wildcard is preserved because concrete indices from setupTest() (emp, emp1, emp2, emp3, logs)
        // also match *, so hasNonView=true and * passes through for later field caps resolution.
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1, emp2, emp3, *")));
    }

    public void testReplaceViewsWildcardAllNoReferencedIndices() {
        addView("view1", "ROW a = 1");
        addView("view2", "ROW a = 2");
        addView("view3", "ROW a = 3");
        LogicalPlan plan = query("FROM *");
        LogicalPlan rewritten = replaceViews(plan);
        // We cannot express the expected plan using subqueries because they only accept FROM commands, so we check its structure instead
        assertThat(rewritten, instanceOf(ViewUnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(4));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM *")),
                matchesPlan(query("ROW a = 1")),
                matchesPlan(query("ROW a = 2")),
                matchesPlan(query("ROW a = 3"))
            )
        );
    }

    public void testReplaceViewsIndexWildcardAll() {
        addIndex("emp");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM *");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1, emp2, emp3, *")));
    }

    public void testReplaceViewsWildcardWithIndex() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp2,emp3,view*")));
    }

    public void testMixedViewAndIndexMergedUnresolvedRelation() {
        addView("view1", "FROM emp");
        addIndex("index1");
        LogicalPlan plan = query("FROM view1, index1");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(rewritten, instanceOf(UnresolvedRelation.class));
        assertThat(as(rewritten, UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("emp,index1"));
    }

    public void testMissingIndexPreservedWhenMixedWithView() {
        addView("view1", "FROM emp");
        LogicalPlan plan = query("FROM view1, missing-index");
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(as(rewritten, UnresolvedRelation.class).indexPattern().indexPattern(), equalTo("emp,missing-index"));
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
        assertThat(replaceViews(plan), matchesPlan(query("""
            FROM
            (FROM emp1 | WHERE emp.age > 30),
            (FROM emp2 | WHERE emp.age < 40),
            (FROM emp3 | WHERE emp.salary > 50000)""")));
    }

    public void testReplaceViewsPlanWildcardAll() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM *");
        // The * wildcard is preserved because concrete indices from setupTest() also match.
        assertThat(replaceViews(plan), matchesPlan(query("""
            FROM
            (FROM *),
            (FROM emp1 | WHERE emp.age > 30),
            (FROM emp2 | WHERE emp.age < 40),
            (FROM emp3 | WHERE emp.salary > 50000)""")));
    }

    public void testReplaceViewsPlanWildcardWithIndex() {
        addIndex("viewX");
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view*");
        assertThat(replaceViews(plan), matchesPlan(query("""
            FROM
            (FROM view*),
            (FROM emp1 | WHERE emp.age > 30),
            (FROM emp2 | WHERE emp.age < 40),
            (FROM emp3 | WHERE emp.salary > 50000)""")));
    }

    public void testReplaceViewsPlanWildcardWithIndexAll() {
        addIndex("viewX");
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM *");
        assertThat(replaceViews(plan), matchesPlan(query("""
            FROM
            (FROM *),
            (FROM emp1 | WHERE emp.age > 30),
            (FROM emp2 | WHERE emp.age < 40),
            (FROM emp3 | WHERE emp.salary > 50000)""")));
    }

    public void testReplaceViewsNestedWildcard() {
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM emp1,emp3),(FROM emp1,emp2)")));
    }

    public void testReplaceViewsNestedWildcardNonOverlapping() {
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_4", "FROM emp4");
        addView("view_x1", "FROM view_1, view_2");
        addView("view_x2", "FROM view_3, view_4");
        LogicalPlan plan = query("FROM view_x*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp2,emp3,emp4")));
    }

    public void testReplaceViewsNestedWildcardWithIndex() {
        addIndex("view_1_X");
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM emp1,emp3,view_1_*),(FROM emp1,emp2)")));
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
        assertThat(replaceViews(plan), matchesPlan(query("""
            FROM
            (FROM emp1,emp2), (FROM emp1,emp3), (FROM emp2,emp1),
            (FROM emp2,emp3), (FROM emp3,emp1), (FROM emp3,emp2)""")));
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
        LogicalPlan rewritten = replaceViews(plan);
        assertThat(rewritten, matchesPlan(query("""
            FROM
            (FROM emp1,emp3,view_2_*),
            (FROM emp1,emp2),
            (FROM emp2,emp1),
            (FROM emp2,emp3),
            (FROM emp3,emp1),
            (FROM emp3,emp2)""")));
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
        assertThat(rewritten, matchesPlan(query("""
            FROM
            (FROM emp1,emp3,view_1_*,view_2_*,view_3_*),
            (FROM emp1,emp2),
            (FROM emp2,emp1),
            (FROM emp2,emp3),
            (FROM emp3,emp1),
            (FROM emp3,emp2)""")));
    }

    public void testReplaceViewsNestedPlansWildcard() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        LogicalPlan rewritten = replaceViews(plan);
        // Nested ViewUnionAlls are flattened into the parent, so all branches appear at the top level
        assertThat(rewritten, instanceOf(ViewUnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(4));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2 | WHERE emp.age < 40")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000"))
            )
        );
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
        // Nested ViewUnionAlls are flattened into the parent, so all branches appear at the top level
        assertThat(rewritten, instanceOf(ViewUnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(5));
        assertThat(
            subqueries,
            containsInAnyOrder(
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2 | WHERE emp.age < 40")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000")),
                matchesPlan(query("FROM view_1_*"))
            )
        );
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
        assertThat(rewritten, instanceOf(ViewUnionAll.class));
        List<LogicalPlan> subqueries = rewritten.children();
        assertThat(subqueries.size(), equalTo(6));
        for (LogicalPlan child : subqueries) {
            child = (child instanceof Subquery subquery) ? subquery.child() : child;
            assertThat(child, instanceOf(ViewUnionAll.class));
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

    public void testNonCircularViewInMultiSource() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("view_a", "FROM emp");
        addView("view_b", "FROM view_c");
        addView("view_c", "FROM view_a");
        // view_b -> view_c -> view_a -> emp is a valid chain, not circular.
        // view_a appearing as both a direct source and a transitive dependency of view_b is fine.
        // Both resolve to UR("emp") but are kept as separate branches (not merged into "emp,emp")
        // because IndexResolution would deduplicate a single "emp,emp" relation, losing the duplicate.
        assertThat(replaceViews(query("FROM view_a, view_b")), matchesPlan(query("FROM (FROM emp),(FROM emp)")));
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

    public void testCircularViewExcludedByWildcard() {
        addView("v_1", "FROM v_*");
        LogicalPlan plan = query("FROM v_*,-v_1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM v_*")));
    }

    public void testCircularViewExcludedByConcreteExclusion() {
        addView("view_a", "FROM view_b");
        addView("view_b", "FROM view_a");
        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM view_a,-view_b")));
        assertThat(e.getMessage(), containsString("circular view reference 'view_a'"));
    }

    public void testCircularViewBodyWithSelfExclusion() {
        addView("v_1", "FROM v_*,-v_1");
        LogicalPlan plan = query("FROM v_1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM v_*")));
    }

    public void testCircularViewBodyWithSelfExclusionAndIndex() {
        addIndex("v_idx");
        addView("v_1", "FROM v_*,-v_1");
        LogicalPlan plan = query("FROM v_1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM v_*")));
    }

    public void testCircularViewBodyWithSelfExclusionAndOtherView() {
        addView("v_1", "FROM v_*,-v_1");
        addView("v_2", "FROM emp");
        LogicalPlan plan = query("FROM v_1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp")));
    }

    public void testWildcardNonCircularReferenceFromViewSibling() {
        addIndex("idx_otel");
        addIndex("idx_otel_linux");
        addView("wired_otel", "FROM idx_otel, wired_otel_linux");
        addView("wired_otel_linux", "FROM idx_otel_linux");
        addView("wired_otel_query", "FROM wired_otel");

        LogicalPlan plan = query("FROM wired_otel_*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM idx_otel_linux),(FROM idx_otel,idx_otel_linux)")));
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
        assertThat(e.getMessage(), containsString("cannot add view, the maximum number of views is reached: 500"));
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

    // --- Behavioral test: views combined with subqueries ---

    public void testViewInsideSubqueryIsResolved() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("my_view", "FROM emp | WHERE emp.age > 30");
        // Parser produces a plain UnionAll for "FROM index, (FROM subquery)" syntax
        LogicalPlan plan = query("FROM emp2, (FROM my_view)");
        assertThat(plan, instanceOf(UnionAll.class));
        assertThat("Parser should produce plain UnionAll, not ViewUnionAll", plan, not(instanceOf(ViewUnionAll.class)));

        // ViewResolver should recurse into the plain UnionAll and resolve my_view
        LogicalPlan rewritten = replaceViews(plan);
        // The top-level UnionAll should be replaced by ViewUnionAll because it contains a named subquery from the resolved view
        assertThat("Top-level UnionAll should be re-written to ViewUnionAll with view name", rewritten, instanceOf(ViewUnionAll.class));
        // After resolution, the subquery's UnresolvedRelation[my_view] should become
        // the view definition: FROM emp | WHERE emp.age > 30
        List<LogicalPlan> children = rewritten.children();
        assertThat(children.size(), equalTo(2));
        // One child should match the resolved view definition, the other should be emp2
        assertThat(children, containsInAnyOrder(matchesPlan(query("FROM emp | WHERE emp.age > 30")), matchesPlan(query("FROM emp2"))));
    }

    public void testSubqueryInsideViewIsResolved() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("my_view", "FROM emp1, (FROM emp3 | WHERE emp.age > 35) | WHERE emp.age > 30");
        // Parser produces a plain UnionAll for "FROM index, (FROM subquery)" syntax
        LogicalPlan plan = query("FROM emp2, (FROM my_view)");
        assertThat(plan, instanceOf(UnionAll.class));
        assertThat("Parser should produce plain UnionAll, not ViewUnionAll", plan, not(instanceOf(ViewUnionAll.class)));

        // ViewResolver should recurse into the plain UnionAll and resolve my_view
        LogicalPlan rewritten = replaceViews(plan);
        // The top-level UnionAll is replaced by a ViewUnionAll as a result of compacting the nested subqueries
        assertThat(rewritten, instanceOf(UnionAll.class));
        assertThat("Top-level UnionAll should be re-written to ViewUnionAll with view name", rewritten, instanceOf(ViewUnionAll.class));
        // After resolution, the subquery's UnresolvedRelation[my_view] should become
        // the view definition: FROM emp1 | WHERE emp.age > 30
        List<LogicalPlan> children = rewritten.children();
        assertThat(children.size(), equalTo(2));
        // One child should match the resolved view definition, the other should be emp2
        assertThat(
            children,
            containsInAnyOrder(
                matchesPlan(query("FROM emp1, (FROM emp3 | WHERE emp.age > 35) | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2"))
            )
        );
    }

    public void testViewNamedInUnionAll() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("my_view1", "FROM emp1 | WHERE emp.age > 30");
        addView("my_view2", "FROM emp2 | WHERE emp.age > 30");
        // Parser produces a plain UnionAll for "FROM index, (FROM subquery)" syntax
        LogicalPlan plan = query("FROM (FROM my_view1), (FROM my_view2)");
        assertThat(plan, instanceOf(UnionAll.class));
        assertThat("Parser should produce plain UnionAll, not ViewUnionAll", plan, not(instanceOf(ViewUnionAll.class)));

        // ViewResolver should recurse into the plain UnionAll and resolve my_view
        LogicalPlan rewritten = replaceViews(plan);
        // The top-level UnionAll is replaced by a ViewUnionAll as a result of compacting the nested subqueries
        assertThat(
            "Top-level UnionAll should changed to include view names after view resolution",
            rewritten,
            instanceOf(ViewUnionAll.class)
        );
        // The names should match the correct sub-plans
        ViewUnionAll namedUnionAll = (ViewUnionAll) rewritten;
        for (Map.Entry<String, LogicalPlan> entry : namedUnionAll.namedSubqueries().entrySet()) {
            if (entry.getKey().equals("my_view1")) {
                assertThat(entry.getValue(), matchesPlan(query("FROM emp1 | WHERE emp.age > 30")));
            } else if (entry.getKey().equals("my_view2")) {
                assertThat(entry.getValue(), matchesPlan(query("FROM emp2 | WHERE emp.age > 30")));
            } else {
                fail("Unexpected named sub-plan: " + entry);
            }
        }
    }

    public void testIndexCompactionWithNestedNamedSubqueries() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("my_view1", "FROM emp1 | WHERE emp.age > 30");
        addView("my_view2", "FROM emp2, my_view1");
        addView("my_view3", "FROM emp3, my_view2");
        addView("my_view4", "FROM emp4, my_view3");
        LogicalPlan plan = query("FROM emp5, my_view4");
        assertThat(plan, instanceOf(UnresolvedRelation.class));

        // ViewResolver should recurse into the plain UnionAll and resolve my_view
        LogicalPlan rewritten = replaceViews(plan);
        // The top-level UnionAll is replaced by a ViewUnionAll as a result of compacting the nested subqueries
        assertThat(
            "Top-level UnionAll should changed to include view names after view resolution",
            rewritten,
            instanceOf(ViewUnionAll.class)
        );
        // The names should match the correct sub-plans
        ViewUnionAll namedUnionAll = (ViewUnionAll) rewritten;
        for (Map.Entry<String, LogicalPlan> entry : namedUnionAll.namedSubqueries().entrySet()) {
            if (entry.getKey() == null || entry.getKey().equals("main")) {
                assertThat(entry.getValue(), matchesPlan(query("FROM emp5, emp4, emp3, emp2")));
            } else if (entry.getKey().equals("my_view1")) {
                assertThat(entry.getValue(), matchesPlan(query("FROM emp1 | WHERE emp.age > 30")));
            } else {
                fail("Unexpected named sub-plan: " + entry);
            }
        }
    }

    /**
     * When CPS is enabled and a wildcard matches only views (no local indexes), the wildcard is still preserved
     * as an unresolved pattern because remote projects may have matching indexes.
     */
    public void testCPSWildcardPreservedWhenOnlyViewsMatch() {
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        // Without CPS: wildcard is fully replaced (no unresolved pattern)
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1, emp2, emp3")));
        // With CPS: wildcard is preserved alongside the resolved views
        assertThat(replaceViewsWithCPS(plan), matchesPlan(query("FROM emp1, emp2, emp3, view*")));
    }

    /**
     * When CPS is enabled and a wildcard matches views with pipe bodies (no local indexes), the wildcard is preserved.
     */
    public void testCPSWildcardPreservedWithPipeBodiesWhenOnlyViewsMatch() {
        addView("view_1", "FROM emp1 | WHERE emp.age > 30");
        addView("view_2", "FROM emp2 | WHERE emp.age < 40");
        addView("view_3", "FROM emp3 | WHERE emp.salary > 50000");
        LogicalPlan plan = query("FROM view*");
        // Without CPS: 3 subqueries (just the views)
        LogicalPlan withoutCPS = replaceViews(plan);
        assertThat(withoutCPS, instanceOf(ViewUnionAll.class));
        assertThat(withoutCPS.children().size(), equalTo(3));
        // With CPS: 4 subqueries (3 views + the preserved wildcard)
        LogicalPlan withCPS = replaceViewsWithCPS(plan);
        assertThat(withCPS, instanceOf(ViewUnionAll.class));
        assertThat(withCPS.children().size(), equalTo(4));
        assertThat(
            withCPS.children(),
            containsInAnyOrder(
                matchesPlan(query("FROM view*")),
                matchesPlan(query("FROM emp1 | WHERE emp.age > 30")),
                matchesPlan(query("FROM emp2 | WHERE emp.age < 40")),
                matchesPlan(query("FROM emp3 | WHERE emp.salary > 50000"))
            )
        );
    }

    /**
     * When CPS is enabled and a wildcard already matches a local index alongside views, the wildcard is preserved
     * (same as without CPS in this case).
     */
    public void testCPSWildcardWithIndexMatchBehavesLikeNonCPS() {
        addIndex("viewX");
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp2");
        addView("view3", "FROM emp3");
        LogicalPlan plan = query("FROM view*");
        // Both should preserve the wildcard since there's a matching local index
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp2,emp3,view*")));
        assertThat(replaceViewsWithCPS(plan), matchesPlan(query("FROM emp1,emp2,emp3,view*")));
    }

    /**
     * CPS does not affect concrete (non-wildcard) view references — they are still fully replaced.
     * This is because we separately report the view names to the index resolution layer for CPS anyway.
     */
    public void testCPSConcreteViewFullyReplaced() {
        addView("view1", "FROM emp1");
        LogicalPlan plan = query("FROM view1");
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1")));
        assertThat(replaceViewsWithCPS(plan), matchesPlan(query("FROM emp1")));
    }

    /**
     * When CPS is enabled and nested views use wildcards that match only views, wildcards are preserved.
     */
    public void testCPSNestedWildcardPreserved() {
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_1_2", "FROM view_1, view_2");
        addView("view_1_3", "FROM view_1, view_3");
        LogicalPlan plan = query("FROM view_1_*");
        // Without CPS: wildcard fully replaced
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM emp1,emp2),emp1,emp3")));
        // With CPS: wildcard preserved
        assertThat(replaceViewsWithCPS(plan), matchesPlan(query("FROM (FROM emp1,emp2),emp1,emp3,view_1_*")));
    }

    /**
     * When CPS is enabled and nested views use wildcards that match only views, wildcards are preserved.
     */
    public void testCPSNestedWildcardPreservedWithCompaction() {
        addView("view_1", "FROM emp1");
        addView("view_2", "FROM emp2");
        addView("view_3", "FROM emp3");
        addView("view_4", "FROM emp4");
        addView("view_x1", "FROM view_1, view_2");
        addView("view_x2", "FROM view_3, view_4");
        LogicalPlan plan = query("FROM view_x*");
        // Without CPS: wildcard fully replaced
        assertThat(replaceViews(plan), matchesPlan(query("FROM emp1,emp2,emp3,emp4")));
        // With CPS: wildcard preserved
        assertThat(replaceViewsWithCPS(plan), matchesPlan(query("FROM emp1,emp2,emp3,emp4,view_x*")));
    }

    /*
     * Test that de-duplication only works when identical indices start out in the same index-pattern,
     * not when they come from views, which should act like separate indexes.
     */
    public void testCompactionWithRepeatingView() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("view1", "FROM emp1");
        addView("view2", "FROM emp1, emp1");
        addIndex("emp1");
        // Tests with view containing single index
        assertThat(replaceViews(query("FROM view1")), matchesPlan(query("FROM emp1")));
        assertThat(replaceViews(query("FROM view1,view1")), matchesPlan(query("FROM emp1")));
        assertThat(replaceViews(query("FROM view1, view1, view1, view1")), matchesPlan(query("FROM emp1")));
        assertThat(replaceViews(query("FROM emp1,view1")), matchesPlan(query("FROM emp1, (FROM emp1)")));
        assertThat(replaceViews(query("FROM view1, emp1")), matchesPlan(query("FROM (FROM emp1), emp1")));
        assertThat(replaceViews(query("FROM emp1, view1, emp1")), matchesPlan(query("FROM emp1, (FROM emp1), emp1")));
        assertThat(replaceViews(query("FROM emp1, view1, emp1, emp1")), matchesPlan(query("FROM emp1, (FROM emp1), emp1, emp1")));
        // Tests with view containing repeating index
        assertThat(replaceViews(query("FROM view2")), matchesPlan(query("FROM emp1,emp1")));
        assertThat(replaceViews(query("FROM view2,view2")), matchesPlan(query("FROM emp1,emp1")));
        assertThat(replaceViews(query("FROM view2, view2, view2, view2")), matchesPlan(query("FROM emp1,emp1")));
        assertThat(replaceViews(query("FROM emp1,view2")), matchesPlan(query("FROM emp1, (FROM emp1,emp1)")));
        assertThat(replaceViews(query("FROM view2, emp1")), matchesPlan(query("FROM (FROM emp1,emp1), emp1")));
        assertThat(replaceViews(query("FROM emp1, view2, emp1")), matchesPlan(query("FROM emp1, (FROM emp1, emp1), emp1")));
        assertThat(replaceViews(query("FROM emp1, view2, emp1, emp1")), matchesPlan(query("FROM emp1, (FROM emp1, emp1), emp1, emp1")));
    }

    /**
     * Further testing of the de-duplication fix.
     * This test is designed to mimic the test csv-spec:views.compactedNestedViewWithIndexGivesTripleCopies
     */
    public void testCompactedNestedRepeating() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("employees_all", "FROM employees");
        addView("employees_extra", "FROM employees, employees_all");
        addIndex("employees");
        assertThat(replaceViews(query("FROM employees_all")), matchesPlan(query("FROM employees")));
        assertThat(
            replaceViews(query("FROM employees_all | STATS count=COUNT()")),
            matchesPlan(query("FROM employees | STATS count=COUNT()"))
        );
        assertThat(
            replaceViews(query("FROM employees, employees_all | STATS count=COUNT()")),
            matchesPlan(query("FROM employees, (FROM employees) | STATS count=COUNT()"))
        );
        assertThat(
            replaceViews(query("FROM employees_all, employees_all | STATS count=COUNT()")),
            matchesPlan(query("FROM employees | STATS count=COUNT()"))
        );
        assertThat(
            replaceViews(query("FROM employees_extra | STATS count=COUNT()")),
            matchesPlan(query("FROM employees, (FROM employees) | STATS count=COUNT()"))
        );
        // Nested ViewUnionAlls that only contain UnresolvedRelations are flattened into the parent,
        // so the nested structure is eliminated and each UR becomes a separate branch.
        assertThat(
            replaceViews(query("FROM employees, employees_extra | STATS count=COUNT()")),
            matchesPlan(query("FROM employees, (FROM employees), (FROM employees) | STATS count=COUNT()"))
        );
        assertThat(
            replaceViews(query("FROM employees_extra | STATS count=COUNT()")),
            matchesPlan(query("FROM employees, (FROM employees) | STATS count=COUNT()"))
        );
    }

    /**
     * Reproduces a bug in {@code mergeUnresolvedRelationEntries} where {@code hasPatternDuplicates}
     * only checks whole-pattern equality, not individual index name overlap. Two inner VUAs can each
     * contribute a bare UR with different whole patterns (e.g. "emp1,emp2" vs "emp1,emp3") that share
     * an individual index ("emp1"). After flattening, {@code mergeUnresolvedRelationEntries} merges
     * them into "emp1,emp2,emp1,emp3" because the whole patterns differ — but IndexResolution would
     * then deduplicate "emp1", losing data.
     */
    public void testFlattenedViewUnionAllWithOverlappingIndices() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        // Non-compactable views (have WHERE) that force NamedSubquery branches
        addView("view_x", "FROM emp1 | WHERE emp.age > 30");
        addView("view_y", "FROM emp1 | WHERE emp.salary > 50000");
        // Each of these resolves to a VUA with a bare UR("emp1,emp2") or UR("emp1,emp3")
        // plus a NamedSubquery for the non-compactable view
        addView("view_a", "FROM emp2, emp1, view_x");
        addView("view_b", "FROM emp3, emp1, view_y");
        addIndex("emp1");
        addIndex("emp2");
        addIndex("emp3");

        // Query FROM view_a, view_b produces a VUA with two inner VUAs.
        // tryFlattenViewUnionAll lifts the entries. The bare URs "emp1,emp2" and "emp1,emp3"
        // share "emp1" — merging them would lose data.
        LogicalPlan result = replaceViews(query("FROM view_a, view_b"));
        assertThat(result, instanceOf(ViewUnionAll.class));
        ViewUnionAll vua = (ViewUnionAll) result;

        // Count how many branches resolve to a bare UR containing "emp1"
        // If the bug is present, the URs get merged into one, losing the second copy of emp1
        long urBranchesWithEmp1 = vua.namedSubqueries()
            .values()
            .stream()
            .filter(p -> p instanceof UnresolvedRelation)
            .map(p -> ((UnresolvedRelation) p).indexPattern().indexPattern())
            .filter(pattern -> Arrays.asList(pattern.split(",")).contains("emp1"))
            .count();

        assertThat(
            "emp1 should appear in at least 2 separate UR branches to preserve view semantics, "
                + "but mergeUnresolvedRelationEntries merged them because hasPatternDuplicates only checks whole patterns. "
                + "VUA entries: "
                + vua.namedSubqueries(),
            urBranchesWithEmp1,
            greaterThanOrEqualTo(2L)
        );
    }

    /**
     * Tests a 12x10 matrix of nesting depth x branching width with compactable views.
     * Compactable views are simple aliases (just {@code FROM <target>}), which the ViewResolver
     * compacts into a single FROM clause, eliminating any FORK branching.
     * Nesting depths beyond the max view depth (default 10) hit a depth limit error.
     */
    public void testCompactableViewNestingBranchingMatrix() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        int maxViewDepth = ViewResolver.MAX_VIEW_DEPTH_SETTING.getDefault(Settings.EMPTY);
        try (var matrixService = matrixViewService()) {
            var matrixResolver = matrixService.getViewResolver();
            for (int nesting = 1; nesting <= 12; nesting++) {
                for (int branching = 1; branching <= 10; branching++) {
                    matrixService.clearAllViewsAndIndices();
                    buildNestingBranchingViewTree(nesting, branching, (d, b) -> false, matrixService);
                    final String queryStr = buildNestingBranchingQuery(nesting, branching, false);

                    if (nesting > maxViewDepth) {
                        var e = expectThrows(VerificationException.class, () -> replaceViews(query(queryStr), matrixResolver));
                        assertThat(
                            "nesting=" + nesting + ", branching=" + branching,
                            e.getMessage(),
                            startsWith("The maximum allowed view depth of " + maxViewDepth + " has been exceeded")
                        );
                    } else {
                        LogicalPlan result = replaceViews(query(queryStr), matrixResolver);
                        assertThat(
                            "Compactable views should compact to a single UnresolvedRelation"
                                + " for nesting="
                                + nesting
                                + ", branching="
                                + branching,
                            result,
                            instanceOf(UnresolvedRelation.class)
                        );
                    }
                }
            }
        }
    }

    /**
     * Tests a 12x10 matrix with non-compactable views only on the diagonal (where depth == branch).
     * Wrapper views (branch 1 at depth &ge; 2) are always compactable, so the ViewResolver can flatten
     * nested ViewUnionAlls through them. The result is a single-level ViewUnionAll that accumulates
     * one branch per diagonal view. The effective FORK branch count grows with min(N, B), hitting the
     * FORK limit when the total (diagonal count + query-level diagonal + 1 for compactable UR) exceeds 8.
     * No nested FORK errors occur because flattening eliminates all nesting.
     */
    public void testDiagonalNonCompactableViewNestingBranchingMatrix() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        int maxViewDepth = ViewResolver.MAX_VIEW_DEPTH_SETTING.getDefault(Settings.EMPTY);
        try (var matrixService = matrixViewService()) {
            var matrixResolver = matrixService.getViewResolver();
            for (int nesting = 1; nesting <= 12; nesting++) {
                for (int branching = 1; branching <= 10; branching++) {
                    matrixService.clearAllViewsAndIndices();
                    buildNestingBranchingViewTree(nesting, branching, (d, b) -> d == b, matrixService);
                    final String queryStr = buildNestingBranchingQuery(nesting, branching, true);

                    if (nesting > maxViewDepth) {
                        var e = expectThrows(VerificationException.class, () -> replaceViews(query(queryStr), matrixResolver));
                        assertThat(
                            "nesting=" + nesting + ", branching=" + branching,
                            e.getMessage(),
                            startsWith("The maximum allowed view depth of " + maxViewDepth + " has been exceeded")
                        );
                    } else {
                        LogicalPlan result = replaceViews(query(queryStr), matrixResolver);
                        assertNotNull("Diagonal resolution should succeed for nesting=" + nesting + ", branching=" + branching, result);
                        // When flattening stays within MAX_BRANCHES, nesting is eliminated and no nested FORK errors occur.
                        // When flattening would exceed MAX_BRANCHES, it is skipped, keeping nested ViewUnionAlls.
                        if (branching >= 2 && effectiveDiagonalBranches(nesting, branching) <= Fork.MAX_BRANCHES) {
                            Failures failures = new Failures();
                            Failures depFailures = new Failures();
                            LogicalVerifier.INSTANCE.checkPlanConsistency(result, failures, depFailures);
                            assertFalse(
                                "No nested FORK errors expected for nesting="
                                    + nesting
                                    + ", branching="
                                    + branching
                                    + " but got: "
                                    + failures,
                                failures.hasFailures()
                            );
                        }
                    }
                }
            }
        }
    }

    /**
     * Computes the effective FORK branch count for the diagonal non-compaction pattern.
     * Each diagonal view (depth == branch) becomes a non-compactable branch. Compactable views merge
     * into a single UR entry. The total is: diagonal count from tree + query-level diagonal + 1 (UR).
     */
    private static int effectiveDiagonalBranches(int nesting, int branching) {
        int diagonal = Math.min(nesting, branching); // v_1_1, v_2_2, ..., v_min(N,B)_min(N,B)
        int queryDiagonal = (nesting + 1 <= branching) ? 1 : 0; // v_(N+1)_(N+1) if it exists
        return diagonal + queryDiagonal + 1; // +1 for the merged compactable UR
    }

    /**
     * Tests a 12x10 matrix of nesting depth x branching width with non-compactable views.
     * Non-compactable views have a LIMIT command (e.g., {@code FROM idx | LIMIT 1000}) that prevents
     * compaction, forcing each view reference to become a separate branch in a FORK.
     * <p>
     * The view tree has branching at every nesting level including the query itself:
     * <ul>
     *   <li>Level 1: {@code branching} leaf views, each referencing a unique index</li>
     *   <li>Level 2: a wrapper view referencing all level-1 leaves</li>
     *   <li>Level k (k &gt; 2): a wrapper view referencing the level-(k-1) wrapper + (branching-1) new leaf views,
     *       maintaining {@code branching} total FORK branches at each level</li>
     *   <li>Query level: the top wrapper + (branching-1) extra leaf views</li>
     * </ul>
     * Expected outcomes:
     * <ul>
     *   <li>nesting &gt; max view depth (10): view depth exceeded error (takes priority)</li>
     *   <li>branching &gt; {@link Fork#MAX_BRANCHES}: FORK branching error at the first level with too many branches</li>
     *   <li>branching &le; {@link Fork#MAX_BRANCHES}: resolution succeeds, producing nested {@link ViewUnionAll}
     *       structures for nesting &ge; 2 with branching &ge; 2</li>
     * </ul>
     */
    public void testNonCompactableViewNestingBranchingMatrix() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        int maxViewDepth = ViewResolver.MAX_VIEW_DEPTH_SETTING.getDefault(Settings.EMPTY);
        LogicalVerifier verifier = LogicalVerifier.INSTANCE;
        try (var matrixService = matrixViewService()) {
            var matrixResolver = matrixService.getViewResolver();
            for (int nesting = 1; nesting <= 12; nesting++) {
                for (int branching = 1; branching <= 10; branching++) {
                    matrixService.clearAllViewsAndIndices();
                    buildNestingBranchingViewTree(nesting, branching, (d, b) -> true, matrixService);
                    final String queryStr = buildNestingBranchingQuery(nesting, branching, true);

                    if (nesting > maxViewDepth) {
                        var e = expectThrows(VerificationException.class, () -> replaceViews(query(queryStr), matrixResolver));
                        assertThat(
                            "nesting=" + nesting + ", branching=" + branching,
                            e.getMessage(),
                            startsWith("The maximum allowed view depth of " + maxViewDepth + " has been exceeded")
                        );
                    } else if (branching > Fork.MAX_BRANCHES) {
                        var e = expectThrows(IllegalArgumentException.class, () -> replaceViews(query(queryStr), matrixResolver));
                        assertThat(
                            "nesting=" + nesting + ", branching=" + branching,
                            e.getMessage(),
                            containsString("FORK supports up to " + Fork.MAX_BRANCHES + " branches")
                        );
                    } else {
                        LogicalPlan result = replaceViews(query(queryStr), matrixResolver);
                        assertNotNull(
                            "Non-compactable resolution should succeed for nesting=" + nesting + ", branching=" + branching,
                            result
                        );
                        if (branching >= 2) {
                            Failures failures = new Failures();
                            Failures depFailures = new Failures();
                            verifier.checkPlanConsistency(result, failures, depFailures);
                            if (nesting >= 2) {
                                assertTrue(
                                    "Expected nested ViewUnionAll for nesting=" + nesting + ", branching=" + branching,
                                    containsNestedViewUnionAll(result)
                                );
                                assertThat("Expect failure count", failures.failures().size(), equalTo(nesting - 1));
                                // Each nested ViewUnionAll failure should reference the view that created it.
                                // The ViewUnionAlls at depths 2..N have view names v_2_1..v_N_1.
                                for (Failure failure : failures.failures()) {
                                    assertThat(failure.failMessage(), containsString("Nested subqueries are not supported"));
                                    assertThat(failure.failMessage(), containsString("(in view [v_"));
                                }
                            } else {
                                assertFalse(
                                    "No nested ViewUnionAll expected for nesting=" + nesting + ", branching=" + branching,
                                    containsNestedViewUnionAll(result)
                                );
                                assertFalse(
                                    "No failures expected for nesting=" + nesting + ", branching=" + branching,
                                    failures.hasFailures()
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Builds a view tree for testing nesting and branching combinations.
     * <p>
     * All views follow the naming convention {@code v_<depth>_<branch>}, where depth 1 is the
     * deepest (leaf) level and depth N is the shallowest (closest to the query). Branch 1 at
     * each depth is the "chain" view that references the level below; branches 2..B are leaf views.
     * <p>
     * Structure for (nesting=N, branching=B):
     * <ul>
     *   <li>Depth 1: B leaf views {@code v_1_1..v_1_B}, each referencing a unique index</li>
     *   <li>Depth k (k &ge; 2): leaf views {@code v_k_2..v_k_B} + wrapper {@code v_k_1} referencing
     *       {@code v_(k-1)_1} plus those leaves</li>
     *   <li>Query level (N &ge; 2): {@code v_(N+1)_2..v_(N+1)_B} extra leaf views so the query has B branches</li>
     * </ul>
     * Every level including the query has B total view references, ensuring B FORK branches at every level
     * when non-compactable.
     * <p>
     * The {@code blocksCompaction} predicate controls which views get {@code | LIMIT 1000} appended,
     * receiving (depth, branch) for each view. Views where the predicate returns true become non-compactable.
     */
    private void buildNestingBranchingViewTree(
        int nesting,
        int branching,
        BiPredicate<Integer, Integer> blocksCompaction,
        InMemoryViewService service
    ) {
        // Depth 1: leaf views referencing unique indices
        for (int j = 1; j <= branching; j++) {
            String idx = "idx_1_" + j;
            service.addIndex(projectId, idx);
            addView("v_1_" + j, "FROM " + idx + viewSuffix(blocksCompaction, 1, j), service);
        }

        // Depths 2 through N: leaf views v_k_2..v_k_B + wrapper v_k_1 referencing v_(k-1)_1 and the new leaves
        for (int k = 2; k <= nesting; k++) {
            for (int j = 2; j <= branching; j++) {
                String idx = "idx_" + k + "_" + j;
                service.addIndex(projectId, idx);
                addView("v_" + k + "_" + j, "FROM " + idx + viewSuffix(blocksCompaction, k, j), service);
            }
            StringBuilder sb = new StringBuilder("FROM v_").append(k - 1).append("_1");
            for (int j = 2; j <= branching; j++) {
                sb.append(", v_").append(k).append("_").append(j);
            }
            addView("v_" + k + "_1", sb + viewSuffix(blocksCompaction, k, 1), service);
        }

        if (nesting < 2) {
            return;
        }

        // Query-level leaf views (depth N+1) so the top-level query also has B branches
        int qDepth = nesting + 1;
        for (int j = 2; j <= branching; j++) {
            String idx = "idx_" + qDepth + "_" + j;
            service.addIndex(projectId, idx);
            addView("v_" + qDepth + "_" + j, "FROM " + idx + viewSuffix(blocksCompaction, qDepth, j), service);
        }
    }

    private static String viewSuffix(BiPredicate<Integer, Integer> blocksCompaction, int depth, int branch) {
        return blocksCompaction.test(depth, branch) ? " | LIMIT 1000" : "";
    }

    /**
     * Builds the query string for the nesting/branching matrix test.
     * For nesting=1, queries all depth-1 views directly.
     * For nesting &ge; 2, queries the top wrapper {@code v_N_1} plus leaf views {@code v_(N+1)_2..v_(N+1)_B},
     * ensuring B branches at the query level too.
     * When {@code withLimit} is true, appends {@code | LIMIT 1000} to mirror real ES|QL behaviour
     * (which always adds an implicit limit).
     */
    private String buildNestingBranchingQuery(int nesting, int branching, boolean withLimit) {
        String suffix = withLimit ? " | LIMIT 1000" : "";
        if (nesting == 1) {
            StringBuilder sb = new StringBuilder("FROM ");
            for (int j = 1; j <= branching; j++) {
                if (j > 1) sb.append(", ");
                sb.append("v_1_").append(j);
            }
            return sb + suffix;
        }
        StringBuilder sb = new StringBuilder("FROM v_").append(nesting).append("_1");
        int qDepth = nesting + 1;
        for (int j = 2; j <= branching; j++) {
            sb.append(", v_").append(qDepth).append("_").append(j);
        }
        return sb + suffix;
    }

    /**
     * Checks whether the plan contains nested {@link ViewUnionAll} nodes,
     * i.e., a ViewUnionAll whose subtree contains another ViewUnionAll.
     */
    private static boolean containsNestedViewUnionAll(LogicalPlan plan) {
        boolean[] found = { false };
        plan.forEachDown(ViewUnionAll.class, outer -> {
            if (found[0]) return;
            for (LogicalPlan child : outer.children()) {
                child.forEachDown(ViewUnionAll.class, inner -> found[0] = true);
            }
        });
        return found[0];
    }

    /**
     * Creates a view service with increased max views count for the nesting/branching matrix tests.
     */
    private InMemoryViewService matrixViewService() {
        return viewService.withSettings(Settings.builder().put(ViewService.MAX_VIEWS_COUNT_SETTING.getKey(), 200).build());
    }

    private LogicalPlan replaceViews(LogicalPlan plan) {
        return replaceViews(plan, viewResolver);
    }

    private LogicalPlan replaceViews(LogicalPlan plan, ViewResolver resolver) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        resolver.replaceViews(plan, this::parse, future);
        return future.actionGet().plan();
    }

    private LogicalPlan replaceViewsWithCPS(LogicalPlan plan) {
        var cpsDecider = new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", true).build());
        InMemoryViewResolver cpsResolver = viewService.getViewResolver(cpsDecider);
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        cpsResolver.replaceViews(plan, this::parse, future);
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
        return TEST_PARSER.parseView(query, queryParams, new SettingsValidationContext(false, false), EMPTY_INFERENCE_SETTINGS, viewName)
            .plan();
    }

    /**
     * Matcher that finds equivalence between views-based plans and sub-query based plans.
     * It currently ignores superfluous Subquery nodes and matches ViewUnionAll and UnionAll based
     * on sorting children.
     * If we fixed subquery parsing we could skip the sorting of children, and assert on child order too.
     */
    private static Matcher<LogicalPlan> matchesPlan(LogicalPlan plan) {
        return new TestLogicalPlanEqualTo(plan);
    }

    private static class TestLogicalPlanEqualTo extends BaseMatcher<LogicalPlan> {

        private final LogicalPlan expected;

        private TestLogicalPlanEqualTo(LogicalPlan expected) {
            this.expected = normalize(expected);
        }

        /**
         * Normalize view-based and sub-query based plans to the same structure, for easier comparison.
         * If Subqueries did not sort concrete indexes first and did not add superfluous Subquery nodes
         * we could simplify this a lot.
         * We also rewrite Alias name-ids to 0 because some queries will have different ids due to differences
         * in the way subqueries and views are processed.
         */
        private static LogicalPlan normalize(LogicalPlan plan) {
            // Transform up, to remove Subquery from children before sorting them
            return plan.transformUp(LogicalPlan.class, p -> switch (p) {
                // UnionAll here covers also ViewUnionAll, because we need to sort children of both
                case UnionAll v -> new UnionAll(v.source(), sortedChildren(v.children()), v.output());
                case Subquery s -> s.child(); // TODO: Remove Subquery node from subquery parsing
                default -> p.transformExpressionsDown(Alias.class, a -> a.withId(new NameId(0)));
            });
        }

        /**
         * Unfortunately the sub-query parser at LogicalPlanBuilder.visitRelation sorts indexes before subqueries,
         * so we need to make child sorting common here too.
         * TODO: Preferably change the subquery parsing to not sort like this.
         */
        private static List<LogicalPlan> sortedChildren(List<LogicalPlan> children) {
            return children.stream().sorted(Comparator.comparing(Node::toString)).toList();
        }

        @Override
        public boolean matches(Object o) {
            if (o instanceof LogicalPlan other) {
                return expected.toString().equals(normalize(other).toString());
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            if (expected != null) {
                description.appendText(expected.toString());
            } else {
                description.appendText("null");
            }
        }

        @Override
        public void describeMismatch(Object item, Description description) {
            if (item instanceof LogicalPlan other) {
                description.appendText("was ").appendValue(normalize(other));
            } else {
                description.appendText("was ").appendValue(item);
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
            current.removeLast();
        }
    }

    protected LogicalPlan query(String e) {
        return query(e, new QueryParams());
    }

    LogicalPlan query(String e, QueryParams params) {
        return TEST_PARSER.parseQuery(e, params);
    }
}
