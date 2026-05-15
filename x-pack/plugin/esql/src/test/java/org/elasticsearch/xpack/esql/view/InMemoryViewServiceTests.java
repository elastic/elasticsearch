/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewShadowRelation;
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
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
        // The view body's -logs-secret exclusion stays scoped to the view — it must not widen
        // onto the sibling outer pattern "logs-secret", which explicitly asks to include it.
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM logs*,-logs-secret),(FROM logs-secret)")));
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

    /**
     * Exclusion patterns must be preserved at their original position in the unresolved-pattern
     * list, not appended after positive patterns. Pattern order matters because
     * {@code IndexAbstractionResolver} processes exclusions against prior accumulated state —
     * reordering {@code -*-view-*} from before {@code data-view-*} to after it changes the
     * semantics (the exclusion would now strip {@code data-view-*} matches from the result).
     * <p>
     * {@code match-nothing-*} is dropped here because its expansion is empty+SUCCESS in the
     * null-fallback path. In the security-enabled path it would be NOT_VISIBLE and preserved,
     * but the exclusion-order bug exists on either path.
     */
    public void testExclusionPreservesOriginalOrder() {
        addIndex("data-index-origin");
        addIndex("data-view-extra");
        addView("data-view-origin", "FROM data-index-origin");
        LogicalPlan plan = query("FROM match-nothing-*,-*-view-*,data-view-*");
        assertThat(replaceViews(plan), matchesPlan(query("FROM data-index-origin,-*-view-*,data-view-*")));
    }

    /**
     * A view body's exclusion must stay scoped to its own body. When the body is a simple
     * {@link UnresolvedRelation} containing an exclusion, it must not be merged with sibling or
     * outer {@link UnresolvedRelation}s, because the merge concatenates patterns into one
     * {@link UnresolvedRelation} and widens the scope of the exclusion to everything in the
     * combined pattern list.
     * <p>
     * Real-world failure (ServerlessCrossProjectEsqlIT):
     * <pre>
     *   indices: index-a1, index-a2, index-b1, index-b2
     *   view data-view = FROM index-b*,-*2        (scopes to index-b*, excludes index-b2)
     *   query: FROM index-a*,data-view            (expects index-a1, index-a2, index-b1)
     * </pre>
     * Before the fix the resolver merged into {@code UnresolvedRelation(index-a*,index-b*,-*2)}
     * so the view body's {@code -*2} excluded {@code index-a2} as well, producing only
     * {@code a1, b1}.
     */
    public void testViewBodyExclusionNotLeakedToOuter() {
        addIndex("index-a1");
        addIndex("index-a2");
        addIndex("index-b1");
        addIndex("index-b2");
        addView("data-view", "FROM index-b*,-*2");
        LogicalPlan plan = query("FROM index-a*,data-view");
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM index-a*),(FROM index-b*,-*2)")));
    }

    /**
     * Reproduces the bug where an inner view's exclusion leaks to an outer view's sibling indices
     * across multiple levels of nesting.
     * <p>
     * Setup:
     * <pre>
     *   indices:    index-1-a, index-1-b, index-2-a, index-2-b,
     *               view-1-r-a, view-1-r-b, view-2-r-a, view-2-r-b
     *   view-2-l:   FROM index-2-*,-*a          (resolves to index-2-b)
     *   view-1-l:   FROM view-2-*,index-1-*,-*b (matches view-2-l + view-2-r-*, index-1-*; excludes *b)
     *   view-0-l:   FROM view-1-*               (matches view-1-l + view-1-r-*)
     * </pre>
     * Query: {@code FROM view-0-l}
     * <p>
     * The {@code -*b} exclusion in {@code view-1-l}'s body must stay scoped to {@code view-1-l} —
     * it must not leak to the outer {@code view-0-l} and cause {@code view-1-r-b} to be excluded.
     * <p>
     * The single-level case is covered by {@link #testViewBodyExclusionNotLeakedToOuter}. In the
     * nested case the view-flattening path in {@code ViewResolver.tryFlattenViewUnionAll} would
     * lift the inner ViewUnionAll's entries (ViewUnionAll extends UnionAll extends Fork, which
     * triggers the fork-flattening branch) and then merge their bare {@link UnresolvedRelation}s
     * with sibling outer {@link UnresolvedRelation}s, re-widening the exclusion's scope. The fix
     * wraps exclusion-bearing {@link UnresolvedRelation}s in a NamedSubquery before lifting so the
     * subsequent merge step leaves them alone.
     */
    public void testViewBodyExclusionNotLeakedThroughNestedViews() {
        addIndex("index-1-a");
        addIndex("index-1-b");
        addIndex("index-2-a");
        addIndex("index-2-b");
        addIndex("view-1-r-a");
        addIndex("view-1-r-b");
        addIndex("view-2-r-a");
        addIndex("view-2-r-b");
        addView("view-2-l", "FROM index-2-*,-*a");
        addView("view-1-l", "FROM view-2-*,index-1-*,-*b");
        addView("view-0-l", "FROM view-1-*");
        LogicalPlan plan = query("FROM view-0-l");
        // The inner -*b exclusion must stay scoped to view-1-l's body, not widen to view-0-l's
        // sibling view-1-r-* indices. Each scope-carrying UnresolvedRelation stays in its own branch.
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM view-1-*),(FROM index-2-*,-*a),(FROM view-2-*,index-1-*,-*b)")));
    }

    /**
     * A user-written subquery inside a view body carries its own scope. When its parent view is
     * composed with sibling outer patterns, the subquery's exclusion must not widen to the outer
     * patterns.
     * <p>
     * Without the fix in {@code tryFlattenViewUnionAll}'s fork-child branch, the inner subquery's
     * {@code -*b} would merge with the outer {@code inner-b} pattern and wrongly exclude it.
     */
    public void testUserSubqueryExclusionInViewBodyDoesNotLeakToOuter() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addIndex("outer-a");
        addIndex("outer-b");
        addIndex("inner-a");
        addIndex("inner-b");
        addView("my_view", "FROM outer-*, (FROM inner-*,-*b)");
        LogicalPlan plan = query("FROM my_view, inner-b");
        // The -*b in the inner subquery must only exclude -*b from matches of inner-*, not from the
        // outer inner-b pattern. Each scope-carrying UnresolvedRelation stays in its own branch.
        assertThat(replaceViews(plan), matchesPlan(query("FROM (FROM inner-b,outer-*),(FROM inner-*,-*b)")));
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
        // Both resolve to UnresolvedRelation("emp") but are kept as separate branches (not merged into "emp,emp")
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

    /**
     * Reproduces https://github.com/elastic/elasticsearch/issues/146665
     * FORK queries that reference no views should succeed even when circular views exist on the cluster.
     */
    public void testForkWithCircularViewsOnClusterButNotReferenced() {
        assumeTrue("Requires FORK support", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        addView("view_x", "FROM view_y");
        addView("view_y", "FROM view_x");
        addIndex("logs-001");
        // FORK query with wildcard that matches only the index, not the circular views
        LogicalPlan plan = query("FROM logs-* | FORK (STATS c = COUNT(*)) (LIMIT 5)");
        LogicalPlan result = replaceViews(plan);
        assertNotNull("FORK query should resolve without circular view errors", result);
        assertThat("Plan did not change, no views matched", result, matchesPlan(plan));
    }

    /**
     * Reproduces https://github.com/elastic/elasticsearch/issues/146665
     * FORK query with FROM * but excluding the circular views should succeed.
     */
    public void testForkWithStarWildcardExcludingCircularViews() {
        assumeTrue("Requires FORK support", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        addView("view_x", "FROM view_y");
        addView("view_y", "FROM view_x");
        addIndex("logs");
        // FROM *,-view_* excludes the circular views — should succeed
        LogicalPlan plan = query("FROM *,-view_* | FORK (STATS c = COUNT(*)) (LIMIT 5)");
        LogicalPlan result = replaceViews(plan);
        assertNotNull("FORK query excluding circular views should resolve without errors", result);
        assertThat("Plan did not change, no views matched", result, matchesPlan(plan));
    }

    /**
     * Reproduces <a href="https://github.com/elastic/elasticsearch/issues/146208">#146208</a>.
     * FROM *,-employees* against a cluster populated with the csv-spec views (several simple
     * views whose bodies share underlying wildcard patterns, plus employee views that are
     * excluded by the outer pattern) should not trigger a false circular view reference.
     * <p>
     * This mirrors the manual-server reproduction: the bug only fires when a recursive
     * re-visit of an already-resolved UnresolvedRelation issues a view-resolve request with
     * empty indices, which the security layer expands to "_all". The fix short-circuits that
     * re-entry in {@link ViewResolver#replaceViewsUnresolvedRelation} when all patterns have
     * already been consumed by seenWildcards.
     */
    public void testFromStarExcludingEmployeesWithCsvSpecViews_Issue146208() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        // Indices referenced by the csv-spec view bodies (plus "logs" from default setup)
        addIndex("addresses");
        addIndex("airports");
        addIndex("airports_mp");
        addIndex("languages_lookup_non_unique_key");
        addIndex("employees");

        // Non-employee views from views.csv-spec — none reference each other, but their bodies
        // share underlying index patterns (airports, addresses) that the outer FROM * expands to.
        addView("country_addresses", "FROM addresses | STATS count=COUNT() BY country");
        addView("country_languages", "FROM languages_lookup_non_unique_key | STATS count=COUNT() BY country");
        addView("airports_mp_filtered", "FROM airports | LOOKUP JOIN airports_mp ON abbrev == abbrev");
        addView("country_airports", "FROM airports | STATS count=COUNT() BY country");

        // Employee views — all excluded by the outer -employees* pattern
        addView("employees_all", "FROM employees");
        addView("employees_extra", "FROM employees, employees_all");
        addView("employees_rehired", "FROM employees | WHERE is_rehired == true");
        addView("employees_not_rehired", "FROM employees | WHERE is_rehired == false");

        // Must not throw a circular view reference error.
        LogicalPlan result = replaceViews(query("FROM *,-employees* | LIMIT 1"));
        assertNotNull("FROM *,-employees* should resolve without false circular reference errors", result);
        assertThat("Should match four views and one index pattern", result, matchesPlan(query("""
            FROM *,-employees*,
            (FROM addresses | STATS count=COUNT() BY country),
            (FROM languages_lookup_non_unique_key | STATS count=COUNT() BY country),
            (FROM airports | LOOKUP JOIN airports_mp ON abbrev == abbrev),
            (FROM airports | STATS count=COUNT() BY country)
            | LIMIT 1""")));
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

    /**
     * Reproduces the bug where composing two views that share an underlying index pattern
     * inside a parent view incorrectly triggers a "circular view reference" error.
     * The error_triage view uses inline subqueries (FROM (subquery), ...) and one of those
     * subqueries references svc-auth-* which is also the target of the suspicious_ips view.
     * Neither view references the other, so there is no circular reference.
     */
    public void testFalseCircularReferenceWithSharedWildcardInSubqueries() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        // Set up concrete indices matching the wildcard patterns
        addIndex("svc-gateway-logs");
        addIndex("svc-payments-logs");
        addIndex("svc-auth-logs");

        // error_triage uses inline subqueries, one of which references svc-auth-*
        addView(
            "error_triage",
            "FROM (FROM svc-gateway-* | WHERE http_status >= 500 | KEEP @timestamp, http_status),"
                + "(FROM svc-payments-* | WHERE http_status >= 500 | KEEP @timestamp, http_status),"
                + "(FROM svc-auth-* | WHERE http_status >= 500 | KEEP @timestamp, http_status)"
        );

        // suspicious_ips also references svc-auth-* but is a completely separate view
        addView("suspicious_ips", "FROM svc-auth-* | STATS attempts = COUNT(*) BY source_ip");

        // incident_dashboard composes both views — no circular reference exists
        addView("incident_dashboard", "FROM error_triage, suspicious_ips");

        // This should NOT throw a circular view reference error
        LogicalPlan result = replaceViews(query("FROM incident_dashboard"));
        assertNotNull("incident_dashboard should resolve without circular reference errors", result);
        assertNoPlanConsistencyFailures(result, "incident_dashboard");
    }

    /**
     * Tests that composing two views in a parent view does not produce a false circular reference
     * when a wildcard in one view's subquery happens to match the sibling view's name, provided
     * the sibling view uses self-exclusion to avoid genuine self-reference.
     * <p>
     * Before the fix, the {@code seenViews} set was polluted by sibling view names from the outer
     * scope, causing the wildcard resolution inside {@code error_view} to see {@code svc-auth-failures}
     * as already visited even though it was only a sibling, not an ancestor.
     */
    public void testFalseCircularReferenceWhenWildcardMatchesSiblingViewName() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addIndex("svc-gateway-logs");
        addIndex("svc-auth-logs");

        // error_view uses inline subqueries, one references svc-auth-*
        addView(
            "error_view",
            "FROM (FROM svc-gateway-* | WHERE http_status >= 500 | KEEP @timestamp, http_status),"
                + "(FROM svc-auth-* | WHERE http_status >= 500 | KEEP @timestamp, http_status)"
        );

        // svc-auth-failures is a view whose NAME matches the svc-auth-* pattern.
        // It self-excludes to avoid genuine self-reference via wildcard.
        addView("svc-auth-failures", "FROM svc-auth-*,-svc-auth-failures | STATS attempts = COUNT(*) BY source_ip");

        // dashboard composes both views — no circular reference exists
        addView("dashboard", "FROM error_view, svc-auth-failures");

        // This should NOT throw a circular view reference error
        LogicalPlan result = replaceViews(query("FROM dashboard"));
        assertNotNull("dashboard should resolve without circular reference errors", result);

        // The wildcard svc-auth-* inside error_view's subquery matches the view svc-auth-failures,
        // creating a nested ViewUnionAll inside the pipeline chain. This produces a "nested subqueries"
        // error — which is the correct behavior (not a false circular reference).
        Failures failures = new Failures();
        Failures depFailures = new Failures();
        LogicalVerifier.INSTANCE.checkPlanConsistency(result, failures, depFailures);
        assertTrue("Expected nested subquery failure", failures.hasFailures());
        for (Failure failure : failures.failures()) {
            assertThat(failure.failMessage(), containsString("Nested subqueries are not supported"));
        }
    }

    /**
     * Tests that a view whose name matches its own wildcard pattern IS correctly detected as
     * a circular self-reference when it does NOT use self-exclusion.
     */
    public void testGenuineSelfReferenceViaWildcardInComposedView() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addIndex("svc-auth-logs");

        // svc-auth-failures queries FROM svc-auth-* which matches itself — genuine self-reference
        addView("svc-auth-failures", "FROM svc-auth-* | STATS attempts = COUNT(*) BY source_ip");

        Exception e = expectThrows(VerificationException.class, () -> replaceViews(query("FROM svc-auth-failures")));
        assertThat(e.getMessage(), containsString("circular view reference 'svc-auth-failures'"));
    }

    /**
     * Reproduces <a href="https://github.com/elastic/elasticsearch/issues/146097">#146097</a>:
     * a subquery-based view composed with a simple view that shares a wildcard index pattern
     * incorrectly triggers a "circular view reference" error.
     * <p>
     * view_x uses subqueries touching two wildcard patterns. view_y is a simple view on one of
     * the same patterns. Neither references the other, so composing them in view_xy should not
     * produce a circular reference error.
     */
    public void testFalseCircularReferenceFromSharedWildcardPattern_Issue146097() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addIndex("app-events-001");
        addIndex("auth-events-001");

        // view_x: subqueries touching both patterns
        addView("view_x", "FROM (FROM app-events-* | KEEP msg, level), (FROM auth-events-* | KEEP msg, level)");

        // view_y: simple view on one of the same patterns
        addView("view_y", "FROM auth-events-* | KEEP msg, level");

        // Compose both views
        addView("view_xy", "FROM view_x, view_y");

        // Before the fix this threw: "circular view reference 'view_y': view_xy -> view_x -> view_y"
        LogicalPlan result = replaceViews(query("FROM view_xy"));
        assertNotNull("view_xy should resolve without circular reference errors", result);
        assertNoPlanConsistencyFailures(result, "view_xy");
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
                // Run the same compaction (and ViewShadowRelation strip) the production pipeline does.
                LogicalPlan rewritten = COMPACTION.apply(future.actionGet().plan());
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
        // so the nested structure is eliminated and each UnresolvedRelation becomes a separate branch.
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
     * contribute a bare {@link UnresolvedRelation} with different whole patterns (e.g. "emp1,emp2"
     * vs "emp1,emp3") that share an individual index ("emp1"). After flattening,
     * {@code mergeUnresolvedRelationEntries} merges them into "emp1,emp2,emp1,emp3" because the
     * whole patterns differ — but IndexResolution would then deduplicate "emp1", losing data.
     */
    public void testFlattenedViewUnionAllWithOverlappingIndices() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        // Non-compactable views (have WHERE) that force NamedSubquery branches
        addView("view_x", "FROM emp1 | WHERE emp.age > 30");
        addView("view_y", "FROM emp1 | WHERE emp.salary > 50000");
        // Each of these resolves to a VUA with a bare UnresolvedRelation("emp1,emp2") or
        // UnresolvedRelation("emp1,emp3") plus a NamedSubquery for the non-compactable view
        addView("view_a", "FROM emp2, emp1, view_x");
        addView("view_b", "FROM emp3, emp1, view_y");
        addIndex("emp1");
        addIndex("emp2");
        addIndex("emp3");

        // Query FROM view_a, view_b produces a VUA with two inner VUAs.
        // tryFlattenViewUnionAll lifts the entries. The bare UnresolvedRelations "emp1,emp2" and
        // "emp1,emp3" share "emp1" — merging them would lose data.
        LogicalPlan result = replaceViews(query("FROM view_a, view_b"));
        assertThat(result, instanceOf(ViewUnionAll.class));
        ViewUnionAll vua = (ViewUnionAll) result;

        // Count how many branches resolve to a bare UnresolvedRelation containing "emp1"
        // If the bug is present, the UnresolvedRelations get merged into one, losing the second copy of emp1
        long urBranchesWithEmp1 = vua.namedSubqueries()
            .values()
            .stream()
            .filter(p -> p instanceof UnresolvedRelation)
            .map(p -> ((UnresolvedRelation) p).indexPattern().indexPattern())
            .filter(pattern -> Arrays.asList(pattern.split(",")).contains("emp1"))
            .count();

        assertThat(
            "emp1 should appear in at least 2 separate UnresolvedRelation branches to preserve view semantics, "
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
     * FORK limit when the total (diagonal count + query-level diagonal + 1 for compactable
     * UnresolvedRelation) exceeds 8.
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
     * into a single {@link UnresolvedRelation} entry. The total is: diagonal count from tree +
     * query-level diagonal + 1 ({@link UnresolvedRelation}).
     */
    private static int effectiveDiagonalBranches(int nesting, int branching) {
        int diagonal = Math.min(nesting, branching); // v_1_1, v_2_2, ..., v_min(N,B)_min(N,B)
        int queryDiagonal = (nesting + 1 <= branching) ? 1 : 0; // v_(N+1)_(N+1) if it exists
        return diagonal + queryDiagonal + 1; // +1 for the merged compactable UnresolvedRelation
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
                        // Branch-count enforcement now lives in Fork's post-analysis verification rather than
                        // its constructor, so view resolution succeeds with a wide ViewUnionAll and the failure
                        // surfaces only when the verifier walks the plan.
                        LogicalPlan result = replaceViews(query(queryStr), matrixResolver);
                        Failures forkFailures = new Failures();
                        result.forEachUp(p -> {
                            if (p instanceof Fork f) {
                                f.postAnalysisPlanVerification().accept(f, forkFailures);
                            }
                        });
                        assertTrue(
                            "Expected FORK branch failures for nesting=" + nesting + ", branching=" + branching + " in plan: " + result,
                            forkFailures.hasFailures()
                        );
                        assertThat(
                            "nesting=" + nesting + ", branching=" + branching,
                            forkFailures.failures().toString(),
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

    /**
     * Walk the plan tree and collect the {@code viewName} of every {@link ViewShadowRelation}.
     * Used by uncompacted-shape tests that want to assert which CPS shadows the resolver emitted
     * without depending on their precise structural position.
     */
    private static List<String> collectShadowNames(LogicalPlan plan) {
        List<String> names = new ArrayList<>();
        plan.forEachDown(ViewShadowRelation.class, sh -> names.add(sh.viewName()));
        return names;
    }

    /**
     * Walk the plan tree and collect each {@link ViewShadowRelation}'s applicable exclusions,
     * keyed by view name.
     */
    private static Map<String, List<String>> collectShadowExclusions(LogicalPlan plan) {
        Map<String, List<String>> exclusions = new java.util.LinkedHashMap<>();
        plan.forEachDown(ViewShadowRelation.class, sh -> exclusions.put(sh.viewName(), sh.exclusions()));
        return exclusions;
    }

    private static void assertNoPlanConsistencyFailures(LogicalPlan plan, String context) {
        Failures failures = new Failures();
        Failures depFailures = new Failures();
        LogicalVerifier.INSTANCE.checkPlanConsistency(plan, failures, depFailures);
        assertFalse("Plan consistency failures for " + context + ": " + failures, failures.hasFailures());
    }

    // -------------------------------------------------------------------------------------------
    // Uncompacted ViewResolver output contract tests.
    //
    // These assert on the raw output of ViewResolver.replaceViews(), before {@link ViewCompaction}
    // runs. They lock in the new ViewResolver contract so the upcoming CPS work
    // (esql-planning #543) can rely on a known, stable nested shape per resolution level. The bulk
    // of this file's coverage is on the post-compaction shape (what users observe via
    // {@link #replaceViews(LogicalPlan)}).
    // -------------------------------------------------------------------------------------------

    /**
     * Single-view query: the resolver returns a {@link ViewUnionAll} with the view's resolved body
     * alongside its CPS shadow. The strip in {@link ViewCompaction} drops the shadow before
     * downstream consumers see it (Phase A); Phase B will replace the strip with a real
     * lenient-resolution rule.
     */
    public void testUncompactedSingleView() {
        addView("v", "FROM emp");
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM v"));
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        ViewUnionAll vua = (ViewUnionAll) resolved;
        assertThat(vua.namedSubqueries().keySet(), containsInAnyOrder("v", "v#shadow"));
        assertThat(vua.namedSubqueries().get("v"), instanceOf(UnresolvedRelation.class));
        assertThat(vua.namedSubqueries().get("v#shadow"), instanceOf(ViewShadowRelation.class));
        assertThat(((ViewShadowRelation) vua.namedSubqueries().get("v#shadow")).viewName(), equalTo("v"));
        assertThat(((ViewShadowRelation) vua.namedSubqueries().get("v#shadow")).exclusions(), equalTo(List.of()));
    }

    /**
     * Two-view query: per-level merge in {@code buildPlanFromBranches} collapses the two strict
     * UnresolvedRelations into one merged entry; both views still get their own
     * {@link ViewShadowRelation} sibling.
     */
    public void testUncompactedTwoSiblingViews() {
        addView("v_a", "FROM emp1");
        addView("v_b", "FROM emp2");
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM v_a, v_b"));
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        ViewUnionAll vua = (ViewUnionAll) resolved;
        // Strict siblings collapsed into one bare UnresolvedRelation by the per-level merge.
        long strictCount = vua.namedSubqueries().values().stream().filter(p -> p instanceof UnresolvedRelation).count();
        assertThat("expected exactly one merged strict UR in: " + vua.namedSubqueries(), strictCount, equalTo(1L));
        // Both views have their own shadow.
        assertThat(vua.namedSubqueries().keySet(), hasItems("v_a#shadow", "v_b#shadow"));
    }

    /**
     * Nested views: every level emits its own {@link ViewShadowRelation} alongside the strict
     * resolution. The outer level wraps the inner level as a {@link NamedSubquery} since the
     * inner's plan is no longer a bare {@link UnresolvedRelation} once a shadow sibling is
     * attached.
     */
    public void testUncompactedNestedViews() {
        addView("inner", "FROM emp1, emp2");
        addView("outer", "FROM inner");
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM outer"));
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        ViewUnionAll outerVua = (ViewUnionAll) resolved;
        assertThat(outerVua.namedSubqueries().keySet(), hasItems("outer", "outer#shadow"));
        // Inner level should also surface its own shadow inside the outer's NamedSubquery wrapper.
        assertThat(
            "outer-level plan tree should contain shadows for both 'outer' and 'inner'",
            collectShadowNames(resolved),
            containsInAnyOrder("outer", "inner")
        );
    }

    /**
     * View body containing an exclusion is wrapped in a {@link NamedSubquery} by the resolver to
     * defeat sibling-merge — this is the scoping mechanism that prevents the exclusion from
     * leaking onto sibling {@link UnresolvedRelation}s. {@link ViewCompaction} unwraps the
     * NamedSubquery only at the very end (after all merging passes have decided to leave it alone).
     */
    public void testUncompactedViewBodyWithExclusionStaysWrapped() {
        addIndex("idx-a1");
        addIndex("idx-a2");
        addIndex("idx-b1");
        addIndex("idx-b2");
        addView("data-view", "FROM idx-b*,-*2");
        // The outer query has two sibling UnresolvedRelations. The view body's exclusion-bearing
        // UnresolvedRelation must stay scoped — represented as a NamedSubquery in the uncompacted output.
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM idx-a*, data-view"));
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        ViewUnionAll vua = (ViewUnionAll) resolved;
        // One entry is the bare outer UnresolvedRelation; another is the data-view body wrapped in
        // a NamedSubquery; another is the data-view shadow.
        assertThat(
            "Expected one entry to be a NamedSubquery wrapping the exclusion-bearing view body. Found: " + vua.namedSubqueries(),
            vua.namedSubqueries().values().stream().anyMatch(p -> p instanceof NamedSubquery),
            equalTo(true)
        );
        assertThat(collectShadowNames(resolved), contains("data-view"));
    }

    /**
     * Non-CPS wildcard-matched siblings of the same view body shape are merged at the resolver
     * level (per the per-level merge), then collapse to a single {@link UnresolvedRelation}. The
     * order in the merged pattern follows the order returned by the view metadata service, which
     * is not guaranteed to be alphabetical — we just check the final set of patterns matches.
     * <p>
     * Under CPS this collapse does not happen — the wildcard is preserved as a CPS lookup pattern
     * alongside the per-view strict resolutions and shadows. See
     * {@link #testUncompactedWildcardMatchedSiblingsMergeCps} for the CPS shape.
     */
    public void testUncompactedWildcardMatchedSiblingsMerge() {
        addView("v_a", "FROM emp1");
        addView("v_b", "FROM emp2");
        LogicalPlan resolved = replaceViewsWithoutCompactionNonCps(query("FROM v_*"));
        // Non-CPS: the wildcard fully resolves into the matched view bodies; per-level merge folds
        // them into one bare UnresolvedRelation. No shadows.
        assertThat(resolved, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation strict = (UnresolvedRelation) resolved;
        assertThat(List.of(strict.indexPattern().indexPattern().split(",")), containsInAnyOrder("emp1", "emp2"));
        assertThat(collectShadowNames(resolved), empty());
    }

    /**
     * CPS variant of {@link #testUncompactedWildcardMatchedSiblingsMerge}: the wildcard pattern
     * is preserved as a strict {@link UnresolvedRelation} for the CPS lookup against linked
     * projects, alongside per-view strict resolutions and a {@link ViewShadowRelation} sibling
     * per matched view.
     */
    public void testUncompactedWildcardMatchedSiblingsMergeCps() {
        addView("v_a", "FROM emp1");
        addView("v_b", "FROM emp2");
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM v_*"));
        // in cps resolved pattern should contain coalesced views patterns (emp1 and emp2)
        // as well as v_* pattern in case it could be resolved on linked projects
        assertThat(resolved, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation strict = (UnresolvedRelation) resolved;
        assertThat(List.of(strict.indexPattern().indexPattern().split(",")), containsInAnyOrder("emp1", "emp2", "v_*"));
        assertThat(collectShadowNames(resolved), empty());
    }

    /**
     * Subquery inside a view body produces a nested {@link ViewUnionAll}/Fork structure that the
     * resolver does <em>not</em> flatten — that's the analyzer's job. This verifies the nested
     * structure survives the resolver, which is the property #543's lenient-call work depends on.
     * The outer view body shows up as a {@link NamedSubquery} wrapping a {@link UnionAll}, since
     * the user-written subquery prevents collapsing to a bare {@link UnresolvedRelation}.
     */
    public void testUncompactedSubqueryInViewBodyKeepsNestedStructure() {
        assumeTrue("Requires views with branching support", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
        addView("inner_a", "FROM emp1");
        addView("inner_b", "FROM emp2 | WHERE emp.age > 30");
        addView("outer", "FROM inner_a, (FROM inner_b)");
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM outer"));
        // Now there's an outer ViewUnionAll containing the resolved 'outer' view body alongside its
        // shadow. The view body itself still carries the user-written subquery as a nested
        // UnionAll/ViewUnionAll.
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        ViewUnionAll vua = (ViewUnionAll) resolved;
        assertThat(vua.namedSubqueries().keySet(), hasItems("outer", "outer#shadow"));
        // Shadows should be present for outer plus the inner views referenced through the body.
        assertThat(collectShadowNames(resolved), hasItems("outer", "inner_a", "inner_b"));
    }

    /**
     * Per-shadow exclusion lists carry the position-aware exclusions from each view's referencing
     * UnresolvedRelation. Reproduces the example from esql-planning #543 where {@code v1}'s body
     * has {@code FROM v2,metrics*,-*2025} so the {@code v2} shadow gets {@code [-*2025]}.
     */
    public void testViewShadowRelationCarriesPositionAwareExclusions() {
        addIndex("logs-001");
        addIndex("metrics-001");
        addView("v2", "FROM logs*,-*2026");
        addView("v1", "FROM v2,metrics*,-*2025");
        addView("v0", "FROM v1");

        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM v0"));
        Map<String, List<String>> shadowExclusions = collectShadowExclusions(resolved);

        // v0 referenced from outer "FROM v0" — no later exclusions.
        assertThat(shadowExclusions.get("v0"), equalTo(List.of()));
        // v1 referenced from v0's body "FROM v1" — no later exclusions.
        assertThat(shadowExclusions.get("v1"), equalTo(List.of()));
        // v2 referenced from v1's body "FROM v2,metrics*,-*2025" — -*2025 follows v2 → applies.
        assertThat(shadowExclusions.get("v2"), equalTo(List.of("-*2025")));
    }

    /**
     * An exclusion that only follows some views applies only to those views. Reproduces the
     * {@code FROM v_a,-staleA-*,v_b,-staleB-*} case: v_a sees both exclusions; v_b sees only the
     * one that comes after it.
     */
    public void testViewShadowRelationExclusionsRespectPosition() {
        addView("v_a", "FROM emp");
        addView("v_b", "FROM emp");
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM v_a,-staleA-*,v_b,-staleB-*"));
        Map<String, List<String>> shadowExclusions = collectShadowExclusions(resolved);
        assertThat(shadowExclusions.get("v_a"), equalTo(List.of("-staleA-*", "-staleB-*")));
        assertThat(shadowExclusions.get("v_b"), equalTo(List.of("-staleB-*")));
    }

    /**
     * Cluster-prefixed exclusions ({@code cluster:-name}, {@code *:-name}) and cluster-level
     * exclusions ({@code -cluster:*}) must travel with the shadow's exclusions list, just like
     * the bare {@code -name} form. Reproduces a serverless integration scenario:
     * {@code FROM my-data, my_linked_project:-my-data} where {@code my-data} is a local view
     * and a remote index on {@code my_linked_project} — the cluster-scoped exclusion has to reach
     * the lenient field-caps target as {@code my-data,my_linked_project:-my-data}, otherwise the
     * lenient lookup matches the remote index and the shadow erroneously resolves to a remote
     * {@code EsRelation}.
     */
    public void testViewShadowRelationCarriesClusterPrefixedExclusions() {
        addView("my-data", "FROM source-index");
        addView("v_a", "FROM emp");
        addView("v_b", "FROM emp");

        // Cluster-prefixed exclusion (cluster:-name): attaches to shadows positioned before it.
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM my-data,my_linked_project:-my-data"));
        Map<String, List<String>> shadowExclusions = collectShadowExclusions(resolved);
        assertThat(shadowExclusions.get("my-data"), equalTo(List.of("my_linked_project:-my-data")));

        // *:-name form (exclusion across all remotes): same handling.
        resolved = replaceViewsWithoutCompaction(query("FROM v_a,*:-stale,v_b"));
        shadowExclusions = collectShadowExclusions(resolved);
        assertThat(shadowExclusions.get("v_a"), equalTo(List.of("*:-stale")));
        assertThat(shadowExclusions.get("v_b"), equalTo(List.of()));

        // Cluster-level exclusion (-cluster:*): also propagates.
        resolved = replaceViewsWithoutCompaction(query("FROM v_a,-stale_cluster:*,v_b"));
        shadowExclusions = collectShadowExclusions(resolved);
        assertThat(shadowExclusions.get("v_a"), equalTo(List.of("-stale_cluster:*")));
        assertThat(shadowExclusions.get("v_b"), equalTo(List.of()));
    }

    // -------------------------------------------------------------------------------------------
    // Staged transformation tests.
    //
    // The pipeline runs three observable phases:
    // 1. View resolution (ViewResolver.replaceViews) — produces a tree with ViewShadowRelation
    // siblings inside per-level ViewUnionAlls.
    // 2. ViewCompaction.preIndexResolution — runs from EsqlSession before PreAnalyzer. Just reshapes
    // user-written Subquery/UnionAll into ViewUnionAll where appropriate. Shadows survive.
    // 3. ViewCompaction.postIndexResolution — runs as an analyzer rule after ResolveTable. Strips
    // shadows, re-runs the rewrite (strip can collapse a single-shadow ViewUnionAll to a
    // sole NamedSubquery, exposing structure rewrite needs), flattens nested ViewUnionAlls,
    // unwraps remaining NamedSubquery wrappers.
    //
    // These tests assert on the tree after each phase to lock in the contract between phases —
    // critical for the CPS index resolution which needs the strict/lenient pairing intact at the
    // analyzer-rule boundary.
    // -------------------------------------------------------------------------------------------

    /**
     * CPS variant: view referenced from inside a user-written {@code (FROM v)} subquery. The
     * interesting case is that {@link ViewCompaction#postIndexResolution} has to re-run
     * {@code rewriteUnionAllsWithNamedSubqueries} <em>after</em> the strip — the strip collapses
     * {@code ViewUnionAll[NamedSubquery, ViewShadowRelation]} to its sole {@link NamedSubquery}
     * child, which is what the rewrite needs to see in order to unwrap the surrounding
     * {@link Subquery} and convert the outer {@link UnionAll} to {@link ViewUnionAll}. Compare
     * with {@link #testStagedSimpleViewInSubqueryNonCps} where the rewrite chain runs in
     * preIndexResolution because no shadow-bearing ViewUnionAll gets in the way.
     */
    public void testStagedSimpleViewInSubqueryCps() {
        addView("my_view", "FROM emp | WHERE emp.age > 30");

        // Stage 1: view resolution only. The Subquery wraps a ViewUnionAll because the inner
        // resolution path emits the (NamedSubquery, ViewShadowRelation) pair as a per-level VUA.
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM emp2, (FROM my_view)"));
        assertThat(resolved, instanceOf(UnionAll.class));
        assertThat(resolved, not(instanceOf(ViewUnionAll.class)));
        assertThat(collectShadowNames(resolved), contains("my_view"));
        Subquery innerSubquery = resolved.children()
            .stream()
            .filter(c -> c instanceof Subquery)
            .map(c -> (Subquery) c)
            .findFirst()
            .orElseThrow();
        assertThat(innerSubquery.child(), instanceOf(ViewUnionAll.class));

        // Stage 2: preIndexResolution. The Subquery's child is a ViewUnionAll (not a NamedSubquery)
        // so rewrite has nothing to unwrap; the outer UnionAll has [UR, Subquery] children with no
        // direct NamedSubquery sibling, so it stays as a plain UnionAll. Shadows untouched.
        LogicalPlan preIndicesResolved = ViewCompaction.preIndexResolution(resolved);
        assertThat(preIndicesResolved, instanceOf(UnionAll.class));
        assertThat(preIndicesResolved, not(instanceOf(ViewUnionAll.class)));
        assertThat(collectShadowNames(preIndicesResolved), contains("my_view"));

        // Stage 3: postIndexResolution. Strip removes the shadow, collapsing the inner ViewUnionAll
        // to its NamedSubquery child; the re-run rewrite then unwraps Subquery(NamedSubquery) and
        // converts the outer UnionAll to ViewUnionAll; the unwrap step at the end strips the
        // remaining NamedSubquery wrapper, leaving the bare view body alongside the sibling.
        LogicalPlan postIndicesResolved = ViewCompaction.postIndexResolution(preIndicesResolved);
        assertThat(postIndicesResolved, instanceOf(ViewUnionAll.class));
        assertThat(collectShadowNames(postIndicesResolved), empty());
        assertThat(
            postIndicesResolved.children(),
            containsInAnyOrder(matchesPlan(query("FROM emp | WHERE emp.age > 30")), matchesPlan(query("FROM emp2")))
        );
    }

    /**
     * Non-CPS variant: same query as {@link #testStagedSimpleViewInSubqueryCps}, but with shadows
     * disabled. Without a shadow sibling the inner resolution returns the resolved view body
     * wrapped in a single {@link NamedSubquery} (no enclosing ViewUnionAll), so preIndexResolution's
     * rewrite chain immediately unwraps {@code Subquery(NamedSubquery)} and converts the outer
     * UnionAll to ViewUnionAll. postIndexResolution then has nothing left to do.
     */
    public void testStagedSimpleViewInSubqueryNonCps() {
        addView("my_view", "FROM emp | WHERE emp.age > 30");

        // Stage 1: view resolution only. No CPS, no shadow. The user-written Subquery wrapper is
        // unwrapped during resolution (replaceViewsFork's Subquery(NamedSubquery) → NamedSubquery
        // step) — without a shadow sibling forcing a per-level ViewUnionAll, the inner branch
        // simplifies straight to the resolved view body wrapped in a NamedSubquery. The outer
        // UnionAll's children are now [UR, NamedSubquery].
        LogicalPlan resolved = replaceViewsWithoutCompactionNonCps(query("FROM emp2, (FROM my_view)"));
        assertThat(resolved, instanceOf(UnionAll.class));
        assertThat(resolved, not(instanceOf(ViewUnionAll.class)));
        assertThat(collectShadowNames(resolved), empty());
        // The Subquery wrapper is gone — the inner branch is a NamedSubquery sibling of the outer UR.
        // (NamedSubquery extends Subquery, so we filter narrowly: bare-Subquery only.)
        boolean hasBareSubquery = resolved.children()
            .stream()
            .anyMatch(c -> c instanceof Subquery && (c instanceof NamedSubquery) == false);
        assertFalse("Did not expect a bare Subquery wrapper in the outer UnionAll's children", hasBareSubquery);
        long namedSubqueryCount = resolved.children().stream().filter(c -> c instanceof NamedSubquery).count();
        assertThat("Expected one NamedSubquery sibling carrying the resolved view body", namedSubqueryCount, equalTo(1L));

        // Stage 2: preIndexResolution. The outer UnionAll has a NamedSubquery child, so the rewrite
        // converts it to ViewUnionAll. This is more compaction than the CPS variant achieves at
        // this stage: there, the inner level is a shadow-bearing ViewUnionAll wrapped in a Subquery,
        // which the rewrite cannot collapse until postIndexResolution strips the shadow.
        LogicalPlan preIndicesResolved = ViewCompaction.preIndexResolution(resolved);
        assertThat(preIndicesResolved, instanceOf(ViewUnionAll.class));
        assertThat(collectShadowNames(preIndicesResolved), empty());

        // Stage 3: postIndexResolution. Strip is a no-op (no shadows). The final NamedSubquery
        // unwrap leaves a clean ViewUnionAll over the resolved bodies.
        LogicalPlan postIndicesResolved = ViewCompaction.postIndexResolution(preIndicesResolved);
        assertThat(postIndicesResolved, instanceOf(ViewUnionAll.class));
        assertThat(collectShadowNames(postIndicesResolved), empty());
        assertThat(
            postIndicesResolved.children(),
            containsInAnyOrder(matchesPlan(query("FROM emp | WHERE emp.age > 30")), matchesPlan(query("FROM emp2")))
        );
    }

    /**
     * CPS variant: two sibling views {@code FROM v_a, v_b}. The per-level merge inside
     * {@code ViewResolver.buildPlanFromBranches} collapses the two strict UnresolvedRelations
     * into one merged entry, but each view still gets its own {@link ViewShadowRelation} sibling.
     * Tracking those shadows from view resolution through
     * {@link ViewCompaction#postIndexResolution} is what proves the strict/lenient pairing
     * survives intact at the analyzer-rule boundary — the CPS lenient-FC rule will read those
     * shadows there.
     */
    public void testStagedTwoSiblingViewsCps() {
        addView("v_a", "FROM emp1");
        addView("v_b", "FROM emp2");

        // Stage 1: view resolution only. Per-level merge folds the two strict URs into one,
        // shadows attached as siblings.
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM v_a, v_b"));
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        ViewUnionAll resolvedVua = (ViewUnionAll) resolved;
        long strictCount = resolvedVua.namedSubqueries().values().stream().filter(p -> p instanceof UnresolvedRelation).count();
        assertThat("expected one merged strict UR", strictCount, equalTo(1L));
        assertThat(collectShadowNames(resolved), containsInAnyOrder("v_a", "v_b"));

        // Stage 2: preIndexResolution is the rewrite step. Already a ViewUnionAll, so no-op.
        LogicalPlan preIndicesResolved = ViewCompaction.preIndexResolution(resolved);
        assertThat(preIndicesResolved, instanceOf(ViewUnionAll.class));
        assertThat(collectShadowNames(preIndicesResolved), containsInAnyOrder("v_a", "v_b"));

        // Stage 3: postIndexResolution strips both shadows; the merged strict UR is the sole survivor.
        LogicalPlan postIndicesResolved = ViewCompaction.postIndexResolution(preIndicesResolved);
        assertThat(collectShadowNames(postIndicesResolved), empty());
        assertThat(postIndicesResolved, matchesPlan(query("FROM emp1,emp2")));
    }

    /**
     * Non-CPS variant: same query as {@link #testStagedTwoSiblingViewsCps}. Without shadows the
     * resolver doesn't need a {@link ViewUnionAll} at this level at all — the per-level merge
     * collapses the two view bodies to a single bare {@link UnresolvedRelation}, which is
     * returned directly. Both compaction phases then have nothing to do.
     */
    public void testStagedTwoSiblingViewsNonCps() {
        addView("v_a", "FROM emp1");
        addView("v_b", "FROM emp2");

        // Stage 1: view resolution only. With no shadows to keep alongside, the resolver collapses
        // the merged UR all the way down to a bare UnresolvedRelation rather than wrapping it in
        // a ViewUnionAll.
        LogicalPlan resolved = replaceViewsWithoutCompactionNonCps(query("FROM v_a, v_b"));
        assertThat(resolved, instanceOf(UnresolvedRelation.class));
        assertThat(collectShadowNames(resolved), empty());
        assertThat(((UnresolvedRelation) resolved).indexPattern().indexPattern(), equalTo("emp1,emp2"));

        // Stage 2 + 3: both compaction phases see only a bare UR — nothing to compact.
        LogicalPlan preIndicesResolved = ViewCompaction.preIndexResolution(resolved);
        assertThat(preIndicesResolved, sameInstance(resolved));
        LogicalPlan postIndicesResolved = ViewCompaction.postIndexResolution(preIndicesResolved);
        assertThat(postIndicesResolved, sameInstance(preIndicesResolved));
        assertThat(postIndicesResolved, matchesPlan(query("FROM emp1,emp2")));
    }

    /**
     * CPS variant: nested non-compactable views. The inner view's body has a {@code | LIMIT} so
     * it cannot be folded into a sibling UnresolvedRelation — the resolver keeps the inner level
     * as a {@link NamedSubquery}-wrapped {@link ViewUnionAll}. Each level emits its own shadow,
     * so the tree carries shadows for both inner and outer at the time it reaches PreAnalyzer.
     * postIndexResolution strips both and then flattens the nested {@link ViewUnionAll} structure
     * into a single level (Strategy A — sibling resolutions stay separate, no merging).
     */
    public void testStagedNestedNonCompactableViewsCps() {
        addView("inner_v", "FROM emp1 | LIMIT 100");
        addView("outer_v", "FROM inner_v, emp2 | LIMIT 200");

        // Stage 1: view resolution. Outer ViewUnionAll wraps a NamedSubquery whose body contains
        // an inner ViewUnionAll (carrying inner_v's shadow). The outer level has its own shadow.
        LogicalPlan resolved = replaceViewsWithoutCompaction(query("FROM outer_v"));
        assertThat(resolved, instanceOf(ViewUnionAll.class));
        assertThat(collectShadowNames(resolved), containsInAnyOrder("inner_v", "outer_v"));

        // Stage 2: preIndexResolution is a no-op (already a ViewUnionAll, no plain Subquery
        // wrapping a NamedSubquery to unwrap). Shadows still present at both levels.
        LogicalPlan preIndicesResolved = ViewCompaction.preIndexResolution(resolved);
        assertThat(preIndicesResolved, instanceOf(ViewUnionAll.class));
        assertThat(collectShadowNames(preIndicesResolved), containsInAnyOrder("inner_v", "outer_v"));

        // Stage 3: postIndexResolution strips both shadows and flattens the nested structure. The
        // non-compactable Limits prevent UR-merging, so we end up with a single ViewUnionAll
        // whose children are the resolved view bodies.
        LogicalPlan postIndicesResolved = ViewCompaction.postIndexResolution(preIndicesResolved);
        assertThat(collectShadowNames(postIndicesResolved), empty());
        assertFalse(
            "Expected no nested ViewUnionAll after postIndexResolution, got: " + postIndicesResolved,
            containsNestedViewUnionAll(postIndicesResolved)
        );
    }

    /**
     * Non-CPS variant of {@link #testStagedNestedNonCompactableViewsCps}: with no shadows to keep
     * alongside, the resolver still produces a nested {@link ViewUnionAll} (the {@code | LIMIT}
     * prevents UR-merging across levels), but the structure is leaner — for {@code FROM outer_v}
     * with a single view ref, the result is the resolved outer_v body wrapped in a NamedSubquery,
     * containing a single inner ViewUnionAll. postIndexResolution flattens the nested
     * ViewUnionAll just like in the CPS case, leaving a single-level ViewUnionAll.
     */
    public void testStagedNestedNonCompactableViewsNonCps() {
        addView("inner_v", "FROM emp1 | LIMIT 100");
        addView("outer_v", "FROM inner_v, emp2 | LIMIT 200");

        // Stage 1: view resolution. With a single outer view ref and no shadow, the resolver
        // returns the resolved view body directly — wrapped in a NamedSubquery (because the body
        // has a Limit, so it's not a bare UnresolvedRelation).
        LogicalPlan resolved = replaceViewsWithoutCompactionNonCps(query("FROM outer_v"));
        assertThat(resolved, instanceOf(NamedSubquery.class));
        assertThat(collectShadowNames(resolved), empty());
        assertTrue(
            "Expected a nested ViewUnionAll inside the resolved outer body before compaction",
            containsNestedViewUnionAll(resolved) || hasViewUnionAllUnderUnary(resolved)
        );

        // Stage 2: preIndexResolution is a no-op here — there's no plain UnionAll-with-NamedSubquery
        // to convert; the structure is already shadow-free, just nested.
        LogicalPlan preIndicesResolved = ViewCompaction.preIndexResolution(resolved);
        assertThat(collectShadowNames(preIndicesResolved), empty());

        // Stage 3: postIndexResolution unwraps the outer NamedSubquery and flattens the nested
        // ViewUnionAll. Strip is a no-op (no shadows). Final plan has no nested ViewUnionAlls.
        LogicalPlan postIndicesResolved = ViewCompaction.postIndexResolution(preIndicesResolved);
        assertThat(collectShadowNames(postIndicesResolved), empty());
        assertFalse(
            "Expected no nested ViewUnionAll after postIndexResolution, got: " + postIndicesResolved,
            containsNestedViewUnionAll(postIndicesResolved)
        );
    }

    private static boolean hasViewUnionAllUnderUnary(LogicalPlan plan) {
        boolean[] found = { false };
        plan.forEachDown(ViewUnionAll.class, vua -> found[0] = true);
        return found[0];
    }

    /**
     * Replace views and apply the compaction step that the analyzer runs in production. Most of
     * this file's assertions are on the compacted shape, since that's what users observe; tests
     * that need to assert on the raw uncompacted resolver output should use
     * {@link #replaceViewsWithoutCompaction(LogicalPlan)}.
     */
    private LogicalPlan replaceViews(LogicalPlan plan) {
        return replaceViews(plan, viewResolver);
    }

    private LogicalPlan replaceViews(LogicalPlan plan, ViewResolver resolver) {
        return COMPACTION.apply(replaceViewsWithoutCompaction(plan, resolver));
    }

    /**
     * Run view resolution under CPS so {@link ViewShadowRelation} siblings are emitted alongside
     * the strict resolution. Default for the uncompacted-shape tests because most of them assert
     * on the strict/lenient pairing — that contract only exists in CPS mode.
     */
    private LogicalPlan replaceViewsWithoutCompaction(LogicalPlan plan) {
        return replaceViewsWithoutCompaction(plan, cpsViewResolver());
    }

    /**
     * Run view resolution without CPS — no {@link ViewShadowRelation}s in the output. Use this
     * when the test wants to demonstrate the non-CPS contract (no shadow emission, more
     * aggressive pre-index-resolution compaction since shadow-bearing {@link ViewUnionAll}s
     * don't get in the way of the outer rewrite chain).
     */
    private LogicalPlan replaceViewsWithoutCompactionNonCps(LogicalPlan plan) {
        return replaceViewsWithoutCompaction(plan, viewResolver);
    }

    private LogicalPlan replaceViewsWithoutCompaction(LogicalPlan plan, ViewResolver resolver) {
        PlainActionFuture<ViewResolver.ViewResolutionResult> future = new PlainActionFuture<>();
        resolver.replaceViews(plan, this::parse, future);
        return future.actionGet().plan();
    }

    private InMemoryViewResolver cpsViewResolver() {
        var cpsDecider = new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", true).build());
        return viewService.getViewResolver(cpsDecider);
    }

    private LogicalPlan replaceViewsWithCPS(LogicalPlan plan) {
        return COMPACTION.apply(replaceViewsWithoutCompaction(plan, cpsViewResolver()));
    }

    private static final ViewCompaction COMPACTION = new ViewCompaction();

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

    public void testDeleteMultipleViews() {
        addView("view1", "FROM emp");
        addView("view2", "FROM emp");
        addView("view3", "FROM emp");
        assertThat(viewService.list(projectId).size(), equalTo(3));

        deleteViews("view1", "view3");
        assertThat(viewService.list(projectId).size(), equalTo(1));
        assertNull(viewService.get(projectId, "view1"));
        assertNotNull(viewService.get(projectId, "view2"));
        assertNull(viewService.get(projectId, "view3"));
    }

    public void testDeleteEmptyListIsNoOp() {
        addView("view1", "FROM emp");
        deleteViews();
        assertNotNull(viewService.get(projectId, "view1"));
    }

    private void deleteViews(String... views) {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        viewService.deleteViews(projectId, TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, List.of(views), future);
        assertTrue(future.actionGet().isAcknowledged());
    }

    protected LogicalPlan query(String e) {
        return query(e, new QueryParams());
    }

    LogicalPlan query(String e, QueryParams params) {
        return TEST_PARSER.parseQuery(e, params);
    }
}
