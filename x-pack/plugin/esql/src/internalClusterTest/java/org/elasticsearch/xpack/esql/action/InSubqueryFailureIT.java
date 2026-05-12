/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.session.ViewAndInSubqueryResolver;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.junit.Before;

import static org.elasticsearch.test.ESIntegTestCase.TEST_REQUEST_TIMEOUT;
import static org.elasticsearch.test.ESIntegTestCase.indexExists;
import static org.elasticsearch.test.ESIntegTestCase.updateClusterSettings;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.session.ViewAndInSubqueryResolver.MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Negative integration tests for WHERE ... IN (subquery).
 * Verifies that unsupported usages of IN subquery are rejected with clear error messages,
 * and that type mismatches between left and right fields are caught.
 * <p>
 * One test lowers {@link ViewAndInSubqueryResolver#MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING} on the
 * cluster and runs a nested view / {@code IN} query via {@link #run(String)} so planning fails the same way as other
 * failures in this suite.
 */
public class InSubqueryFailureIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
    }

    @Before
    public void setupIndices() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 3)))
                .setMapping("id", "type=integer", "name", "type=keyword", "score", "type=double")
        );
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("id", 1, "name", "alice", "score", 1.5))
            .add(new IndexRequest("test").id("2").source("id", 2, "name", "bob", "score", 2.5))
            .add(new IndexRequest("test").id("3").source("id", 3, "name", "carol", "score", 3.5))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");
    }

    // ---- IN subquery in unsupported positions ----

    public void testRejectsInSubqueryInEval() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | EVAL x = id IN (FROM test | KEEP id)"));
        assertThat(e.getMessage(), containsString("IN subquery is not supported in [EVAL x = id IN (FROM test | KEEP id)]"));
    }

    public void testRejectsNotInSubqueryInEval() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | EVAL x = id NOT IN (FROM test | KEEP id)"));
        assertThat(e.getMessage(), containsString("IN subquery is not supported in [EVAL x = id NOT IN (FROM test | KEEP id)]"));
    }

    public void testRejectsInSubqueryInSort() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | SORT id IN (FROM test | KEEP id)"));
        assertThat(e.getMessage(), containsString("IN subquery is not supported in [SORT id IN (FROM test | KEEP id)]"));
    }

    public void testRejectsInSubqueryInStatsBy() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | STATS c = COUNT(*) BY id IN (FROM test | KEEP id)"));
        assertThat(e.getMessage(), containsString("IN subquery is not supported in [STATS c = COUNT(*) BY id IN (FROM test | KEEP id)]"));
    }

    public void testRejectsInSubqueryInStatsWhereFilter() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | STATS c = COUNT(*) WHERE id IN (FROM test | KEEP id)"));
        assertThat(
            e.getMessage(),
            containsString("IN subquery is not supported in [STATS c = COUNT(*) WHERE id IN (FROM test | KEEP id)]")
        );
    }

    public void testRejectsInSubqueryInWhereInsideCaseExpression() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE CASE(id IN (FROM test | KEEP id), true, false)"));
        assertThat(
            e.getMessage(),
            containsString("IN subquery is not supported within other expressions [CASE(id IN (FROM test | KEEP id), true, false)]")
        );
    }

    public void testRejectInSubqueryUsedInWhereInsideMvContains() {
        var e = expectThrows(
            VerificationException.class,
            () -> run("FROM main_index | WHERE emp_no > 0 and MV_CONTAINS(x IN (FROM main_index), [true, false])")
        );
        assertThat(
            e.getMessage(),
            containsString("IN subquery is not supported within other expressions [MV_CONTAINS(x IN (FROM main_index), [true, false])]")
        );
    }

    // ---- subquery returning multiple columns ----

    public void testRejectsSubqueryWithMultipleColumns() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (FROM test | KEEP id, name) | KEEP id"));
        assertThat(e.getMessage(), containsString("IN subquery must return exactly one column, found [id, name]"));
    }

    // ---- type mismatches ----

    public void testRejectsTypeMismatchIntegerVsKeyword() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (FROM test | KEEP name) | KEEP id"));
        assertThat(
            e.getMessage(),
            containsString("left field [id] of type [INTEGER] is incompatible with right field [name] of type [KEYWORD]")
        );
    }

    public void testRejectsTypeMismatchKeywordVsInteger() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE name IN (FROM test | KEEP id) | KEEP name"));
        assertThat(
            e.getMessage(),
            containsString("left field [name] of type [KEYWORD] is incompatible with right field [id] of type [INTEGER]")
        );
    }

    /**
     * {@link ViewAndInSubqueryResolver} alternates {@link ViewResolver} with {@link InSubqueryResolver}.
     * Views {@code in_layer_1} … {@code in_layer_3} nest {@code IN} subqueries so each references another view; the
     * outer query adds another {@code IN} over {@code in_layer_3}. That forces many view / {@code IN} alternations
     * before a fixed point. With {@link ViewAndInSubqueryResolver#MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING}
     * set well below the default ({@code 10}), resolution exceeds the cap and {@link #run(String)} fails with
     * {@link VerificationException} like the other tests in this class. The exception message reports the
     * zero-based iteration that exceeded the cap and the effective cluster setting (see
     * {@link org.elasticsearch.xpack.esql.session.ViewAndInSubqueryResolver}).
     */
    public void testMaxViewInSubqueryResolutionIterationsClusterSetting() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());

        final int maxIterations = 1;
        // With maxIterations==1, iterations 0 and 1 run; the next outer pass uses iteration==2 and fails before work.
        final int failingIteration = maxIterations + 1;
        updateClusterSettings(Settings.builder().put(MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING.getKey(), maxIterations));
        try {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate("employees")
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
                    .setMapping("id", "type=integer", "score", "type=double")
            );
            client().prepareBulk()
                .add(new IndexRequest("employees").id("1").source("id", 1, "score", 60000.0))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            ensureYellow("employees");

            installView("in_layer_1", "FROM employees | WHERE id IN (FROM test | KEEP id) | KEEP id");
            installView("in_layer_2", "FROM employees | WHERE id IN (FROM in_layer_1 | KEEP id) | KEEP id");
            installView("in_layer_3", "FROM employees | WHERE id IN (FROM in_layer_2 | KEEP id) | KEEP id, score");

            String query = "FROM test | WHERE id IN (FROM in_layer_3 | WHERE score > 50000 | KEEP id)";
            VerificationException e = expectThrows(VerificationException.class, () -> run(query));
            String expectedMessage = "Too many view/IN subquery resolution iterations: "
                + failingIteration
                + " (exceeds "
                + MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING.getKey()
                + "="
                + maxIterations
                + ")";
            assertThat(e.getMessage(), equalTo(expectedMessage));
        } finally {
            for (String viewName : new String[] { "in_layer_1", "in_layer_2", "in_layer_3" }) {
                try {
                    assertAcked(
                        client().execute(
                            DeleteViewAction.INSTANCE,
                            new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { viewName })
                        )
                    );
                } catch (RuntimeException e) {
                    // View may be absent if the test body failed before creating it.
                }
            }
            if (indexExists("employees")) {
                assertAcked(indicesAdmin().prepareDelete("employees").get());
            }
            updateClusterSettings(Settings.builder().putNull(MAX_VIEW_IN_SUBQUERY_RESOLUTION_ITERATIONS_SETTING.getKey()));
        }
    }

    private void installView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            )
        );
    }
}
