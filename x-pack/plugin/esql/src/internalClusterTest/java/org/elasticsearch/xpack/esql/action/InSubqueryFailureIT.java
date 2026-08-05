/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.view.ViewResolver;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.InSubqueryIT.deleteViews;
import static org.elasticsearch.xpack.esql.action.InSubqueryIT.installView;
import static org.hamcrest.Matchers.containsString;

/**
 * Negative integration tests for WHERE ... IN (subquery).
 * Verifies that unsupported usages of IN subquery are rejected with clear error messages,
 * and that type mismatches between left and right fields are caught.
 */
public class InSubqueryFailureIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
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

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("empty_mapping")
                .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 3)))
        );
        ensureYellow("empty_mapping");
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

    // ---- subquery returning multiple or zero columns ----

    public void testRejectsSubqueryWithMultipleColumns() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (FROM test | KEEP id, name) | KEEP id"));
        assertThat(e.getMessage(), containsString("IN subquery must return exactly one column, found [id, name]"));
    }

    public void testRejectsSubqueryWithMultipleColumnsWithRow() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (ROW x = 1, id = 2) | KEEP id"));
        assertThat(e.getMessage(), containsString("IN subquery must return exactly one column, found [x, id]"));
    }

    public void testRejectsSubqueryWithZeroColumnInOutput() {
        var e = expectThrows(
            VerificationException.class,
            () -> run("FROM test | WHERE id IN (FROM test | DROP id, name, score) | KEEP id")
        );
        assertThat(e.getMessage(), containsString("IN subquery must return exactly one column, found []"));
    }

    public void testRejectsSubqueryWithZeroColumnInOutputWithRow() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (ROW x = 1 | DROP x) | KEEP id"));
        assertThat(e.getMessage(), containsString("IN subquery must return exactly one column, found []"));
    }

    public void testRejectsViewSubqueryWithMultipleColumns() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires IN subquery view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        installView("multi_col_view", "FROM test | KEEP id, name");
        try {
            var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (FROM multi_col_view) | KEEP id"));
            assertThat(e.getMessage(), containsString("IN subquery must return exactly one column, found [id, name]"));
        } finally {
            deleteViews("multi_col_view");
        }
    }

    public void testRejectsSubqueryWithEmptyMapping() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE id IN (FROM empty_mapping) | KEEP id"));
        assertThat(e.getMessage(), containsString("IN subquery cannot reference an index with empty mapping"));
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

    // ---- view depth cap ----
    public void testMaxViewDepthClusterSettingWithInSubquery() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires IN subquery view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());

        // Three nested views (in_layer_3 -> in_layer_2 -> in_layer_1) exceed maxDepth=2.
        final int maxDepth = 2;
        final String settingKey = ViewResolver.MAX_VIEW_DEPTH_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(settingKey, maxDepth));
        try {
            installView("in_layer_1", "FROM test | WHERE id IN (FROM test | KEEP id) | KEEP id");
            installView("in_layer_2", "FROM test | WHERE id IN (FROM in_layer_1 | KEEP id) | KEEP id");
            installView("in_layer_3", "FROM test | WHERE id IN (FROM in_layer_2 | KEEP id) | KEEP id");

            String query = "FROM test | WHERE id IN (FROM in_layer_3 | KEEP id)";
            VerificationException e = expectThrows(VerificationException.class, () -> run(query));
            assertThat(e.getMessage(), containsString("The maximum allowed view depth of " + maxDepth + " has been exceeded"));
        } finally {
            deleteViews("in_layer_1", "in_layer_2", "in_layer_3");
            updateClusterSettings(Settings.builder().putNull(settingKey));
        }
    }
}
