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
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

/**
 * Negative integration tests for WHERE ... IN (subquery).
 * Verifies that unsupported usages of IN subquery are rejected with clear error messages,
 * and that type mismatches between left and right fields are caught.
 */
public class InSubqueryFailureIT extends AbstractEsqlIntegTestCase {

    @Before
    public void checkCapability() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
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
        assertThat(e.getMessage(), containsString("IN/NOT IN subquery is not supported in Eval [EVAL x = id IN (FROM test | KEEP id)]"));
    }

    public void testRejectsNotInSubqueryInEval() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | EVAL x = id NOT IN (FROM test | KEEP id)"));
        assertThat(
            e.getMessage(),
            containsString("IN/NOT IN subquery is not supported in Eval [EVAL x = id NOT IN (FROM test | KEEP id)]")
        );
    }

    public void testRejectsInSubqueryInSort() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | SORT id IN (FROM test | KEEP id)"));
        assertThat(e.getMessage(), containsString("IN/NOT IN subquery is not supported in OrderBy [SORT id IN (FROM test | KEEP id)]"));
    }

    public void testRejectsInSubqueryInStatsBy() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | STATS c = COUNT(*) BY id IN (FROM test | KEEP id)"));
        assertThat(
            e.getMessage(),
            containsString("IN/NOT IN subquery is not supported in Aggregate [STATS c = COUNT(*) BY id IN (FROM test | KEEP id)]")
        );
    }

    public void testRejectsInSubqueryInStatsWhereFilter() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | STATS c = COUNT(*) WHERE id IN (FROM test | KEEP id)"));
        assertThat(
            e.getMessage(),
            containsString("IN/NOT IN subquery is not supported in Aggregate [STATS c = COUNT(*) WHERE id IN (FROM test | KEEP id)]")
        );
    }

    public void testRejectsInSubqueryInWhereNonPredicate() {
        var e = expectThrows(VerificationException.class, () -> run("FROM test | WHERE CASE(id IN (FROM test | KEEP id), true, false)"));
        assertThat(
            e.getMessage(),
            containsString("IN/NOT IN subquery is not supported in Filter [WHERE CASE(id IN (FROM test | KEEP id), true, false)]")
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
}
