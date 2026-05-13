/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/**
 * End-to-end coverage for the PreAnalysisVerifier
 *
 * <p>Unit tests in {@code PreAnalysisVerifierTests} cover the verifier in isolation,
 * but they call {@code PreAnalysisVerifier.verify} directly — they don't exercise
 * the wiring inside {@code EsqlSession}. This integration test runs a real query
 * against a real cluster and confirms that the failure surfaces from the actual
 * request path, with the field-caps round trip short-circuited.
 *
 * <p>TODO: when the InSubquery feature is fully implemented, the rejection assertions
 * here turn into success assertions. Until then, this class doubles as a guard against
 * accidentally exposing a half-built feature to clients.
 */
public class PreAnalysisVerifierIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setUpIndices() {
        // Two indices that actually exist, to make it obvious the rejection happens
        // regardless of whether the subquery references a resolvable index.
        assertAcked(client().admin().indices().prepareCreate("main_index"));
        indexRandom(true, "main_index", 1);
        assertAcked(client().admin().indices().prepareCreate("sub_index"));
        indexRandom(true, "sub_index", 1);
    }

    // TODO: Remove once WHERE IN subquery is fully in snapshot
    public void testWhereInSubqueryRejectedAtRuntime() {
        assumeTrue("IN subquery is not enabled", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
        expectThrows(
            VerificationException.class,
            containsString("IN subquery is not yet supported"),
            () -> run(syncEsqlQueryRequest("FROM main_index | WHERE x IN (FROM sub_index)"))
        );
    }

    public void testInSubqueryOutsideWhereRejectedAtRuntime() {
        assumeTrue("IN subquery is not enabled", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
        expectThrows(
            VerificationException.class,
            containsString("IN subquery is not supported in [EVAL y = x IN (FROM sub_index)]"),
            () -> run(syncEsqlQueryRequest("FROM main_index | EVAL y = x IN (FROM sub_index)"))
        );
    }

    public void testInSubqueryInStatsWhereRejectedAtRuntime() {
        assumeTrue("IN subquery is not enabled", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
        expectThrows(
            VerificationException.class,
            containsString("IN subquery is not supported in [STATS c = COUNT(*) WHERE x IN (FROM sub_index)]"),
            () -> run(syncEsqlQueryRequest("FROM main_index | STATS c = COUNT(*) WHERE x IN (FROM sub_index)"))
        );
    }

    public void testInSubqueryInInlineStatsWhereRejectedAtRuntime() {
        assumeTrue("IN subquery is not enabled", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());

        expectThrows(
            VerificationException.class,
            containsString("IN subquery is not supported in [INLINE STATS c = COUNT(*) WHERE x IN (FROM sub_index)]"),
            () -> run(syncEsqlQueryRequest("FROM main_index | INLINE STATS c = COUNT(*) WHERE x IN (FROM sub_index)"))
        );
    }

    public void testInSubqueryUsedAsValueRejectedAtRuntime() {
        assumeTrue("IN subquery is not enabled", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());

        expectThrows(
            VerificationException.class,
            containsString("IN subquery is not supported within other expressions [MV_CONTAINS(x IN (FROM main_index), [true, false])]"),
            () -> run(syncEsqlQueryRequest("FROM main_index | WHERE emp_no > 0 and MV_CONTAINS(x IN (FROM main_index), [true, false])"))
        );
    }
}
