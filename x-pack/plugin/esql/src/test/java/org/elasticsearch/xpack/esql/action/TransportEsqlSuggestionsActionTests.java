/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.hamcrest.Matchers.equalTo;

/**
 * Exercises the coordinator-only completion path of the transport action ({@link
 * TransportEsqlSuggestionsAction#suggest}) — the fallback used for a remote-qualified query, and the
 * unit-testable half of the remote-index detection ({@link TransportEsqlSuggestionsAction#hasRemoteTarget}) that
 * decides whether the real, analysis-wired path ({@link TransportEsqlSuggestionsAction#suggestFromAnalyzedPlan})
 * is reachable at all. The full analysis-wired path itself needs a real cluster to resolve indices against; see
 * {@code EsqlSuggestionsActionIT} for that.
 */
public class TransportEsqlSuggestionsActionTests extends ESTestCase {

    public void testPipePositionReturnsEmptyWarnings() {
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query("FROM foo | KEEP a\n").cursor("FROM foo | KEEP a\n".length());
        EsqlSuggestionsResponse response = TransportEsqlSuggestionsAction.suggest(TEST_PARSER, request);
        assertNotNull(response.fields());
        assertTrue(response.warnings().isEmpty());
    }

    public void testStringLiteralContextReturnsSkeleton() {
        String query = "FROM foo | WHERE agent == \"as\"";
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(query.indexOf("as\"") + 1);
        EsqlSuggestionsResponse response = TransportEsqlSuggestionsAction.suggest(TEST_PARSER, request);
        // Single-field literal context: no coordinator-side type is known, so the field map is empty
        // (values would come from a deferred data-node visit).
        assertTrue(response.fields().isEmpty());
        assertTrue(response.warnings().isEmpty());
    }

    public void testHasRemoteTargetDetectsClusterQualifiedFrom() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM my_cluster:logs | KEEP a");
        assertThat(TransportEsqlSuggestionsAction.hasRemoteTarget(plan), equalTo(true));
    }

    public void testHasRemoteTargetIgnoresPlainLocalFrom() {
        LogicalPlan plan = TEST_PARSER.parseQuery("FROM logs,other_logs | KEEP a");
        assertThat(TransportEsqlSuggestionsAction.hasRemoteTarget(plan), equalTo(false));
    }
}
