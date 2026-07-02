/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;

/**
 * Exercises the coordinator-only completion path of the transport action ({@link
 * TransportEsqlSuggestionsAction#suggest}). The parser used here is the same stateless parser the
 * action builds; index schema is not resolved on the coordinator, so field-name completion is
 * populated only where the schema is statically knowable — this asserts the shape and the honest
 * limitation rather than data-node statistics (which are deferred).
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
}
