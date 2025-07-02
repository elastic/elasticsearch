/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IndexResolutionIT extends AbstractEsqlIntegTestCase {

    public void testResolvesConcreteIndex() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesAlias() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);
        assertAcked(client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("index-1", "alias-1"));

        try (var response = run(syncEsqlQueryRequest().query("FROM alias-1"))) {
            assertOk(response);
        }
    }

    public void testResolvesDataStream() {
        // TODO
    }

    public void testResolvesPattern() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);
        assertAcked(client().admin().indices().prepareCreate("index-2"));
        indexRandom(true, "index-2", 10);

        try (var response = run(syncEsqlQueryRequest().query("FROM index-*"))) {
            assertOk(response);
        }
    }

    public void testDoesNotResolveMissingIndex() {
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [no-such-index]"),
            () -> run(syncEsqlQueryRequest().query("FROM no-such-index"))
        );
    }

    public void testDoesNotResolveEmptyPattern() {
        expectThrows(
            VerificationException.class,
            containsString("Unknown index [index-*]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-*"))
        );
    }

    public void testDoesNotResolveClosedIndex() {
        assertAcked(client().admin().indices().prepareCreate("index-1"));
        indexRandom(true, "index-1", 10);
        assertAcked(client().admin().indices().prepareClose("index-1"));

        expectThrows(
            ClusterBlockException.class,
            containsString("index [index-1] blocked by: [FORBIDDEN/4/index closed]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-1"))
        );
    }

    public void testPartialResolution() {
        // TODO
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
    }
}
