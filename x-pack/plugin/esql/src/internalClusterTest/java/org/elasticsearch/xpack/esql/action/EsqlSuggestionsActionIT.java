/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

/**
 * Exercises {@link TransportEsqlSuggestionsAction} through the real transport action against a real index
 * mapping: field-name completion resolves the index's actual mapped fields/types rather than a stub
 * schema (see {@code TransportEsqlSuggestionsActionTests} for the coordinator-only, unanalyzed fallback
 * path).
 */
public class EsqlSuggestionsActionIT extends AbstractEsqlIntegTestCase {

    public void testFieldNameCompletionResolvesRealIndexSchema() {
        assertAcked(client().admin().indices().prepareCreate("suggestions_test").setMapping("value", "type=long", "name", "type=keyword"));
        client().prepareIndex("suggestions_test").setSource("value", 1, "name", "a").get();
        client().admin().indices().prepareRefresh("suggestions_test").get();

        String query = "FROM suggestions_test | KEEP *";
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(query.length());
        EsqlSuggestionsResponse response = client().execute(EsqlSuggestionsAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);

        assertThat(response.fields(), hasKey("value"));
        assertThat(response.fields(), hasKey("name"));
        assertThat(response.fields().get("value").type(), equalTo("long"));
        assertThat(response.fields().get("name").type(), equalTo("keyword"));
    }

    public void testRemoteQualifiedTargetFallsBackToCoordinatorOnlyWithoutError() {
        String query = "FROM remote_cluster:suggestions_test | KEEP val*";
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(query).cursor(query.length());
        // No error, no attempted remote-cluster resolution: falls back to the parse-only skeleton.
        EsqlSuggestionsResponse response = client().execute(EsqlSuggestionsAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);
        assertThat(response.fields().isEmpty(), equalTo(true));
        assertThat(response.warnings().isEmpty(), equalTo(true));
    }
}
