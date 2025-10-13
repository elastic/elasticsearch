/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LookupIndexResolutionIT extends AbstractEsqlIntegTestCase {

    public void testResolvesConcreteIndex() {
        createMainIndex("index-1");
        createLookupIndex("lookup");

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1 | LOOKUP JOIN lookup ON language_code"))) {
            assertOk(response);
        }
    }

    public void testResolvesAlias() {
        createMainIndex("index-1");
        createLookupIndex("lookup");
        assertAcked(
            client().admin().indices().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias("lookup", "lookup-alias")
        );

        try (var response = run(syncEsqlQueryRequest().query("FROM index-1 | LOOKUP JOIN lookup-alias ON language_code"))) {
            assertOk(response);
        }
    }

    public void testDoesNotResolveMissingIndex() {
        createMainIndex("index-1");

        expectThrows(
            VerificationException.class,
            containsString("Unknown index [fake-lookup]"),
            () -> run(syncEsqlQueryRequest().query("FROM index-1 | LOOKUP JOIN fake-lookup ON language_code"))
        );
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
    }

    private void createMainIndex(String name) {
        assertAcked(client().admin().indices().prepareCreate(name));
        indexRandom(true, false, prepareIndex(name).setSource(Map.of("id", randomIdentifier(), "language_code", 1)));
    }

    private void createLookupIndex(String name) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(name)
                .setSettings(
                    Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                )
        );
        indexRandom(true, false, prepareIndex(name).setSource(Map.of("language_code", 1, "language", "english")));
    }
}
