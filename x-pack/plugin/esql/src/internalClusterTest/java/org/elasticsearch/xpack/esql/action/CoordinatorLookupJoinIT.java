/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class CoordinatorLookupJoinIT extends AbstractEsqlIntegTestCase {

    public void testFullTextFunctionsNotAllowedAfterCoordinatorLookupJoin() {
        assertAcked(client().admin().indices().prepareCreate("data").setMapping("key", "type=keyword", "country", "type=keyword"));
        client().prepareBulk()
            .add(prepareIndex("data").setSource(Map.of("key", "us", "country", "United States")))
            .add(prepareIndex("data").setSource(Map.of("key", "uk", "country", "United Kingdom")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("lookup")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
                .setMapping("key", "type=keyword", "info", "type=keyword")
        );
        client().prepareBulk()
            .add(prepareIndex("lookup").setSource(Map.of("key", "us", "info", "America")))
            .add(prepareIndex("lookup").setSource(Map.of("key", "uk", "info", "Britain")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        String expectedError =
            "cannot be used in a WHERE clause that references both data-side and lookup-side fields after LOOKUP JOIN _coordinator:";

        expectThrows(VerificationException.class, containsString(expectedError), () -> run("""
            FROM data
            | LOOKUP JOIN _coordinator:lookup ON key
            | WHERE match(country, "United States") OR info IS NOT NULL
            | KEEP key, country, info
            | SORT key
            """).close());

        expectThrows(VerificationException.class, containsString(expectedError), () -> run("""
            FROM data
            | LOOKUP JOIN _coordinator:lookup ON key
            | WHERE match_phrase(country, "United States") OR info IS NOT NULL
            | KEEP key, country, info
            | SORT key
            """).close());

        expectThrows(VerificationException.class, containsString(expectedError), () -> run("""
            FROM data
            | LOOKUP JOIN _coordinator:lookup ON key
            | WHERE country : "United States" OR info IS NOT NULL
            | KEEP key, country, info
            | SORT key
            """).close());
    }
}
