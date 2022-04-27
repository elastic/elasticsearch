/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search;

import org.apache.lucene.tests.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ClusterScope(scope = SUITE)
public class StressSearchServiceReaperIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // very frequent checks
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .build();
    }

    // see issue #5165 - this test fails each time without the fix in pull #5170
    public void testStressReaper() throws ExecutionException, InterruptedException {
        int num = randomIntBetween(100, 150);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[num];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test").setId("" + i).setSource("f", English.intToEnglish(i));
        }
        createIndex("test");
        indexRandom(true, builders);
        final int iterations = scaledRandomIntBetween(500, 1000);
        for (int i = 0; i < iterations; i++) {
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).setSize(num).get();
            assertNoFailures(searchResponse);
            assertHitCount(searchResponse, num);
        }
    }
}
