/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.DocValuesBatchPrefetcher;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

/** Fetching synthetic {@code _source} returns the same documents whether the dynamic prefetch budget is off or on. */
public class FetchDocValuesPrefetchSingleNodeTests extends ESSingleNodeTestCase {

    public void testSameSourceWithAndWithoutPrefetch() throws Exception {
        createIndex(
            "test",
            Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), "synthetic").build(),
            "long",
            "type=long",
            "keyword",
            "type=keyword",
            "text",
            "type=keyword,doc_values=false,store=true"
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        int numDocs = randomIntBetween(20, 200);
        for (int i = 0; i < numDocs; i++) {
            Map<String, Object> doc = new HashMap<>();
            if (randomBoolean()) {
                doc.put("long", randomLong());
            }
            if (randomBoolean()) {
                doc.put("keyword", randomAlphaOfLength(5));
            }
            doc.put("text", randomAlphaOfLength(8));
            bulk.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource(doc));
        }
        assertNoFailures(bulk.get());

        Map<String, Map<String, Object>> off = sources(numDocs);
        updateBudget(randomIntBetween(1, 64));
        try {
            assertThat(sources(numDocs), equalTo(off));
        } finally {
            updateBudget(null);
        }
    }

    private Map<String, Map<String, Object>> sources(int numDocs) {
        Map<String, Map<String, Object>> sources = new HashMap<>();
        assertNoFailuresAndResponse(client().prepareSearch("test").setSize(numDocs), response -> {
            for (SearchHit hit : response.getHits()) {
                sources.put(hit.getId(), hit.getSourceAsMap());
            }
        });
        assertThat(sources.size(), equalTo(numDocs));
        return sources;
    }

    private void updateBudget(Integer budget) {
        String key = DocValuesBatchPrefetcher.BUDGET_SETTING.getKey();
        Settings.Builder settings = budget == null ? Settings.builder().putNull(key) : Settings.builder().put(key, budget);
        clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings).get();
    }
}
