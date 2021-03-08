/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.persistent;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.persistent.GetPersistentSearchAction;
import org.elasticsearch.action.search.persistent.GetPersistentSearchRequest;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchAction;
import org.elasticsearch.action.search.persistent.SubmitPersistentSearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.core.IsEqual.equalTo;

public class PersistentSearchIT extends ESIntegTestCase {
    public void testBasicPersistentSearch() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(indexName));
        PutMappingRequest request = new PutMappingRequest().indices(indexName).source("key", "type=keyword");
        client().admin().indices().putMapping(request).actionGet();
        ensureGreen(indexName);

        int docCount = randomIntBetween(50, 300);
        populateIndex(indexName, docCount);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);
        searchRequest.indices(indexName);
        searchRequest.source(SearchSourceBuilder.searchSource().aggregation(new TermsAggregationBuilder("agg").field("key")));

        boolean withoutHits = randomBoolean();
        if (withoutHits) {
            searchRequest.source().size(0);
        }

        final SubmitPersistentSearchResponse submitPersistentSearchResponse =
            client().execute(SubmitPersistentSearchAction.INSTANCE, searchRequest).actionGet();

        final GetPersistentSearchRequest getReq =
            new GetPersistentSearchRequest(submitPersistentSearchResponse.getSearchId().getSearchId());

        assertBusy(() -> {
            final PersistentSearchResponse persistentSearchResponse =
                client().execute(GetPersistentSearchAction.INSTANCE, getReq).actionGet();

            assertNotNull(persistentSearchResponse);
            if (withoutHits == false) {
                final SearchHit[] hits = persistentSearchResponse.getSearchResponse().getHits().getHits();
                assertThat(hits.length, equalTo(10));
            }

            final StringTerms agg = persistentSearchResponse.getSearchResponse().getAggregations().get("agg");
            assertThat(agg.getBucketByKey("bar").getDocCount(), equalTo((long) docCount));
        });

        assertBusy(() -> {
            final SearchRequest getPersistentSearchResultDocs
                = new SearchRequest().allowPartialSearchResults(false).indices(PersistentSearchResultsIndexStore.INDEX);
            final SearchResponse searchResponse =
                client().search(getPersistentSearchResultDocs).actionGet();
            // Only the final reduced result is kept around
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        });
    }

    public void testPersistentSearchWithCanMatch() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(indexName));
        PutMappingRequest request = new PutMappingRequest().indices(indexName).source("key", "type=keyword", "date", "type=long");
        client().admin().indices().putMapping(request).actionGet();
        ensureGreen(indexName);

        int docCount = randomIntBetween(50, 300);
        populateIndex(indexName, docCount);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(false);
        searchRequest.indices(indexName);
        searchRequest.source(SearchSourceBuilder.searchSource()
            .query(QueryBuilders.rangeQuery("date").from(0).to(73).includeLower(true).includeUpper(true))
            .aggregation(new TermsAggregationBuilder("agg").field("key"))
        );

        boolean withoutHits = randomBoolean();
        if (withoutHits) {
            searchRequest.source().size(0);
        }

        final SubmitPersistentSearchResponse submitPersistentSearchResponse =
            client().execute(SubmitPersistentSearchAction.INSTANCE, searchRequest).actionGet();

        final GetPersistentSearchRequest getReq =
            new GetPersistentSearchRequest(submitPersistentSearchResponse.getSearchId().getSearchId());

        assertBusy(() -> {
            final PersistentSearchResponse persistentSearchResponse =
                client().execute(GetPersistentSearchAction.INSTANCE, getReq).actionGet();

            assertNotNull(persistentSearchResponse);
            if (withoutHits == false) {
                final SearchHit[] hits = persistentSearchResponse.getSearchResponse().getHits().getHits();
                assertThat(hits.length, equalTo(10));
            }

            final StringTerms agg = persistentSearchResponse.getSearchResponse().getAggregations().get("agg");
            assertThat(agg.getBucketByKey("bar").getDocCount(), equalTo((long) docCount));
        });

        assertBusy(() -> {
            final SearchRequest getPersistentSearchResultDocs
                = new SearchRequest().allowPartialSearchResults(false).indices(PersistentSearchResultsIndexStore.INDEX);
            final SearchResponse searchResponse =
                client().search(getPersistentSearchResultDocs).actionGet();
            // Only the final reduced result is kept around
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        });
    }

    private void populateIndex(String indexName, int docCount) throws InterruptedException {
        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final String key = "key";
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource(key, "bar", "data", randomAlphaOfLength(4), "date", 12));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);
    }
}
