/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Test that index enough data to trigger the creation of Cuckoo filters.
 */

public class RareTermsIT extends ESSingleNodeTestCase {

    private static final String index = "idx";

    private void indexDocs(int numDocs) {
        final BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < numDocs; ++i) {
            bulk.add(new IndexRequest(index).source("{\"str_value\" : \"s" + i + "\"}", XContentType.JSON));
        }
        assertNoFailures(bulk.get());
    }

    public void testSingleValuedString() {
        final Settings.Builder settings = indexSettings(2, 0);
        createIndex(index, settings.build());
        // We want to trigger the usage of cuckoo filters that happen only when there are
        // more than 10k distinct values in one shard.
        final int numDocs = randomIntBetween(12000, 17000);
        // Index every value 3 times
        for (int i = 0; i < 3; i++) {
            indexDocs(numDocs);
            assertNoFailures(client().admin().indices().prepareRefresh(index).get());
        }
        // There are no rare terms that only appear in one document
        assertNumRareTerms(1, 0);
        // All terms have a cardinality lower than 10
        assertNumRareTerms(10, numDocs);
    }

    private void assertNumRareTerms(int maxDocs, int rareTerms) {
        assertNoFailuresAndResponse(
            client().prepareSearch(index)
                .addAggregation(new RareTermsAggregationBuilder("rareTerms").field("str_value.keyword").maxDocCount(maxDocs)),
            response -> {
                final RareTerms terms = response.getAggregations().get("rareTerms");
                assertThat(terms.getBuckets().size(), Matchers.equalTo(rareTerms));
            }
        );
    }

    public void testGlobalAggregationWithScore() {
        createIndex("global", Settings.EMPTY, "_doc", "keyword", "type=keyword");
        prepareIndex("global").setSource("keyword", "a").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("global").setSource("keyword", "c").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("global").setSource("keyword", "e").setRefreshPolicy(IMMEDIATE).get();
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
            new RareTermsAggregationBuilder("terms").field("keyword")
                .subAggregation(
                    new RareTermsAggregationBuilder("sub_terms").field("keyword")
                        .subAggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"))
                )
        );
        assertNoFailuresAndResponse(client().prepareSearch("global").addAggregation(globalBuilder), response -> {
            InternalGlobal result = response.getAggregations().get("global");
            InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
            assertThat(terms.getBuckets().size(), equalTo(3));
            for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                InternalMultiBucketAggregation<?, ?> subTerms = bucket.getAggregations().get("sub_terms");
                assertThat(subTerms.getBuckets().size(), equalTo(1));
                MultiBucketsAggregation.Bucket subBucket = subTerms.getBuckets().get(0);
                InternalTopHits topHits = subBucket.getAggregations().get("top_hits");
                assertThat(topHits.getHits().getHits().length, equalTo(1));
                for (SearchHit hit : topHits.getHits()) {
                    assertThat(hit.getScore(), greaterThan(0f));
                }
            }
        });
    }
}
