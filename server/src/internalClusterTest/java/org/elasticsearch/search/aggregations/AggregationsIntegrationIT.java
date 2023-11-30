/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

@ESIntegTestCase.SuiteScopeTestCase
public class AggregationsIntegrationIT extends ESIntegTestCase {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("index").setMapping("f", "type=keyword").get());
        numDocs = randomIntBetween(1, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            docs.add(prepareIndex("index").setSource("f", Integer.toString(i / 3)));
        }
        indexRandom(true, docs);
    }

    public void testScroll() {
        final int size = randomIntBetween(1, 4);
        final String[] scroll = new String[1];
        final int[] total = new int[1];
        assertNoFailuresAndResponse(
            prepareSearch("index").setSize(size).setScroll(TimeValue.timeValueMinutes(1)).addAggregation(terms("f").field("f")),
            response -> {
                Aggregations aggregations = response.getAggregations();
                assertNotNull(aggregations);
                Terms terms = aggregations.get("f");
                assertEquals(Math.min(numDocs, 3L), terms.getBucketByKey("0").getDocCount());
                scroll[0] = response.getScrollId();
                total[0] = response.getHits().getHits().length;
            }
        );
        int currentTotal = 0;
        while (total[0] - currentTotal > 0) {
            currentTotal = total[0];
            assertNoFailuresAndResponse(
                client().prepareSearchScroll(scroll[0]).setScroll(TimeValue.timeValueMinutes(1)),
                scrollResponse -> {
                    assertNull(scrollResponse.getAggregations());
                    total[0] += scrollResponse.getHits().getHits().length;
                    scroll[0] = scrollResponse.getScrollId();
                }
            );
        }
        clearScroll(scroll[0]);
        assertEquals(numDocs, total[0]);
    }
}
