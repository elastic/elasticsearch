/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class GlobalIT extends ESIntegTestCase {

    static int numDocs;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx2");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        numDocs = randomIntBetween(3, 20);
        for (int i = 0; i < numDocs / 2; i++) {
            builders.add(client().prepareIndex("idx").setId(""+i+1).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag1")
                    .endObject()));
        }
        for (int i = numDocs / 2; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx").setId(""+i+1).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i + 1)
                    .field("tag", "tag2")
                    .field("name", "name" + i+1)
                    .endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testWithStatsSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                .addAggregation(global("global")
                        .subAggregation(stats("value_stats").field("value")))
                .get();

        assertSearchResponse(response);


        Global global = response.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo((long) numDocs));
        assertThat((long) ((InternalAggregation)global).getProperty("_count"), equalTo((long) numDocs));
        assertThat(global.getAggregations().asList().isEmpty(), is(false));

        Stats stats = global.getAggregations().get("value_stats");
        assertThat((Stats) ((InternalAggregation)global).getProperty("value_stats"), sameInstance(stats));
        assertThat(stats, notNullValue());
        assertThat(stats.getName(), equalTo("value_stats"));
        long sum = 0;
        for (int i = 0; i < numDocs; ++i) {
            sum += i + 1;
        }
        assertThat(stats.getAvg(), equalTo((double) sum / numDocs));
        assertThat(stats.getMin(), equalTo(1.0));
        assertThat(stats.getMax(), equalTo((double) numDocs));
        assertThat(stats.getCount(), equalTo((long) numDocs));
        assertThat(stats.getSum(), equalTo((double) sum));
    }

    public void testNonTopLevel() throws Exception {
        try {
            client().prepareSearch("idx")
                    .setQuery(QueryBuilders.termQuery("tag", "tag1"))
                    .addAggregation(global("global")
                            .subAggregation(global("inner_global")))
                    .get();

            fail("expected to fail executing non-top-level global aggregator. global aggregations are only allowed as top level" +
                    "aggregations");

        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }
}
