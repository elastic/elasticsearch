/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class DelayedShardAggregationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(DelayedShardAggregationPlugin.class);
    }

    public void testSimple() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("index"));
        float expectedMax = Float.MIN_VALUE;
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            float rand = randomFloat();
            expectedMax = Math.max(rand, expectedMax);
            reqs.add(client().prepareIndex("index").setSource("number", rand));
        }
        indexRandom(true, reqs);
        SearchResponse response = client().prepareSearch("index")
            .addAggregation(
                new DelayedShardAggregationBuilder("delay", TimeValue.timeValueMillis(10)).subAggregation(
                    new MaxAggregationBuilder("max").field("number")
                )
            )
            .get();
        Aggregations aggs = response.getAggregations();
        assertThat(aggs.get("delay"), instanceOf(InternalFilter.class));
        InternalFilter filter = aggs.get("delay");
        InternalMax max = filter.getAggregations().get("max");
        assertThat((float) max.getValue(), equalTo(expectedMax));
    }
}
