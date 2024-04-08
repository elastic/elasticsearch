/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.maxBucket;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

public class MetadataIT extends ESIntegTestCase {

    public void testMetadataSetOnAggregationResult() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("idx").setMapping("name", "type=keyword").get());
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomInt(30)];
        for (int i = 0; i < builders.length; i++) {
            String name = "name_" + randomIntBetween(1, 10);
            builders[i] = prepareIndex("idx").setSource("name", name, "value", randomInt());
        }
        indexRandom(true, builders);
        ensureSearchable();

        final var nestedMetadata = Map.of("nested", "value");
        var metadata = Map.of("key", "value", "numeric", 1.2, "bool", true, "complex", nestedMetadata);

        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                terms("the_terms").setMetadata(metadata).field("name").subAggregation(sum("the_sum").setMetadata(metadata).field("value"))
            ).addAggregation(maxBucket("the_max_bucket", "the_terms>the_sum").setMetadata(metadata)),
            response -> {
                InternalAggregations aggs = response.getAggregations();
                assertNotNull(aggs);

                Terms terms = aggs.get("the_terms");
                assertNotNull(terms);
                assertMetadata(terms.getMetadata());

                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                for (Terms.Bucket bucket : buckets) {
                    InternalAggregations subAggs = bucket.getAggregations();
                    assertNotNull(subAggs);

                    Sum sum = subAggs.get("the_sum");
                    assertNotNull(sum);
                    assertMetadata(sum.getMetadata());
                }

                InternalBucketMetricValue maxBucket = aggs.get("the_max_bucket");
                assertNotNull(maxBucket);
                assertMetadata(maxBucket.getMetadata());
            }
        );
    }

    private void assertMetadata(Map<String, Object> returnedMetadata) {
        assertNotNull(returnedMetadata);
        assertEquals(4, returnedMetadata.size());
        assertEquals("value", returnedMetadata.get("key"));
        assertEquals(1.2, returnedMetadata.get("numeric"));
        assertEquals(true, returnedMetadata.get("bool"));

        Object nestedObject = returnedMetadata.get("complex");
        assertNotNull(nestedObject);

        Map<?, ?> nestedMap = (Map<?, ?>) nestedObject;
        assertEquals("value", nestedMap.get("nested"));
    }
}
