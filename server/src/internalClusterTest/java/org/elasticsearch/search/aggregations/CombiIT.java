/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.missing.Missing;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.missing;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class CombiIT extends ESIntegTestCase {

    /**
     * Making sure that if there are multiple aggregations, working on the same field, yet require different
     * value source type, they can all still work. It used to fail as we used to cache the ValueSource by the
     * field name. If the cached value source was of type "bytes" and another aggregation on the field required to see
     * it as "numeric", it didn't work. Now we cache the Value Sources by a custom key (field name + ValueSource type)
     * so there's no conflict there.
     */
    public void testMultipleAggsOnSameField_WithDifferentRequiredValueSourceType() throws Exception {

        createIndex("idx");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[randomInt(30)];
        Map<Integer, Integer> values = new HashMap<>();
        long missingValues = 0;
        for (int i = 0; i < builders.length; i++) {
            String name = "name_" + randomIntBetween(1, 10);
            if (rarely()) {
                missingValues++;
                builders[i] = prepareIndex("idx").setSource(jsonBuilder().startObject().field("name", name).endObject());
            } else {
                int value = randomIntBetween(1, 10);
                values.put(value, values.getOrDefault(value, 0) + 1);
                builders[i] = prepareIndex("idx").setSource(
                    jsonBuilder().startObject().field("name", name).field("value", value).endObject()
                );
            }
        }
        indexRandom(true, builders);
        ensureSearchable();

        final long finalMissingValues = missingValues;
        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(missing("missing_values").field("value"))
                .addAggregation(terms("values").field("value").collectMode(aggCollectionMode)),
            response -> {
                InternalAggregations aggs = response.getAggregations();

                Missing missing = aggs.get("missing_values");
                assertNotNull(missing);
                assertThat(missing.getDocCount(), equalTo(finalMissingValues));

                Terms terms = aggs.get("values");
                assertNotNull(terms);
                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                assertThat(buckets.size(), equalTo(values.size()));
                for (Terms.Bucket bucket : buckets) {
                    values.remove(((Number) bucket.getKey()).intValue());
                }
                assertTrue(values.isEmpty());
            }
        );
    }

    /**
     * Some top aggs (eg. date_/histogram) that are executed on unmapped fields, will generate an estimate count of buckets - zero.
     * when the sub aggregator is then created, it will take this estimation into account. This used to cause
     * and an ArrayIndexOutOfBoundsException...
     */
    public void testSubAggregationForTopAggregationOnUnmappedField() throws Exception {

        prepareCreate("idx").setMapping(
            jsonBuilder().startObject()
                .startObject("_doc")
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("value")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ).get();

        ensureSearchable("idx");

        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());
        assertNoFailuresAndResponse(
            prepareSearch("idx").addAggregation(
                histogram("values").field("value1").interval(1).subAggregation(terms("names").field("name").collectMode(aggCollectionMode))
            ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), Matchers.equalTo(0L));
                Histogram values = response.getAggregations().get("values");
                assertThat(values, notNullValue());
                assertThat(values.getBuckets().isEmpty(), is(true));
            }
        );
    }
}
