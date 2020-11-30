/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.latest.LatestDocConfig;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LatestDocContinuousIT extends ContinuousTestCase {

    private static final String NAME = "continuous-latest-test";

    private static final String EVENT_FIELD = "event";
    private static final String TIMESTAMP_FIELD = "timestamp";

    private static final String MISSING_BUCKET_KEY = "~~NULL~~"; // ensure that this key is last after sorting

    public LatestDocContinuousIT() {}

    @Override
    public TransformConfig createConfig() {
        TransformConfig.Builder transformConfigBuilder =
            new TransformConfig.Builder()
                .setId(NAME)
                .setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX))
                .setDest(new DestConfig(NAME, INGEST_PIPELINE))
                .setLatestDocConfig(
                    LatestDocConfig.builder()
                        .setUniqueKey(EVENT_FIELD)
                        .setSort(SortBuilders.fieldSort(TIMESTAMP_FIELD).order(SortOrder.DESC))
                        .build());
        addCommonBuilderParameters(transformConfigBuilder);
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void testIteration(int iteration) throws IOException {
        SearchRequest searchRequestSource =
            new SearchRequest(CONTINUOUS_EVENTS_SOURCE_INDEX)
                .allowPartialSearchResults(false)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .source(
                    new SearchSourceBuilder()
                        .size(0)
                        .aggregation(
                            new TermsAggregationBuilder("by_event")
                                .size(1000)
                                .field(EVENT_FIELD)
                                .order(BucketOrder.key(true))
                                .subAggregation(AggregationBuilders.max("max_timestamp").field(TIMESTAMP_FIELD))));
        SearchResponse responseSource = search(searchRequestSource);

        SearchRequest searchRequestDest =
            new SearchRequest(NAME)
                .allowPartialSearchResults(false)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .source(new SearchSourceBuilder().size(1000).sort(EVENT_FIELD));
        SearchResponse responseDest = search(searchRequestDest);

        List<? extends Bucket> buckets = ((Terms) responseSource.getAggregations().get("by_event")).getBuckets();

        // the number of search hits should be equal to the number of buckets returned by the aggregation
        assertThat(
            "Number of buckets did not match, source: "
                + responseDest.getHits().getTotalHits().value
                + ", expected: "
                + Long.valueOf(buckets.size())
                + ", iteration: "
                + iteration,
            responseDest.getHits().getTotalHits().value,
            is(equalTo(Long.valueOf(buckets.size())))
        );

        Iterator<? extends Bucket> sourceIterator = buckets.iterator();
        Iterator<SearchHit> destIterator = responseDest.getHits().iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Bucket bucket = sourceIterator.next();
            SearchHit searchHit = destIterator.next();
            Map<String, Object> source = searchHit.getSourceAsMap();

            String transformBucketKey = (String) XContentMapValues.extractValue(EVENT_FIELD, source);
            if (transformBucketKey == null) {
                transformBucketKey = MISSING_BUCKET_KEY;
            }

            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucket.getKey() + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucket.getKey())
            );
            SingleValue maxAgg = bucket.getAggregations().get("max_timestamp");
            String timestampValueAsString = (String) XContentMapValues.extractValue(TIMESTAMP_FIELD, source);
            assertThat(
                "Metric aggregation did not match, source: " + source + ", expected: " + maxAgg.value() + ", iteration: " + iteration,
                timestampValueAsString.substring(0, timestampValueAsString.lastIndexOf('.')),
                equalTo(maxAgg.getValueAsString().substring(0, maxAgg.getValueAsString().lastIndexOf('.')))
            );

            // test optimization, transform should only rewrite documents that require it
            // run.ingest is set by the pipeline, run.max is set by the transform
            // run.ingest > run.max means, the data point has been re-created/re-fed although it wasn't necessary,
            // this is probably a bug in transform's change collection optimization
            // run.ingest < run.max means the ingest pipeline wasn't updated, this might be a bug in put pipeline
            assertThat(
                "Ingest run: "
                    + XContentMapValues.extractValue(INGEST_RUN_FIELD, source)
                    + " did not match max run: "
                    + iteration
                    + ", iteration: "
                    + iteration
                    + " full source: "
                    + source,
                (int) XContentMapValues.extractValue(INGEST_RUN_FIELD, source),  // TODO: Make it stricter
                is(lessThanOrEqualTo(iteration))
            );
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }
}
