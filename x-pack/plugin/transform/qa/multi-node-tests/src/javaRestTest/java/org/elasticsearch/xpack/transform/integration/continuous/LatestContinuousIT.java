/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.latest.LatestConfig;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class LatestContinuousIT extends ContinuousTestCase {

    private static final String NAME = "continuous-latest-test";

    private static final String EVENT_FIELD = "event";
    private static final String TIMESTAMP_FIELD = "timestamp";

    private static final String MISSING_BUCKET_KEY = "~~NULL~~"; // ensure that this key is last after sorting

    public LatestContinuousIT() {}

    @Override
    public TransformConfig createConfig() {
        TransformConfig.Builder transformConfigBuilder =
            new TransformConfig.Builder()
                .setId(NAME)
                .setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX))
                .setDest(new DestConfig(NAME, INGEST_PIPELINE))
                .setLatestConfig(
                    LatestConfig.builder()
                        .setUniqueKey(EVENT_FIELD)
                        .setSort(TIMESTAMP_FIELD)
                        .build());
        addCommonBuilderParameters(transformConfigBuilder);
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
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
                                .missing(MISSING_BUCKET_KEY)
                                .order(BucketOrder.key(true))
                                .subAggregation(AggregationBuilders.max("max_timestamp").field(TIMESTAMP_FIELD))));
        SearchResponse searchResponseSource = search(searchRequestSource);

        SearchRequest searchRequestDest =
            new SearchRequest(NAME)
                .allowPartialSearchResults(false)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .source(new SearchSourceBuilder().size(1000).sort(EVENT_FIELD));
        SearchResponse searchResponseDest = search(searchRequestDest);

        List<? extends Bucket> buckets = ((Terms) searchResponseSource.getAggregations().get("by_event")).getBuckets();

        // the number of search hits should be equal to the number of buckets returned by the aggregation
        assertThat(
            new ParameterizedMessage(
                "Number of buckets did not match, source: {}, expected: {}, iteration: {}",
                searchResponseDest.getHits().getTotalHits().value, Long.valueOf(buckets.size()), iteration).getFormattedMessage(),
            searchResponseDest.getHits().getTotalHits().value,
            is(equalTo(Long.valueOf(buckets.size())))
        );

        Iterator<? extends Bucket> sourceIterator = buckets.iterator();
        Iterator<SearchHit> destIterator = searchResponseDest.getHits().iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Bucket bucket = sourceIterator.next();
            SearchHit searchHit = destIterator.next();
            Map<String, Object> source = searchHit.getSourceAsMap();
            String eventFieldValue = (String) XContentMapValues.extractValue(EVENT_FIELD, source);
            String timestampFieldValue = (String) XContentMapValues.extractValue(TIMESTAMP_FIELD, source);

            String transformBucketKey =
                Optional.ofNullable((String) XContentMapValues.extractValue(EVENT_FIELD, source))
                    .orElse(MISSING_BUCKET_KEY);

            // Verify that the results from the aggregation and the results from the transform are the same.
            assertThat(
                new ParameterizedMessage(
                    "Buckets did not match, source: {}, expected: {}, iteration: {}",
                    source, bucket.getKey(), iteration).getFormattedMessage(),
                transformBucketKey,
                is(equalTo(bucket.getKey()))
            );
            String maxTimestampValueAsString = ((SingleValue) bucket.getAggregations().get("max_timestamp")).getValueAsString();
            // In the assertion below we only take 3 fractional (i.e.: after a dot) digits for comparison.
            // This is due to the lack of precision of the max aggregation value which is represented as "double".
            assertThat(
                new ParameterizedMessage(
                    "Timestamps did not match, source: {}, expected: {}, iteration: {}",
                    source, maxTimestampValueAsString, iteration).getFormattedMessage(),
                timestampFieldValue.substring(0, timestampFieldValue.lastIndexOf('.') + 3),
                is(equalTo(maxTimestampValueAsString.substring(0, maxTimestampValueAsString.lastIndexOf('.') + 3)))
            );

            // Verify that transform only rewrites documents that require it.
            // Whether or not the document got rewritten is reflected in the field set by the ingest pipeline.
            if (modifiedEvents.contains(eventFieldValue)) {
                assertThat(XContentMapValues.extractValue(INGEST_RUN_FIELD, source), is(equalTo(iteration)));
            } else {
                assertThat(XContentMapValues.extractValue(INGEST_RUN_FIELD, source), is(not(equalTo(iteration))));
            }
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }
}
