/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class LatestContinuousIT extends ContinuousTestCase {

    private static final String NAME = "continuous-latest-test";

    private static final Map<String, Object> RUNTIME_MAPPINGS = new HashMap<String, Object>() {
        {
            put("event-upper-at-search", new HashMap<String, Object>() {
                {
                    put("type", "keyword");
                    put("script", singletonMap("source", "if (params._source.event != null) {emit(params._source.event.toUpperCase())}"));
                }
            });
        }
    };
    private static final String MISSING_BUCKET_KEY = "~~NULL~~"; // ensure that this key is last after sorting

    private final String eventField;
    private final String timestampField;

    public LatestContinuousIT() {
        eventField = randomFrom("event", "event-upper", "event-upper-at-search");
        timestampField = randomFrom(TIMESTAMP_FIELDS);
    }

    @Override
    public TransformConfig createConfig() {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder().setId(NAME)
            .setSource(SourceConfig.builder().setIndex(CONTINUOUS_EVENTS_SOURCE_INDEX).setRuntimeMappings(RUNTIME_MAPPINGS).build())
            .setDest(new DestConfig(NAME, INGEST_PIPELINE))
            .setLatestConfig(LatestConfig.builder().setUniqueKey(eventField).setSort(timestampField).build());
        addCommonBuilderParameters(transformConfigBuilder);
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
        SearchRequest searchRequestSource = new SearchRequest(CONTINUOUS_EVENTS_SOURCE_INDEX).allowPartialSearchResults(false)
            .source(
                new SearchSourceBuilder()
                    // runtime mappings are needed in case "event-upper-at-search" is selected as the event field in test constructor
                    .runtimeMappings(RUNTIME_MAPPINGS)
                    .size(0)
                    .aggregation(
                        new TermsAggregationBuilder("by_event").size(1000)
                            .field(eventField)
                            .missing(MISSING_BUCKET_KEY)
                            .order(BucketOrder.key(true))
                            .subAggregation(AggregationBuilders.max("max_timestamp").field(timestampField))
                    )
            );
        SearchResponse searchResponseSource = search(searchRequestSource);

        SearchRequest searchRequestDest = new SearchRequest(NAME).allowPartialSearchResults(false)
            // In destination index we don't have access to runtime fields from source index, let's use what we have i.e.: event.keyword
            // and assume the sorting order will be the same (it is true as the runtime field applies "toUpperCase()" which preserves
            // sorting order)
            .source(new SearchSourceBuilder().size(1000).sort("event.keyword"));
        SearchResponse searchResponseDest = search(searchRequestDest);

        List<? extends Bucket> buckets = ((Terms) searchResponseSource.getAggregations().get("by_event")).getBuckets();

        // the number of search hits should be equal to the number of buckets returned by the aggregation
        assertThat(
            new ParameterizedMessage(
                "Number of buckets did not match, source: {}, expected: {}, iteration: {}",
                searchResponseDest.getHits().getTotalHits().value,
                Long.valueOf(buckets.size()),
                iteration
            ).getFormattedMessage(),
            searchResponseDest.getHits().getTotalHits().value,
            is(equalTo(Long.valueOf(buckets.size())))
        );

        Iterator<? extends Bucket> sourceIterator = buckets.iterator();
        Iterator<SearchHit> destIterator = searchResponseDest.getHits().iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Bucket bucket = sourceIterator.next();
            SearchHit searchHit = destIterator.next();
            Map<String, Object> source = searchHit.getSourceAsMap();
            String eventFieldValue = (String) XContentMapValues.extractValue("event", source);
            String timestampFieldValue = (String) XContentMapValues.extractValue("timestamp", source);
            String transformBucketKey = eventFieldValue != null
                // The bucket key in source can be either an ordinary field or a runtime field. When it is runtime field, simulate its
                // script ("toUpperCase()") here.
                ? "event".equals(eventField) ? eventFieldValue : eventFieldValue.toUpperCase(Locale.ROOT)
                : MISSING_BUCKET_KEY;

            // Verify that the results from the aggregation and the results from the transform are the same.
            assertThat(
                new ParameterizedMessage(
                    "Buckets did not match, source: {}, expected: {}, iteration: {}",
                    source,
                    bucket.getKey(),
                    iteration
                ).getFormattedMessage(),
                transformBucketKey,
                is(equalTo(bucket.getKey()))
            );
            String maxTimestampValueAsString = ((SingleValue) bucket.getAggregations().get("max_timestamp")).getValueAsString();
            // In the assertion below we only take 3 fractional (i.e.: after a dot) digits for comparison.
            // This is due to the lack of precision of the max aggregation value which is represented as "double".
            assertThat(
                new ParameterizedMessage(
                    "Timestamps did not match, source: {}, expected: {}, iteration: {}",
                    source,
                    maxTimestampValueAsString,
                    iteration
                ).getFormattedMessage(),
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
