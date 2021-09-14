/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SettingsConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class DateHistogramGroupByIT extends ContinuousTestCase {
    private static final String NAME = "continuous-date-histogram-pivot-test";
    private static final String MISSING_BUCKET_KEY = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
        .format(Instant.ofEpochMilli(42));

    private final boolean missing;
    private final boolean datesAsEpochMillis;
    private final String timestampField;

    public DateHistogramGroupByIT() {
        missing = randomBoolean();
        datesAsEpochMillis = randomBoolean();
        timestampField = randomFrom(TIMESTAMP_FIELDS);
    }

    @Override
    public TransformConfig createConfig() {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        if (datesAsEpochMillis) {
            transformConfigBuilder.setSettings(addCommonSetings(new SettingsConfig.Builder()).setDatesAsEpochMillis(true).build());
        }

        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);
        PivotConfig.Builder pivotConfigBuilder = new PivotConfig.Builder();
        pivotConfigBuilder.setGroups(
            new GroupConfig.Builder().groupBy(
                "second",
                new DateHistogramGroupSource.Builder().setField(timestampField)
                    .setInterval(new DateHistogramGroupSource.FixedInterval(DateHistogramInterval.SECOND))
                    .setMissingBucket(missing)
                    .build()
            ).build()
        );
        AggregatorFactories.Builder aggregations = new AggregatorFactories.Builder();
        addCommonAggregations(aggregations);

        pivotConfigBuilder.setAggregations(aggregations);
        transformConfigBuilder.setPivotConfig(pivotConfigBuilder.build());
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
        SearchRequest searchRequestSource = new SearchRequest(CONTINUOUS_EVENTS_SOURCE_INDEX).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilderSource = new SearchSourceBuilder().size(0);
        DateHistogramAggregationBuilder bySecond = new DateHistogramAggregationBuilder("second").field(timestampField)
            .fixedInterval(DateHistogramInterval.SECOND)
            .order(BucketOrder.key(true));
        if (missing) {
            // missing_bucket produces `null`, we can't use `null` in aggs, so we have to use a magic value, see gh#60043
            bySecond.missing(MISSING_BUCKET_KEY);
        }
        sourceBuilderSource.aggregation(bySecond);
        searchRequestSource.source(sourceBuilderSource);
        SearchResponse responseSource = search(searchRequestSource);

        SearchRequest searchRequestDest = new SearchRequest(NAME).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilderDest = new SearchSourceBuilder().size(100).sort("second");
        searchRequestDest.source(sourceBuilderDest);
        SearchResponse responseDest = search(searchRequestDest);

        List<? extends Bucket> buckets = ((Histogram) responseSource.getAggregations().get("second")).getBuckets();

        Iterator<? extends Bucket> sourceIterator = buckets.iterator();
        Iterator<SearchHit> destIterator = responseDest.getHits().iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Bucket bucket = sourceIterator.next();
            SearchHit searchHit = destIterator.next();
            Map<String, Object> source = searchHit.getSourceAsMap();

            String transformBucketKey;
            if (datesAsEpochMillis) {
                transformBucketKey = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
                    .format(Instant.ofEpochMilli((Long) XContentMapValues.extractValue("second", source)));
            } else {
                transformBucketKey = (String) XContentMapValues.extractValue("second", source);
            }

            if (transformBucketKey == null) {
                transformBucketKey = MISSING_BUCKET_KEY;
            }

            // aggs return buckets with 0 doc_count while composite aggs skip over them
            while (bucket.getDocCount() == 0L) {
                assertTrue(sourceIterator.hasNext());
                bucket = sourceIterator.next();
            }

            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucket.getKeyAsString() + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucket.getKeyAsString())
            );
            assertThat(
                "Doc count did not match, source: " + source + ", expected: " + bucket.getDocCount() + ", iteration: " + iteration,
                ((Integer) XContentMapValues.extractValue("count", source)).longValue(),
                equalTo(bucket.getDocCount())
            );

            // transform should only rewrite documents that require it
            if (missing == false) {
                assertThat(
                    "Ingest run: "
                        + XContentMapValues.extractValue(INGEST_RUN_FIELD, source)
                        + " did not match max run: "
                        + XContentMapValues.extractValue(MAX_RUN_FIELD, source)
                        + ", iteration: "
                        + iteration,
                    // we use a fixed_interval of `1s`, the transform runs every `1s` so it the bucket might be recalculated at the next run
                    // but
                    // should NOT be recalculated for the 2nd/3rd/... run
                    (Integer) XContentMapValues.extractValue(INGEST_RUN_FIELD, source) - (Integer) XContentMapValues.extractValue(
                        MAX_RUN_FIELD,
                        source
                    ),
                    is(lessThanOrEqualTo(1))
                );
            }
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }
}
