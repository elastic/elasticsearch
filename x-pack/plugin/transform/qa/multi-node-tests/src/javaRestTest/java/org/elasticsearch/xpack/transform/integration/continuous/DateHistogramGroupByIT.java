/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.transform.integration.TransformRestTestCase;

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
    public TransformConfig createConfig() throws IOException {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        if (datesAsEpochMillis) {
            transformConfigBuilder.setSettings(addCommonSettings(new SettingsConfig.Builder()).setDatesAsEpochMillis(true).build());
        }

        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);

        var groupConfig = TransformRestTestCase.createGroupConfig(
            Map.of(
                "second",
                new DateHistogramGroupSource(
                    timestampField,
                    null,
                    missing,
                    new DateHistogramGroupSource.FixedInterval(DateHistogramInterval.SECOND),
                    null
                )
            ),
            xContentRegistry()
        );

        AggregatorFactories.Builder aggregations = new AggregatorFactories.Builder();
        addCommonAggregations(aggregations);

        PivotConfig pivotConfig = new PivotConfig(
            groupConfig,
            TransformRestTestCase.createAggConfig(aggregations, xContentRegistry()),
            null
        );

        transformConfigBuilder.setPivotConfig(pivotConfig);
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
        String query = """
            {
              "aggs": {
                "second": {
                  "date_histogram": {
                    "field": "%s",
                    "order": {"_key": "asc"},
                    %s
                    "fixed_interval": "1s"
                  }
                }
              }
            }
            """.formatted(timestampField, missing ? "\"missing\": \"" + MISSING_BUCKET_KEY + "\"," : "");

        Response searchResponse = search(
            CONTINUOUS_EVENTS_SOURCE_INDEX,
            query,
            Map.of("allow_partial_search_results", "false", "size", "100")
        );

        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "aggregations.second.buckets",
            entityAsMap(searchResponse)
        );

        String destQuery = """
            {
              "sort": ["second"]
            }
            """;
        var searchResponseDest = entityAsMap(search(NAME, destQuery, Map.of("allow_partial_search_results", "false", "size", "100")));
        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResponseDest);

        Iterator<Map<String, Object>> sourceIterator = buckets.iterator();
        Iterator<Map<String, Object>> destIterator = hits.iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            var bucket = sourceIterator.next();
            var searchHit = destIterator.next();
            var source = (Map<String, Object>) searchHit.get("_source");

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
            while ((Integer) bucket.get("doc_count") == 0) {
                assertTrue(sourceIterator.hasNext());
                bucket = sourceIterator.next();
            }

            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucket.get("key_as_string") + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucket.get("key_as_string"))
            );
            assertThat(
                "Doc count did not match, source: " + source + ", expected: " + bucket.get("doc_count") + ", iteration: " + iteration,
                (Integer) XContentMapValues.extractValue("count", source),
                equalTo(bucket.get("doc_count"))
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
