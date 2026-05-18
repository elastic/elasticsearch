/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.xpack.transform.integration.TransformRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Testcase for date histogram group_by on _different_ fields than used for sync
 */
public class DateHistogramGroupByOtherTimeFieldIT extends ContinuousTestCase {
    private static final String NAME = "continuous-date-histogram-pivot-other-timefield-test";

    private final boolean addGroupByTerms;
    private final boolean datesAsEpochMillis;
    private final String metricTimestampField;
    private final String termsField;

    public DateHistogramGroupByOtherTimeFieldIT() {
        addGroupByTerms = randomBoolean();
        datesAsEpochMillis = randomBoolean();
        metricTimestampField = randomFrom(METRIC_TIMESTAMP_FIELDS);
        termsField = randomFrom(TERMS_FIELDS);
    }

    @Override
    public TransformConfig createConfig() throws IOException {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        if (datesAsEpochMillis) {
            transformConfigBuilder.setSettings(addCommonSettings(new SettingsConfig.Builder()).setDatesAsEpochMillis(true).build());
        }
        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, null, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);

        Map<String, SingleGroupSource> groupSource = new HashMap<>();
        groupSource.put(
            "second",
            new DateHistogramGroupSource(
                metricTimestampField,
                null,
                false,
                new DateHistogramGroupSource.FixedInterval(DateHistogramInterval.SECOND),
                null,
                null
            )
        );
        if (addGroupByTerms) {
            groupSource.put("event", new TermsGroupSource(termsField, null, false));
        }
        var groupConfig = TransformRestTestCase.createGroupConfig(groupSource, xContentRegistry());

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
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
        String eventAgg = Strings.format("""
            , "aggs" : {"event": {"terms": {"field": "%s", "size": 1000, "order": {"_key": "asc"}}}}
            """, termsField);

        Object[] args = new Object[] { metricTimestampField, addGroupByTerms ? eventAgg : "" };
        String querySource = Strings.format("""
            {
              "aggs": {
                "second": {
                  "date_histogram": {
                    "field": "%s",
                    "order": {"_key": "asc"},
                    "fixed_interval": "1s"
                  }%s
                }
              }
            }
            """, args);

        Response searchResponseSource = search(
            CONTINUOUS_EVENTS_SOURCE_INDEX,
            querySource,
            Map.of("allow_partial_search_results", "false", "size", "100")
        );

        String queryDest = addGroupByTerms ? """
            {
                "sort": ["second", "event"]
            }
            """ : """
            {
                "sort": ["second"]
            }
            """;
        Response searchResponseDest = search(NAME, queryDest, Map.of("allow_partial_search_results", "false", "size", "10000"));

        if (addGroupByTerms) {
            assertResultsGroupByDateHistogramAndTerms(iteration, searchResponseSource, searchResponseDest);
        } else {
            assertResultsGroupByDateHistogram(iteration, searchResponseSource, searchResponseDest);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertResultsGroupByDateHistogram(int iteration, Response responseSource, Response responseDest) throws IOException {
        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "aggregations.second.buckets",
            entityAsMap(responseSource)
        );

        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", entityAsMap(responseDest));

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
            assertThat(
                "Ingest run: "
                    + XContentMapValues.extractValue(INGEST_RUN_FIELD, source)
                    + " did not match max run: "
                    + XContentMapValues.extractValue(MAX_RUN_FIELD, source)
                    + ", iteration: "
                    + iteration,
                // we use a fixed_interval of `1s`, the transform runs every `1s`, a bucket might be recalculated at the next run
                // but should NOT be recalculated for the 2nd/3rd/... run
                (Integer) XContentMapValues.extractValue(INGEST_RUN_FIELD, source) - (Integer) XContentMapValues.extractValue(
                    MAX_RUN_FIELD,
                    source
                ),
                is(lessThanOrEqualTo(1))
            );

        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }

    @SuppressWarnings("unchecked")
    private void assertResultsGroupByDateHistogramAndTerms(int iteration, Response responseSource, Response responseDest)
        throws IOException {
        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "aggregations.second.buckets",
            entityAsMap(responseSource)
        );

        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", entityAsMap(responseDest));

        List<Map<String, Object>> flattenedBuckets = new ArrayList<>();
        for (var b : buckets) {
            if ((Integer) b.get("doc_count") == 0) {
                continue;
            }
            var terms = ((List<Map<String, Object>>) XContentMapValues.extractValue("event.buckets", b));
            for (var t : terms) {
                flattenedBuckets.add(flattenedResult((String) b.get("key_as_string"), (String) t.get("key"), (Integer) t.get("doc_count")));
            }
        }

        Iterator<Map<String, Object>> sourceIterator = flattenedBuckets.iterator();
        Iterator<Map<String, Object>> destIterator = hits.iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            Map<String, Object> bucket = sourceIterator.next();

            var searchHit = destIterator.next();
            var source = (Map<String, Object>) searchHit.get("_source");

            String transformBucketKey;
            if (datesAsEpochMillis) {
                transformBucketKey = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
                    .format(Instant.ofEpochMilli((Long) XContentMapValues.extractValue("second", source)));
            } else {
                transformBucketKey = (String) XContentMapValues.extractValue("second", source);
            }

            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucket.get("second") + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucket.get("second"))
            );
            assertThat(
                "Doc count did not match, source: " + source + ", expected: " + bucket.get("count") + ", iteration: " + iteration,
                (Integer) XContentMapValues.extractValue("count", source),
                equalTo(bucket.get("count"))
            );
            assertThat(
                "Term did not match, source: " + source + ", expected: " + bucket.get("event") + ", iteration: " + iteration,
                XContentMapValues.extractValue("event", source),
                equalTo(bucket.get("event"))
            );

            // transform should only rewrite documents that require it
            assertThat(
                "Ingest run: "
                    + XContentMapValues.extractValue(INGEST_RUN_FIELD, source)
                    + " did not match max run: "
                    + XContentMapValues.extractValue(MAX_RUN_FIELD, source)
                    + ", iteration: "
                    + iteration,
                // we use a fixed_interval of `1s`, the transform runs every `1s`, a bucket might be recalculated at the next run
                // but should NOT be recalculated for the 2nd/3rd/... run
                (Integer) XContentMapValues.extractValue(INGEST_RUN_FIELD, source) - (Integer) XContentMapValues.extractValue(
                    MAX_RUN_FIELD,
                    source
                ),
                is(lessThanOrEqualTo(2))
            );
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }

    private static Map<String, Object> flattenedResult(String second, String event, int count) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("second", second);
        doc.put("event", event);
        doc.put("count", count);
        return doc;
    }
}
