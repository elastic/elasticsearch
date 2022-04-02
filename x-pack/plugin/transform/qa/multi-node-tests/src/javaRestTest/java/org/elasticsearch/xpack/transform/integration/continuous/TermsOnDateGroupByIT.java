/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.xpack.transform.integration.TransformRestTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test case for `terms` on a date type field
 *
 * Note: dates are currently written as long (epoch_ms) into the transform dest index.
 */
public class TermsOnDateGroupByIT extends ContinuousTestCase {

    private static final String NAME = "continuous-terms-on-date-pivot-test";
    private static final String MISSING_BUCKET_KEY = ContinuousTestCase.STRICT_DATE_OPTIONAL_TIME_PRINTER_NANOS.withZone(ZoneId.of("UTC"))
        .format(Instant.ofEpochMilli(1262304000000L)); // 01/01/2010 should end up last when sorting

    private final boolean missing;
    private final String metricField;
    private final String timestampField;

    public TermsOnDateGroupByIT() {
        missing = randomBoolean();
        metricField = randomFrom(METRIC_FIELDS);
        timestampField = randomFrom(OTHER_TIMESTAMP_FIELDS);
    }

    @Override
    public TransformConfig createConfig() throws IOException {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);

        var groupConfig = TransformRestTestCase.createGroupConfig(
            Map.of("some-timestamp", new TermsGroupSource(timestampField, null, missing)),
            xContentRegistry()
        );

        AggregatorFactories.Builder aggregations = new AggregatorFactories.Builder();
        addCommonAggregations(aggregations);
        aggregations.addAggregator(AggregationBuilders.avg("metric.avg").field(metricField));

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
                "some-timestamp": {
                  "terms": {
                    "field": "%s",
                    "order": {"_key": "asc"},
                    %s
                    "size": "1000"
                  },
                  "aggs": {
                    "metric.avg": {
                      "avg" : {
                        "field": "%s"
                      }
                    }
                  }
                }
              }
            }
            """.formatted(timestampField, missing ? "\"missing\": \"" + MISSING_BUCKET_KEY + "\"," : "", metricField);

        Response searchResponseSource = search(
            CONTINUOUS_EVENTS_SOURCE_INDEX,
            query,
            Map.of("allow_partial_search_results", "false", "size", "0")
        );

        String destQuery = """
            {
              "sort": ["some-timestamp"]
            }
            """;
        var searchResponseDest = entityAsMap(search(NAME, destQuery, Map.of("allow_partial_search_results", "false", "size", "1000")));

        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "aggregations.some-timestamp.buckets",
            entityAsMap(searchResponseSource)
        );

        // the number of search hits should be equal to the number of buckets returned by the aggregation
        int numHits = (Integer) XContentMapValues.extractValue("hits.total.value", searchResponseDest);
        assertThat(
            "Number of buckets did not match, source: " + numHits + ", expected: " + buckets.size() + ", iteration: " + iteration,
            numHits,
            equalTo(buckets.size())
        );

        var sourceIterator = buckets.iterator();
        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResponseDest);
        var destIterator = hits.iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            var bucket = sourceIterator.next();
            var searchHit = destIterator.next();
            var source = (Map<String, Object>) searchHit.get("_source");

            String transformBucketKey = (String) XContentMapValues.extractValue("some-timestamp", source);

            if (transformBucketKey == null) {
                transformBucketKey = MISSING_BUCKET_KEY;
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

            var avgAggValue = (Double) XContentMapValues.extractValue("metric.avg.value", bucket);
            assertThat(
                "Metric aggregation did not match, source: " + source + ", expected: " + avgAggValue + ", iteration: " + iteration,
                XContentMapValues.extractValue("metric.avg", source),
                equalTo(avgAggValue)
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
                    + XContentMapValues.extractValue(MAX_RUN_FIELD, source)
                    + ", iteration: "
                    + iteration
                    + " full source: "
                    + source,
                XContentMapValues.extractValue(INGEST_RUN_FIELD, source),
                equalTo(XContentMapValues.extractValue(MAX_RUN_FIELD, source))
            );
        }
        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }
}
