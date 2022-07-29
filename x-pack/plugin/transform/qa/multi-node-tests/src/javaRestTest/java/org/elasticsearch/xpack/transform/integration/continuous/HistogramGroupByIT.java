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
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.transform.integration.TransformRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class HistogramGroupByIT extends ContinuousTestCase {
    private static final String NAME = "continuous-histogram-pivot-test";

    private final String metricField;

    public HistogramGroupByIT() {
        metricField = randomFrom(METRIC_FIELDS);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public TransformConfig createConfig() throws IOException {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);

        var groupConfig = TransformRestTestCase.createGroupConfig(
            Map.of("metric", new HistogramGroupSource(metricField, null, false, 50.0)),
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
    @SuppressWarnings("unchecked")
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {
        String querySource = """
            {
              "aggs": {
                "metric": {
                  "histogram": {
                    "field": "%s",
                    "order": {"_key": "asc"},
                    "interval": "50.0"
                  }
                }
              }
            }
            """.formatted(metricField);

        Response searchResponseSource = search(
            CONTINUOUS_EVENTS_SOURCE_INDEX,
            querySource,
            Map.of("allow_partial_search_results", "false", "size", "0")
        );

        String destQuery = """
            {
              "sort": ["metric"]
            }
            """;
        Response searchResponseDest = search(NAME, destQuery, Map.of("allow_partial_search_results", "false", "size", "10000"));

        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "aggregations.metric.buckets",
            entityAsMap(searchResponseSource)
        );
        var sourceIterator = buckets.iterator();

        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", entityAsMap(searchResponseDest));
        var destIterator = hits.iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            var bucket = sourceIterator.next();
            var searchHit = destIterator.next();
            var source = (Map<String, Object>) searchHit.get("_source");

            long transformBucketKey = ((Integer) extractValue("metric", source)).longValue();

            // aggs return buckets with 0 doc_count while composite aggs skip over them
            while ((Integer) bucket.get("doc_count") == 0) {
                assertTrue(sourceIterator.hasNext());
                bucket = sourceIterator.next();
            }
            long bucketKey = ((Double) bucket.get("key")).longValue();

            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucketKey + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucketKey)
            );
            assertThat(
                "Doc count did not match, source: " + source + ", expected: " + bucket.get("doc_count") + ", iteration: " + iteration,
                (Integer) extractValue("count", source),
                equalTo(bucket.get("doc_count"))
            );

            // test optimization, transform should only rewrite documents that require it
            // we artificially created a trend, that's why smaller buckets should not get rewritten
            if (transformBucketKey < iteration * METRIC_TREND) {
                assertThat(
                    "Ingest run: "
                        + extractValue(INGEST_RUN_FIELD, source)
                        + " did not match max run: "
                        + extractValue(MAX_RUN_FIELD, source)
                        + ", iteration: "
                        + iteration
                        + " full source: "
                        + source,
                    (Integer) extractValue(INGEST_RUN_FIELD, source) - (Integer) extractValue(MAX_RUN_FIELD, source),
                    is(lessThanOrEqualTo(1))
                );
            }
        }

        assertFalse(sourceIterator.hasNext());
        assertFalse(destIterator.hasNext());
    }

}
