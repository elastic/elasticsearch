/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TermsGroupByIT extends ContinuousTestCase {

    private static final String NAME = "continuous-terms-pivot-test";
    private static final String MISSING_BUCKET_KEY = "~~NULL~~"; // ensure that this key is last after sorting

    private final boolean missing;
    private final String metricField;
    private final String termsField;

    public TermsGroupByIT() {
        missing = randomBoolean();
        metricField = randomFrom(METRIC_FIELDS);
        termsField = randomFrom(TERMS_FIELDS);
    }

    @Override
    public TransformConfig createConfig() throws IOException {
        TransformConfig.Builder transformConfigBuilder = new TransformConfig.Builder();
        addCommonBuilderParameters(transformConfigBuilder);
        transformConfigBuilder.setSource(new SourceConfig(CONTINUOUS_EVENTS_SOURCE_INDEX));
        transformConfigBuilder.setDest(new DestConfig(NAME, INGEST_PIPELINE));
        transformConfigBuilder.setId(NAME);

        var groupConfig = TransformRestTestCase.createGroupConfig(
            Map.of("event", new TermsGroupSource(termsField, null, missing)),
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
                "event": {
                  "terms": {
                    "field": "%s",
                    "order": {"_key": "asc"},
                    %s
                    "size": 1000
                  },
                  "aggs": {
                    "metric.avg": {
                      "avg" : {
                        "field": "%s"
                      }
                    }
                  }
                }
              },
              "sort": ["event"]
            }
            """.formatted(termsField, missing ? "\"missing\": \"" + MISSING_BUCKET_KEY + "\"," : "", metricField);

        var searchResponseSource = entityAsMap(
            search(CONTINUOUS_EVENTS_SOURCE_INDEX, query, Map.of("allow_partial_search_results", "false", "size", "0"))
        );

        String destQuery = """
            {
              "sort": ["event"]
            }
            """;
        var searchResponseDest = entityAsMap(search(NAME, destQuery, Map.of("allow_partial_search_results", "false", "size", "1000")));
        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue("aggregations.event.buckets", searchResponseSource);

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

            String transformBucketKey = (String) XContentMapValues.extractValue("event", source);
            if (transformBucketKey == null) {
                transformBucketKey = MISSING_BUCKET_KEY;
            }

            // test correctness, the results from the aggregation and the results from the transform should be the same
            assertThat(
                "Buckets did not match, source: " + source + ", expected: " + bucket.get("key") + ", iteration: " + iteration,
                transformBucketKey,
                equalTo(bucket.get("key"))
            );
            assertThat(
                "Doc count did not match, source: " + source + ", expected: " + bucket.get("doc_count") + ", iteration: " + iteration,
                (Integer) XContentMapValues.extractValue("count", source),
                equalTo(bucket.get("doc_count"))
            );

            var aggValue = (Double) XContentMapValues.extractValue("metric.avg.value", bucket);
            assertThat(
                "Metric aggregation did not match, source: " + source + ", expected: " + aggValue + ", iteration: " + iteration,
                (Double) XContentMapValues.extractValue("metric.avg", source),
                equalTo(aggValue)
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
