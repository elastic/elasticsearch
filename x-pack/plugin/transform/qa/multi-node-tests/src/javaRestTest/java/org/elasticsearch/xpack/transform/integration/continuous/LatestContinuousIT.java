/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration.continuous;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.latest.LatestConfig;

import java.io.IOException;
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

    private static final Map<String, Object> RUNTIME_MAPPINGS = Map.of(
        "event-upper-at-search",
        Map.of(
            "type",
            "keyword",
            "script",
            singletonMap("source", "if (params._source.event != null) {emit(params._source.event.toUpperCase())}")
        )
    );

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
            .setSource(new SourceConfig(new String[] { CONTINUOUS_EVENTS_SOURCE_INDEX }, QueryConfig.matchAll(), RUNTIME_MAPPINGS))
            .setDest(new DestConfig(NAME, INGEST_PIPELINE))
            .setLatestConfig(new LatestConfig(List.of(eventField), timestampField));
        addCommonBuilderParameters(transformConfigBuilder);
        return transformConfigBuilder.build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void testIteration(int iteration, Set<String> modifiedEvents) throws IOException {

        var runtimeMappings = toJson(RUNTIME_MAPPINGS);
        String query = """
            {
              "runtime_mappings" : %s,
              "aggs": {
                "by_event": {
                  "terms": {
                    "size": 1000,
                    "field": "%s",
                    "order": {"_key": "asc"},
                    "missing": "%s"
                  },
                  "aggs": {
                    "max_timestamp": {
                      "max": {
                        "field": "%s"
                      }
                    }
                  }
                }
              }
            }
            """.formatted(runtimeMappings, eventField, MISSING_BUCKET_KEY, timestampField);

        Response searchResponseSource = search(
            CONTINUOUS_EVENTS_SOURCE_INDEX,
            query,
            Map.of("allow_partial_search_results", "false", "size", "0")
        );

        var buckets = (List<Map<String, Object>>) XContentMapValues.extractValue(
            "aggregations.by_event.buckets",
            entityAsMap(searchResponseSource)
        );

        Response searchResponseDest = search(NAME, """
            {
              "sort": ["event.keyword"]
            }
            """, Map.of("allow_partial_search_results", "false", "size", "1000"));
        var searchResponse = entityAsMap(searchResponseDest);

        // the number of search hits should be equal to the number of buckets returned by the aggregation
        int numHits = (Integer) XContentMapValues.extractValue("hits.total.value", searchResponse);
        assertThat(
            new ParameterizedMessage(
                "Number of buckets did not match, source: {}, expected: {}, iteration: {}",
                numHits,
                buckets.size(),
                iteration
            ).getFormattedMessage(),
            numHits,
            is(equalTo(buckets.size()))
        );

        var sourceIterator = buckets.iterator();
        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", searchResponse);
        var destIterator = hits.iterator();

        while (sourceIterator.hasNext() && destIterator.hasNext()) {
            var bucket = sourceIterator.next();
            var searchHit = destIterator.next();
            var source = (Map<String, Object>) searchHit.get("_source");
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
                    bucket.get("key"),
                    iteration
                ).getFormattedMessage(),
                transformBucketKey,
                is(equalTo(bucket.get("key")))
            );
            logger.info("bucket" + bucket);
            var maxTimestampValueAsString = (String) XContentMapValues.extractValue("max_timestamp.value_as_string", bucket);

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

    private String toJson(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(map);
            return Strings.toString(builder);
        }
    }
}
