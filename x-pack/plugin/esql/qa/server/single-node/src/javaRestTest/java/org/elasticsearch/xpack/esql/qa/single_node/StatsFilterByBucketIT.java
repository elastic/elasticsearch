/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Regression test for <a href="https://github.com/elastic/elasticsearch/issues/162338">#162338</a>:
 * {@code STATS ... WHERE MATCH(...) BY BUCKET(...)} returned wrong counts when the documents of a later bucket
 * had lower doc ids than those of an earlier bucket.
 * <p>
 * Grouping by a date bucket is rewritten into one query slice per bucket, and a single driver scores the slices
 * in bucket order. The index holds the newest bucket first, so the doc ids of each bucket are below those of the
 * bucket before it and the driver scores the dense doc id ranges of a segment in decreasing order.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class StatsFilterByBucketIT extends ESRestTestCase {

    private static final String INDEX = "stats-filter-by-bucket";
    private static final int DAYS = 5;
    private static final int DOCS_PER_DAY = 20;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void setupIndex() throws IOException {
        assumeTrue("requires the task_concurrency pragma", Build.current().isSnapshot());
        if (indexExists(INDEX)) {
            return;
        }
        Request createIndex = new Request("PUT", "/" + INDEX);
        createIndex.setJsonEntity("""
            {
              "settings": { "index.number_of_shards": 1 },
              "mappings": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "message": { "type": "text" }
                }
              }
            }
            """);
        assertOK(client().performRequest(createIndex));

        // The newest day comes first, so every day has lower doc ids than the day before it. Within day d, the
        // first d documents match and the rest do not, so every day has a different number of matches.
        StringBuilder bulk = new StringBuilder();
        for (int day = DAYS; day >= 1; day--) {
            for (int i = 0; i < DOCS_PER_DAY; i++) {
                bulk.append("{\"index\": {}}\n");
                bulk.append(
                    Strings.format(
                        "{\"@timestamp\": \"2026-01-%02dT%02d:00:00Z\", \"message\": \"%s\"}\n",
                        day,
                        i,
                        i < day ? "needle in a haystack" : "just hay"
                    )
                );
            }
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        assertThat(entityAsMap(client().performRequest(bulkRequest)).get("errors"), equalTo(false));

        // The doc ids only follow the order the documents were indexed in if they all live in one segment.
        assertOK(client().performRequest(new Request("POST", "/" + INDEX + "/_forcemerge?max_num_segments=1")));
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    public void testFilteredCountByBucket() throws IOException {
        List<List<Object>> values = values(
            "FROM " + INDEX + " | STATS c = COUNT(*) WHERE MATCH(message, \"needle\") BY b = BUCKET(@timestamp, 1 day) | SORT b"
        );
        assertThat(values, hasSize(DAYS));
        for (int day = 1; day <= DAYS; day++) {
            assertThat("day " + day, ((Number) values.get(day - 1).get(0)).intValue(), equalTo(day));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> values(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        try (XContentBuilder body = JsonXContent.contentBuilder()) {
            body.startObject().field("query", query);
            // A single driver scores the buckets one after another, in bucket order.
            body.startObject("pragma").field("task_concurrency", 1).endObject();
            body.endObject();
            request.setJsonEntity(Strings.toString(body));
        }
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        return (List<List<Object>>) response.get("values");
    }
}
