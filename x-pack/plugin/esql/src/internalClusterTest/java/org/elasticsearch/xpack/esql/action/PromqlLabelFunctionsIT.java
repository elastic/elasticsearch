/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * End-to-end coverage for the PromQL {@code label_replace}/{@code label_join} collision guard. When a relabel maps two
 * distinct source series onto the same label set within the same time bucket, PromQL treats it as the evaluation error
 * "vector cannot contain metrics with the same labelset"; ES|QL surfaces the same condition as a hard query failure.
 * <p>
 * This lives in an internalClusterTest rather than csv-spec because csv-spec can only assert successful results and
 * response warnings, not a failed query.
 */
public class PromqlLabelFunctionsIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "prom-metrics-collision";

    public void testRelabelCollisionWithinBucketFailsQuery() throws IOException {
        assumeTrue("promql label functions available in snapshot builds only", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        createPromIndex();
        // Two distinct source series (idx=1 and idx=2) for the same pod in the same 1h bucket. Overwriting idx with a
        // constant collapses them onto the same identity within that bucket, which must fail rather than silently merge.
        indexSample("2024-05-10T00:58:04.000Z", "podD", "1", 2.0);
        indexSample("2024-05-10T00:58:05.000Z", "podD", "2", 4.0);
        client().admin().indices().prepareRefresh(INDEX).get();

        String query = "PROMQL index="
            + INDEX
            + " step=1h result=(label_replace(metrics.requests{pod=\"podD\"}, \"idx\", \"replaced\", \"idx\", \".*\"))";

        Exception e = expectThrows(Exception.class, () -> run(query).close());
        Throwable cause = e;
        while (cause != null && (cause.getMessage() == null || cause.getMessage().contains("same labelset") == false)) {
            cause = cause.getCause();
        }
        assertThat("expected the PromQL labelset collision failure", cause, notNullValue());
        assertThat(cause.getMessage(), containsString("vector cannot contain metrics with the same labelset"));
    }

    private void createPromIndex() throws IOException {
        Settings settings = Settings.builder()
            .put("mode", "time_series")
            .putList("routing_path", List.of("labels.*"))
            .put("time_series.start_time", "2024-05-10T00:00:00Z")
            .put("time_series.end_time", "2024-05-20T00:00:00Z")
            .build();

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        {
            mapping.startObject("@timestamp").field("type", "date").endObject();
            mapping.startObject("labels");
            {
                mapping.field("type", "passthrough").field("priority", 10).field("time_series_dimension", true);
                mapping.startObject("properties");
                {
                    mapping.startObject("__name__").field("type", "keyword").endObject();
                    mapping.startObject("pod").field("type", "keyword").endObject();
                    mapping.startObject("idx").field("type", "keyword").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
            mapping.startObject("metrics");
            {
                mapping.startObject("properties");
                {
                    mapping.startObject("requests").field("type", "double").field("time_series_metric", "gauge").endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();

        client().admin().indices().prepareCreate(INDEX).setSettings(settings).setMapping(mapping).get();
    }

    private void indexSample(String timestamp, String pod, String idx, double requests) throws IOException {
        XContentBuilder source = JsonXContent.contentBuilder().startObject();
        source.field("@timestamp", timestamp);
        source.startObject("labels").field("__name__", "over").field("pod", pod).field("idx", idx).endObject();
        source.startObject("metrics").field("requests", requests).endObject();
        source.endObject();
        client().prepareIndex(INDEX).setSource(source).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
    }
}
