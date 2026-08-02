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

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The single place that demonstrates PromQL {@code label_replace}/{@code label_join} queries that ES|QL does <b>not</b>
 * yet handle the way Prometheus does (tracked in
 * <a href="https://github.com/elastic/elasticsearch/issues/136256">#136256</a>).
 * <p>
 * Each test runs the real query end-to-end and asserts the <b>Prometheus-correct</b> outcome, so it <b>fails against the
 * current implementation</b>. That failure is the demonstration: it pins the exact gap so a future implementation can
 * see it disappear once fixed. This is an internalClusterTest (not a unit test) because every gap here is a wrong
 * <i>value</i>/<i>result</i> that only surfaces when the query executes over real time-series data.
 */
public class PromqlLabelFunctionsUnsupportedIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "prom-metrics-unsupported";

    /**
     * A1: {@code without} over a <em>derived</em> label. {@code sum without (region)} groups by every label except
     * {@code region}, so the result must not carry {@code region}. {@code label_replace} derives {@code region} from
     * {@code pod}, and {@code without (region)} should drop it again - but the exclusion is pushed to the storage layer,
     * where a derived label does not exist, so it is ignored and {@code region} stays baked into the {@code _timeseries}
     * identity.
     */
    public void testWithoutDerivedLabelShouldDropIt() throws IOException {
        assumeLabelFunctionsAndWithout();
        createPromIndex();
        indexSample("2024-05-10T00:58:04.000Z", "podD", "1", 2.0);
        client().admin().indices().prepareRefresh(INDEX).get();

        String query = "PROMQL index="
            + INDEX
            + " step=1h result=(sum without (region) (label_replace(metrics.requests{pod=\"podD\"}, \"region\", \"$1\", \"pod\", \"(.+)\")))"
            + " | EVAL has_region = _timeseries LIKE \"*region*\" | KEEP has_region";

        List<List<Object>> rows;
        try (var resp = run(query)) {
            rows = getValuesList(resp);
        }
        assertThat(rows, hasSize(1));
        assertThat("`without (region)` must drop the derived label, but it survives in the identity", rows.get(0).get(0), equalTo(false));
    }

    /**
     * A2: {@code without ()} over a relabel. {@code without ()} groups by every label, so every distinct series must
     * survive. Over a relabel the whole-series {@code _timeseries} identity is dropped, collapsing distinct series into
     * one - unlike {@code sum without () (metric)}, which keeps them.
     */
    public void testWithoutEmptyOverRelabelShouldKeepAllSeries() throws IOException {
        assumeLabelFunctionsAndWithout();
        createPromIndex();
        indexSample("2024-05-10T00:58:04.000Z", "podD", "1", 2.0);
        indexSample("2024-05-10T00:58:05.000Z", "podE", "1", 4.0);
        client().admin().indices().prepareRefresh(INDEX).get();

        String query = "PROMQL index="
            + INDEX
            + " step=1h result=(sum without () (label_replace(metrics.requests, \"region\", \"$1\", \"pod\", \"(.+)\")))";

        List<List<Object>> rows;
        try (var resp = run(query)) {
            rows = getValuesList(resp);
        }
        assertThat("`without ()` groups by all labels, so both series must survive", rows, hasSize(2));
    }

    private void assumeLabelFunctionsAndWithout() {
        assumeTrue("promql label functions available in snapshot builds only", EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.isEnabled());
        assumeTrue("requires promql without grouping", EsqlCapabilities.Cap.PROMQL_WITHOUT_GROUPING.isEnabled());
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
