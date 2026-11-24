/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexFeatures;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.logsdb.TsdbIT.formatInstant;
import static org.elasticsearch.xpack.logsdb.TsdbIT.getTemplate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class TsdbIndexingRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

    static String BULK_ITEM_TEMPLATE =
        """
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "$name", "uid":"$uid", "ip": "$ip", "network": {"tx": $tx, "rx": $rx}}}}
            """;

    public void testIndexing() throws Exception {
        Boolean useSyntheticId = oldClusterHasFeature(IndexFeatures.TIME_SERIES_SYNTHETIC_ID) ? randomBoolean() : null;
        Boolean disableSeqNo = oldClusterHasFeature(IndexFeatures.TIME_SERIES_NO_SEQNO) ? randomBoolean() : null;

        String dataStreamName = "k9s";
        createTemplate(
            dataStreamName,
            getClass().getSimpleName().toLowerCase(Locale.ROOT),
            templateWithNetworkGauges(useSyntheticId, disableSeqNo)
        );

        Instant startTime = Instant.now().minusSeconds(60 * 60);
        bulkIndex(dataStreamName, 4, 1024, startTime, TsdbIndexingRollingUpgradeIT::docSupplier);

        String firstBackingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
        var settings = (Map<?, ?>) getIndexSettings(firstBackingIndex, true).get(firstBackingIndex);
        assertThat(((Map<?, ?>) settings.get("settings")).get(IndexSettings.MODE.getKey()), equalTo(IndexMode.TIME_SERIES.getName()));
        assertThat(
            ((Map<?, ?>) settings.get("defaults")).get(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey()),
            equalTo(SourceFieldMapper.Mode.SYNTHETIC.toString())
        );
        if (useSyntheticId != null) {
            assertThat(((Map<?, ?>) settings.get("settings")).get(IndexSettings.SYNTHETIC_ID.getKey()), equalTo(useSyntheticId.toString()));
        }
        if (disableSeqNo != null) {
            assertThat(
                ((Map<?, ?>) settings.get("settings")).get(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey()),
                equalTo(disableSeqNo.toString())
            );
        }

        var mapping = getIndexMappingAsMap(firstBackingIndex);
        assertThat(
            "incorrect k8s.pod.name field in mapping:" + mapping,
            "keyword",
            equalTo(ObjectPath.evaluate(mapping, "properties.k8s.properties.pod.properties.name.type"))
        );

        ensureGreen(dataStreamName);
        search(dataStreamName);
        query(dataStreamName);
        queryTimeSeries(dataStreamName);

        AtomicReference<Instant> startTimeRef = new AtomicReference<>(startTime);
        clusterRollingUpgrade(index -> {
            startTimeRef.set(startTimeRef.get().plusNanos(60 * 30));
            bulkIndex(dataStreamName, 4, 1024, startTime, TsdbIndexingRollingUpgradeIT::docSupplier);
            search(dataStreamName);
            query(dataStreamName);
            queryTimeSeries(dataStreamName);
        });

        var forceMergeRequest = new Request("POST", "/" + dataStreamName + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(forceMergeRequest));

        ensureGreen(dataStreamName);
        search(dataStreamName);
        query(dataStreamName);
        queryTimeSeries(dataStreamName);
    }

    /**
     * The shared TSDB template, with the network fields mapped as gauges. {@code METRICS_INFO} and {@code TS_INFO} only report
     * fields that are time series metrics.
     */
    private static String templateWithNetworkGauges(Boolean useSyntheticId, Boolean disableSeqNo) {
        String template = getTemplate(useSyntheticId, disableSeqNo);
        String withGauges = template.replace("\"type\": \"long\"", "\"type\": \"long\", \"time_series_metric\": \"gauge\"");
        assertThat("expected the network fields to be mapped as long", withGauges, not(equalTo(template)));
        return withGauges;
    }

    static String docSupplier(Instant time, int j) {
        String podName = "pod" + j % 5; // Not realistic, but makes asserting search / query response easier.
        String podUid = randomUUID();
        String podIp = NetworkAddress.format(randomIp(true));
        long podTx = randomLong();
        long podRx = randomLong();

        return BULK_ITEM_TEMPLATE.replace("$now", formatInstant(time))
            .replace("$name", podName)
            .replace("$uid", podUid)
            .replace("$ip", podIp)
            .replace("$tx", Long.toString(podTx))
            .replace("$rx", Long.toString(podRx));
    }

    void search(String dataStreamName) throws Exception {
        var searchRequest = new Request("POST", "/" + dataStreamName + "/_search");
        searchRequest.addParameter("pretty", "true");
        searchRequest.setJsonEntity("""
            {
                "size": 0,
                "aggs": {
                    "pod_name": {
                        "terms": {
                            "field": "k8s.pod.name",
                            "order": { "_key": "asc" }
                        },
                        "aggs": {
                            "max_tx": {
                                "max": {
                                    "field": "k8s.pod.network.tx"
                                }
                            },
                            "max_rx": {
                                "max": {
                                    "field": "k8s.pod.network.rx"
                                }
                            }
                        }
                    }
                }
            }
            """);
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);

        Integer totalCount = ObjectPath.evaluate(responseBody, "hits.total.value");
        assertThat(totalCount, greaterThanOrEqualTo(4096));
        String key = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.key");
        assertThat(key, equalTo("pod0"));
        Integer docCount = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.doc_count");
        assertThat(docCount, greaterThan(0));
        Double maxTx = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.max_tx.value");
        assertThat(maxTx, notNullValue());
        Double maxRx = ObjectPath.evaluate(responseBody, "aggregations.pod_name.buckets.0.max_rx.value");
        assertThat(maxRx, notNullValue());
    }

    void query(String dataStreamName) throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | STATS max(k8s.pod.network.rx), max(k8s.pod.network.tx) BY k8s.pod.name | SORT k8s.pod.name | LIMIT 5"
            }
            """.replace("$ds", dataStreamName));
        var response = client().performRequest(queryRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);

        String column1 = ObjectPath.evaluate(responseBody, "columns.0.name");
        String column2 = ObjectPath.evaluate(responseBody, "columns.1.name");
        String column3 = ObjectPath.evaluate(responseBody, "columns.2.name");
        assertThat(column1, equalTo("max(k8s.pod.network.rx)"));
        assertThat(column2, equalTo("max(k8s.pod.network.tx)"));
        assertThat(column3, equalTo("k8s.pod.name"));

        String key = ObjectPath.evaluate(responseBody, "values.0.2");
        assertThat(key, equalTo("pod0"));
        Long maxRx = ObjectPath.evaluate(responseBody, "values.0.0");
        assertThat(maxRx, notNullValue());
        Long maxTx = ObjectPath.evaluate(responseBody, "values.0.1");
        assertThat(maxTx, notNullValue());
    }

    /**
     * Runs {@code TS}, {@code METRICS_INFO} and {@code TS_INFO} against the data stream. Each command only runs once every node
     * supports it, so in a mixed cluster the old and new nodes exchange its plans and pages.
     */
    void queryTimeSeries(String dataStreamName) throws Exception {
        if (clusterSupportsEsql("ts_command_v0")) {
            Map<String, Object> response = esql(
                "TS " + dataStreamName + " | STATS max_rx = max(max_over_time(k8s.pod.network.rx)) BY metricset | LIMIT 10"
            );
            List<List<Object>> rows = ObjectPath.evaluate(response, "values");
            assertThat(rows, hasSize(1));
            assertThat(column(response, rows.getFirst(), "metricset"), equalTo("pod"));
            assertThat(column(response, rows.getFirst(), "max_rx"), notNullValue());
        }
        if (clusterSupportsEsql("metrics_info_command")) {
            Map<String, Object> response = esql("TS " + dataStreamName + " | METRICS_INFO | SORT metric_name | LIMIT 10");
            List<List<Object>> rows = ObjectPath.evaluate(response, "values");
            assertThat(rows, hasSize(2));
            assertThat(column(response, rows.get(0), "metric_name"), equalTo("k8s.pod.network.rx"));
            assertThat(column(response, rows.get(1), "metric_name"), equalTo("k8s.pod.network.tx"));
            for (List<Object> row : rows) {
                assertThat(column(response, row, "data_stream"), equalTo(dataStreamName));
                assertThat(column(response, row, "metric_type"), equalTo("gauge"));
            }
        }
        if (clusterSupportsEsql("ts_info_command")) {
            // Every document has its own uid, so there are far more series than the limit. The limit is pushed down to
            // TS_INFO, which sends it to the data nodes with the plan.
            Map<String, Object> response = esql("TS " + dataStreamName + " | TS_INFO | KEEP metric_name, data_stream | LIMIT 10");
            List<List<Object>> rows = ObjectPath.evaluate(response, "values");
            assertThat(rows, hasSize(10));
            for (List<Object> row : rows) {
                assertThat(column(response, row, "metric_name"), oneOf("k8s.pod.network.rx", "k8s.pod.network.tx"));
                assertThat(column(response, row, "data_stream"), equalTo(dataStreamName));
            }
        }
    }

    /** Whether every node supports the ES|QL capability. */
    private static boolean clusterSupportsEsql(String capability) throws IOException {
        Request request = new Request("GET", "/_capabilities");
        request.addParameter("method", "POST");
        request.addParameter("path", "/_query");
        request.addParameter("capabilities", capability);
        Response response = client().performRequest(request);
        assertOK(response);
        return Boolean.TRUE.equals(entityAsMap(response).get("supported"));
    }

    private static Map<String, Object> esql(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("query", query).endObject()));
        Response response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    private static Object column(Map<String, Object> response, List<Object> row, String name) throws IOException {
        List<Map<String, Object>> columns = ObjectPath.evaluate(response, "columns");
        for (int i = 0; i < columns.size(); i++) {
            if (name.equals(columns.get(i).get("name"))) {
                return row.get(i);
            }
        }
        throw new AssertionError("no column [" + name + "] in " + columns);
    }

    static Map<String, Object> getIndexSettingsWithDefaults(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        request.addParameter("include_defaults", "true");
        Response response = client().performRequest(request);
        try (InputStream is = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(
                XContentType.fromMediaType(response.getEntity().getContentType().getValue()).xContent(),
                is,
                true
            );
        }
    }

    static void createTemplate(String dataStreamName, String id, String template) throws IOException {
        final String INDEX_TEMPLATE = """
            {
                "index_patterns": ["$DATASTREAM"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putIndexTemplateRequest = new Request("POST", "/_index_template/" + id);
        putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", template).replace("$DATASTREAM", dataStreamName));
        assertOK(client().performRequest(putIndexTemplateRequest));
    }

    @SuppressWarnings("unchecked")
    static String getWriteBackingIndex(final RestClient client, final String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        return backingIndices.get(backingIndex).get("index_name");
    }
}
