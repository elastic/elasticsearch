/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class LogsdbIndexingRollingUpgradeIT extends ESRestTestCase {

    static String BULK_ITEM_TEMPLATE =
        """
            {"@timestamp": "$now", "host.name": "$host", "method": "$method", "ip": "$ip", "message": "$message", "length": $length, "factor": $factor}
            """;

    private static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "method": {
                  "type": "keyword"
                },
                "message": {
                  "type": "text"
                },
                "ip": {
                  "type": "ip"
                },
                "length": {
                  "type": "long"
                },
                "factor": {
                  "type": "double"
                }
              }
            }
        }""";

    private static final String USER = "admin-user";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionCluster(USER, PASS);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testIndexing() throws Exception {
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            logger.info("system_property: {} / {}", entry.getKey(), entry.getValue());
        }

        String dataStreamName = "logs-bwc-test";
        Instant time;
        {
            maybeEnableLogsdbByDefault();

            String templateId = getClass().getSimpleName().toLowerCase(Locale.ROOT);
            createTemplate(dataStreamName, templateId, TEMPLATE);

            time = Instant.now().minusSeconds(60 * 60);
            bulkIndex(dataStreamName, 4, 1024, time);

            String firstBackingIndex = getWriteBackingIndex(client(), dataStreamName, 0);
            var settings = (Map<?, ?>) getIndexSettingsWithDefaults(firstBackingIndex).get(firstBackingIndex);
            assertThat(((Map<?, ?>) settings.get("settings")).get("index.mode"), equalTo("logsdb"));
            assertThat(((Map<?, ?>) settings.get("defaults")).get("index.mapping.source.mode"), equalTo("SYNTHETIC"));

            // check prior to rollover
            assertDataStream(dataStreamName, templateId);
            ensureGreen(dataStreamName);
            search(dataStreamName);
            query(dataStreamName);
        }
        int numNodes = Integer.parseInt(System.getProperty("tests.num_nodes", "3"));
        for (int i = 0; i < numNodes; i++) {
            upgradeNode(i);
            time = time.plusNanos(60 * 30);
            bulkIndex(dataStreamName, 4, 1024, time);
            search(dataStreamName);
            query(dataStreamName);
        }
        {
            var forceMergeRequest = new Request("POST", "/" + dataStreamName + "/_forcemerge");
            forceMergeRequest.addParameter("max_num_segments", "1");
            assertOK(client().performRequest(forceMergeRequest));

            ensureGreen(dataStreamName);
            search(dataStreamName);
            query(dataStreamName);
        }
    }

    private void upgradeNode(int n) throws IOException {
        closeClients();
        var upgradeVersion = System.getProperty("tests.new_cluster_version") != null
            ? Version.fromString(System.getProperty("tests.new_cluster_version"))
            : Version.CURRENT;
        logger.info("Upgrading node {} to version {}", n, upgradeVersion);
        cluster.upgradeNodeToVersion(n, upgradeVersion);
        initClient();
    }

    static void assertDataStream(String dataStreamName, String templateId) throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream/" + dataStreamName);
        var getDataStreamResponse = client().performRequest(getDataStreamsRequest);
        assertOK(getDataStreamResponse);
        var dataStreams = entityAsMap(getDataStreamResponse);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo(templateId));
    }

    static void createTemplate(String dataStreamName, String id, String template) throws IOException {
        final String INDEX_TEMPLATE = """
            {
                "priority": 200,
                "index_patterns": ["$DATASTREAM"],
                "template": $TEMPLATE,
                "data_stream": {
                }
            }""";
        var putIndexTemplateRequest = new Request("POST", "/_index_template/" + id);
        putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", template).replace("$DATASTREAM", dataStreamName));
        assertOK(client().performRequest(putIndexTemplateRequest));
    }

    static String bulkIndex(String dataStreamName, int numRequest, int numDocs, Instant startTime) throws Exception {
        String firstIndex = null;
        for (int i = 0; i < numRequest; i++) {
            var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
            StringBuilder requestBody = new StringBuilder();
            for (int j = 0; j < numDocs; j++) {
                String hostName = "host" + j % 50; // Not realistic, but makes asserting search / query response easier.
                String methodName = "method" + j % 5;
                String ip = NetworkAddress.format(randomIp(true));
                String message = randomAlphaOfLength(128);
                long length = randomLong();
                double factor = randomDouble();

                requestBody.append("{\"create\": {}}");
                requestBody.append('\n');
                requestBody.append(
                    BULK_ITEM_TEMPLATE.replace("$now", formatInstant(startTime))
                        .replace("$host", hostName)
                        .replace("$method", methodName)
                        .replace("$ip", ip)
                        .replace("$message", message)
                        .replace("$length", Long.toString(length))
                        .replace("$factor", Double.toString(factor))
                );
                requestBody.append('\n');

                startTime = startTime.plusMillis(1);
            }
            bulkRequest.setJsonEntity(requestBody.toString());
            bulkRequest.addParameter("refresh", "true");
            var response = client().performRequest(bulkRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));
            if (firstIndex == null) {
                firstIndex = (String) ((Map<?, ?>) ((Map<?, ?>) ((List<?>) responseBody.get("items")).get(0)).get("create")).get("_index");
            }
        }
        return firstIndex;
    }

    void search(String dataStreamName) throws Exception {
        var searchRequest = new Request("POST", "/" + dataStreamName + "/_search");
        searchRequest.addParameter("pretty", "true");
        searchRequest.setJsonEntity("""
            {
                "size": 0,
                "aggs": {
                    "host_name": {
                        "terms": {
                            "field": "host.name",
                            "order": { "_key": "asc" }
                        },
                        "aggs": {
                            "max_length": {
                                "max": {
                                    "field": "length"
                                }
                            },
                            "max_factor": {
                                "max": {
                                    "field": "factor"
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
        String key = ObjectPath.evaluate(responseBody, "aggregations.host_name.buckets.0.key");
        assertThat(key, equalTo("host0"));
        Integer docCount = ObjectPath.evaluate(responseBody, "aggregations.host_name.buckets.0.doc_count");
        assertThat(docCount, greaterThan(0));
        Double maxTx = ObjectPath.evaluate(responseBody, "aggregations.host_name.buckets.0.max_length.value");
        assertThat(maxTx, notNullValue());
        Double maxRx = ObjectPath.evaluate(responseBody, "aggregations.host_name.buckets.0.max_factor.value");
        assertThat(maxRx, notNullValue());
    }

    void query(String dataStreamName) throws Exception {
        var queryRequest = new Request("POST", "/_query");
        queryRequest.addParameter("pretty", "true");
        queryRequest.setJsonEntity("""
            {
                "query": "FROM $ds | STATS max(length), max(factor) BY host.name | SORT host.name | LIMIT 5"
            }
            """.replace("$ds", dataStreamName));
        var response = client().performRequest(queryRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);

        String column1 = ObjectPath.evaluate(responseBody, "columns.0.name");
        String column2 = ObjectPath.evaluate(responseBody, "columns.1.name");
        String column3 = ObjectPath.evaluate(responseBody, "columns.2.name");
        assertThat(column1, equalTo("max(length)"));
        assertThat(column2, equalTo("max(factor)"));
        assertThat(column3, equalTo("host.name"));

        String key = ObjectPath.evaluate(responseBody, "values.0.2");
        assertThat(key, equalTo("host0"));
        Long maxRx = ObjectPath.evaluate(responseBody, "values.0.0");
        assertThat(maxRx, notNullValue());
        Double maxTx = ObjectPath.evaluate(responseBody, "values.0.1");
        assertThat(maxTx, notNullValue());
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

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    static String getWriteBackingIndex(final RestClient client, final String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<?> dataStreams = (List<?>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<?, ?> dataStream = (Map<?, ?>) dataStreams.getFirst();
        final List<?> backingIndices = (List<?>) dataStream.get("indices");
        return (String) ((Map<?, ?>) backingIndices.get(backingIndex)).get("index_name");
    }

    static void maybeEnableLogsdbByDefault() throws IOException {
        if (System.getProperty("tests.bwc.tag") != null) {
            return;
        }

        var version = Version.fromString(System.getProperty("tests.old_cluster_version"));
        if (version.onOrAfter(Version.fromString("9.0.0"))) {
            return;
        }

        var request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            {
                "persistent": {
                    "cluster.logsdb.enabled": true
                }
            }
            """);
        assertOK(client().performRequest(request));
    }

}
