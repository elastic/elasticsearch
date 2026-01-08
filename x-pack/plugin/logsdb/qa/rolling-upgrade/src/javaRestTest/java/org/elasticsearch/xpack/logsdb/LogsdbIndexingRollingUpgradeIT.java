/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class LogsdbIndexingRollingUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {

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
            bulkIndex(dataStreamName, 4, 1024, time, LogsdbIndexingRollingUpgradeIT::docSupplier);

            String firstBackingIndex = getDataStreamBackingIndexNames(dataStreamName).getFirst();
            var settings = (Map<?, ?>) getIndexSettings(firstBackingIndex, true).get(firstBackingIndex);
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
            bulkIndex(dataStreamName, 4, 1024, time, LogsdbIndexingRollingUpgradeIT::docSupplier);
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

    static void assertDataStream(String dataStreamName, String templateId) throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream/" + dataStreamName);
        var getDataStreamResponse = client().performRequest(getDataStreamsRequest);
        assertOK(getDataStreamResponse);
        var dataStreams = entityAsMap(getDataStreamResponse);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo(templateId));
    }

    static String docSupplier(Instant startTime, int j) {
        String hostName = "host" + j % 50; // Not realistic, but makes asserting search / query response easier.
        String methodName = "method" + j % 5;
        String ip = NetworkAddress.format(randomIp(true));
        String message = randomAlphaOfLength(128);
        long length = randomLong();
        double factor = randomDouble();
        return BULK_ITEM_TEMPLATE.replace("$now", formatInstant(startTime))
            .replace("$host", hostName)
            .replace("$method", methodName)
            .replace("$ip", ip)
            .replace("$message", message)
            .replace("$length", Long.toString(length))
            .replace("$factor", Double.toString(factor));
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

    static void maybeEnableLogsdbByDefault() throws IOException {
        if (System.getProperty("tests.bwc.tag") != null) {
            return;
        }

        var version = System.getProperty("tests.old_cluster_version") != null
            ? Version.fromString(System.getProperty("tests.old_cluster_version"))
            : Version.CURRENT;
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
