/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TsdbIT extends AbstractRollingUpgradeWithSecurityTestCase {

    public TsdbIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    static final String TEMPLATE = """
        {
            "settings":{
                "index": {
                    "mode": "time_series"
                }
            },
            "mappings":{
                "dynamic_templates": [
                    {
                        "labels": {
                            "path_match": "pod.labels.*",
                            "mapping": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                ],
                "properties": {
                    "@timestamp" : {
                        "type": "date"
                    },
                    "metricset": {
                        "type": "keyword",
                        "time_series_dimension": true
                    },
                    "k8s": {
                        "properties": {
                            "pod": {
                                "properties": {
                                    "uid": {
                                        "type": "keyword",
                                        "time_series_dimension": true
                                    },
                                    "name": {
                                        "type": "keyword"
                                    },
                                    "ip": {
                                        "type": "ip"
                                    },
                                    "network": {
                                        "properties": {
                                            "tx": {
                                                "type": "long"
                                            },
                                            "rx": {
                                                "type": "long"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """;
    private static final String BULK =
        """
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "ip": "10.10.55.1", "network": {"tx": 2001818691, "rx": 802133794}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "hamster", "uid":"947e4ced-1786-4e53-9e0c-5c447e959508", "ip": "10.10.55.1", "network": {"tx": 2005177954, "rx": 801479970}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cow", "uid":"947e4ced-1786-4e53-9e0c-5c447e959509", "ip": "10.10.55.1", "network": {"tx": 2006223737, "rx": 802337279}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "rat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959510", "ip": "10.10.55.2", "network": {"tx": 2012916202, "rx": 803685721}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9", "ip": "10.10.55.3", "network": {"tx": 1434521831, "rx": 530575198}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "tiger", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea10", "ip": "10.10.55.3", "network": {"tx": 1434577921, "rx": 530600088}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "lion", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876e11", "ip": "10.10.55.3", "network": {"tx": 1434587694, "rx": 530604797}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "elephant", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876eb4", "ip": "10.10.55.3", "network": {"tx": 1434595272, "rx": 530605511}}}}
            """;

    private static final String DOC = """
        {
            "@timestamp": "$time",
            "metricset": "pod",
            "k8s": {
                "pod": {
                    "name": "dog",
                    "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9",
                    "ip": "10.10.55.3",
                    "network": {
                        "tx": 1434595272,
                        "rx": 530605511
                    }
                }
            }
        }
        """;

    public void testTsdbDataStream() throws Exception {
        String dataStreamName = "k8s";
        if (isOldCluster()) {
            final String INDEX_TEMPLATE = """
                {
                    "index_patterns": ["$PATTERN"],
                    "template": $TEMPLATE,
                    "data_stream": {
                    }
                }""";
            // Add composable index template
            String templateName = "1";
            var putIndexTemplateRequest = new Request("POST", "/_index_template/" + templateName);
            putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", TEMPLATE).replace("$PATTERN", dataStreamName));
            assertOK(client().performRequest(putIndexTemplateRequest));

            performOldClustertOperations(templateName, dataStreamName);
        } else if (isMixedCluster()) {
            performMixedClusterOperations(dataStreamName);
        } else if (isUpgradedCluster()) {
            performUpgradedClusterOperations(dataStreamName);
        }
    }

    private void performUpgradedClusterOperations(String dataStreamName) throws Exception {
        ensureGreen(dataStreamName);
        var rolloverRequest = new Request("POST", "/" + dataStreamName + "/_rollover");
        assertOK(client().performRequest(rolloverRequest));

        var dataStreams = getDataStream(dataStreamName);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(2));
        String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        String secondBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.1.index_name");
        assertThat(secondBackingIndex, backingIndexEqualTo(dataStreamName, 2));
        indexDoc(dataStreamName);
        assertSearch(dataStreamName, 10);
        closeIndex(firstBackingIndex);
        closeIndex(secondBackingIndex);
        openIndex(firstBackingIndex);
        openIndex(secondBackingIndex);
        assertBusy(() -> {
            try {
                assertSearch(dataStreamName, 10);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    private static void performMixedClusterOperations(String dataStreamName) throws IOException {
        ensureHealth(dataStreamName, request -> request.addParameter("wait_for_status", "yellow"));
        if (isFirstMixedCluster()) {
            indexDoc(dataStreamName);
        }
        assertSearch(dataStreamName, 9);
    }

    private static void performOldClustertOperations(String templateName, String dataStreamName) throws IOException {
        var bulkRequest = new Request("POST", "/" + dataStreamName + "/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstant(Instant.now())));
        bulkRequest.addParameter("refresh", "true");
        var response = client().performRequest(bulkRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));

        var dataStreams = getDataStream(dataStreamName);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo(dataStreamName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo(templateName));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        assertThat(firstBackingIndex, backingIndexEqualTo(dataStreamName, 1));
        assertSearch(dataStreamName, 8);
    }

    private static void indexDoc(String dataStreamName) throws IOException {
        var indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
        indexRequest.addParameter("refresh", "true");
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(Instant.now())));
        var response = client().performRequest(indexRequest);
        assertOK(response);
    }

    private static void assertSearch(String dataStreamName, int expectedHitCount) throws IOException {
        var searchRequest = new Request("GET", dataStreamName + "/_search");
        var response = client().performRequest(searchRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), equalTo(expectedHitCount));
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static Map<String, Object> getDataStream(String dataStreamName) throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream/" + dataStreamName);
        var response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        return entityAsMap(response);
    }

    private static Map<?, ?> getIndex(String indexName) throws IOException {
        var getIndexRequest = new Request("GET", "/" + indexName + "?human");
        var response = client().performRequest(getIndexRequest);
        assertOK(response);
        return entityAsMap(response);
    }

}
