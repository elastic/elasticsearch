/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
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

public class TsdbIT extends AbstractRollingTestCase {

    private static final String TEMPLATE = """
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
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507","ip": "10.10.55.1", "network": {"tx": 2001818691, "rx": 802133794}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "hamster", "uid":"947e4ced-1786-4e53-9e0c-5c447e959508","ip": "10.10.55.1", "network": {"tx": 2005177954, "rx": 801479970}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cow", "uid":"947e4ced-1786-4e53-9e0c-5c447e959509","ip": "10.10.55.1", "network": {"tx": 2006223737, "rx": 802337279}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "rat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959510","ip": "10.10.55.2", "network": {"tx": 2012916202, "rx": 803685721}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9","ip": "10.10.55.3", "network": {"tx": 1434521831, "rx": 530575198}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "tiger", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea10","ip": "10.10.55.3", "network": {"tx": 1434577921, "rx": 530600088}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "lion", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876e11","ip": "10.10.55.3", "network": {"tx": 1434587694, "rx": 530604797}}}}
            {"create": {}}
            {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "elephant", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876eb4","ip": "10.10.55.3", "network": {"tx": 1434595272, "rx": 530605511}}}}
            """;

    public void testTsdbDataStream() throws Exception {
        assumeTrue("TSDB was GA-ed in 8.7.0", UPGRADE_FROM_VERSION.onOrAfter(Version.V_8_7_0));
        if (CLUSTER_TYPE == ClusterType.OLD) {
            final String INDEX_TEMPLATE = """
                {
                    "index_patterns": ["k8s*"],
                    "template": $TEMPLATE,
                    "data_stream": {
                    }
                }""";
            // Add composable index template
            var putIndexTemplateRequest = new Request("POST", "/_index_template/1");
            putIndexTemplateRequest.setJsonEntity(INDEX_TEMPLATE.replace("$TEMPLATE", TEMPLATE));
            assertOK(client().performRequest(putIndexTemplateRequest));

            var bulkRequest = new Request("POST", "/k8s/_bulk");
            bulkRequest.setJsonEntity(BULK.replace("$now", formatInstant(Instant.now())));
            bulkRequest.addParameter("refresh", "true");
            var response = client().performRequest(bulkRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));

            var dataStreams = getDataStream();
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(1));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo("1"));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
            String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
            assertThat(firstBackingIndex, backingIndexEqualTo("k8s", 1));
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            var searchRequest = new Request("GET", "k8s/_search");
            var response = client().performRequest(searchRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), equalTo(10));
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            var rolloverRequest = new Request("POST", "/k8s/_rollover");
            assertOK(client().performRequest(rolloverRequest));

            var dataStreams = getDataStream();
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
            assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(2));
            String secondBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.1.index_name");
            assertThat(secondBackingIndex, backingIndexEqualTo("k8s", 2));

            var searchRequest = new Request("GET", "k8s/_search");
            var response = client().performRequest(searchRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), equalTo(10));
        }
    }

    private static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    private static Map<String, Object> getDataStream() throws IOException {
        var getDataStreamsRequest = new Request("GET", "/_data_stream");
        var response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        return entityAsMap(response);
    }

}
