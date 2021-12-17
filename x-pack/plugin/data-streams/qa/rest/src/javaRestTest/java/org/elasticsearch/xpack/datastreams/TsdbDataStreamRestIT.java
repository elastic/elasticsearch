/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.time.Instant;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class TsdbDataStreamRestIT extends ESRestTestCase {

    private static final String TEMPLATE = """
        {
            "index_patterns": ["k8s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 2,
                        "mode": "time_series",
                        "routing_path": ["metricset", "time_series_dimension"]
                    }
                },
                "mappings":{
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
            },
            "data_stream": {}
        }""";

    public void testTsdbDataStreams() throws Exception {
        // Create a template
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        Instant now = Instant.now();
        String nowAsString = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(now);
        var bulkRequest = new Request("POST", "/k8s/_bulk");
        bulkRequest.setJsonEntity(
            """
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "ip": "10.10.55.1", "network": {"tx": 2001818691, "rx": 802133794}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "ip": "10.10.55.1", "network": {"tx": 2005177954, "rx": 801479970}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "ip": "10.10.55.1", "network": {"tx": 2006223737, "rx": 802337279}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "cat", "uid":"947e4ced-1786-4e53-9e0c-5c447e959507", "ip": "10.10.55.2", "network": {"tx": 2012916202, "rx": 803685721}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9", "ip": "10.10.55.3", "network": {"tx": 1434521831, "rx": 530575198}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9", "ip": "10.10.55.3", "network": {"tx": 1434577921, "rx": 530600088}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9", "ip": "10.10.55.3", "network": {"tx": 1434587694, "rx": 530604797}}}}
                {"create": {}}
                {"@timestamp": "$now", "metricset": "pod", "k8s": {"pod": {"name": "dog", "uid":"df3145b3-0563-4d3b-a0f7-897eb2876ea9", "ip": "10.10.55.3", "network": {"tx": 1434595272, "rx": 530605511}}}}
                """
                .replace("$now", nowAsString)
        );
        bulkRequest.addParameter("refresh", "true");
        assertOK(client().performRequest(bulkRequest));

        var getDataStreamsRequest = new Request("GET", "/_data_stream");
        var response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        var dataStreams = entityAsMap(response);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo("1"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        String backingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        assertThat(backingIndex, backingIndexEqualTo("k8s", 1));

        var getIndexRequest = new Request("GET", "/" + backingIndex + "?human");
        response = client().performRequest(getIndexRequest);
        assertOK(response);
        var indices = entityAsMap(response);
        var escapedBackingIndex = backingIndex.replace(".", "\\.");
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), equalTo("time_series"));
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time"), notNullValue());
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time"), notNullValue());
    }

    public void testSimulateTsdbDataStreamTemplate() throws Exception {
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        var simulateIndexTemplateRequest = new Request("POST", "/_index_template/_simulate_index/k8s");
        var response = client().performRequest(simulateIndexTemplateRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index"), aMapWithSize(4));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.number_of_shards"), equalTo("2"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.number_of_replicas"), equalTo("0"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.mode"), equalTo("time_series"));
        assertThat(
            ObjectPath.evaluate(responseBody, "template.settings.index.routing_path"),
            contains("metricset", "time_series_dimension")
        );
        assertThat(ObjectPath.evaluate(responseBody, "overlapping"), empty());
    }

}
