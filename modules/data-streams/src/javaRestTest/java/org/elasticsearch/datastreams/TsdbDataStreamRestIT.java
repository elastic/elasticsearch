/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TsdbDataStreamRestIT extends ESRestTestCase {

    private static final String TEMPLATE = """
        {
            "index_patterns": ["k8s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 2,
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
            "data_stream": {
                "index_mode": "time_series"
            }
        }""";

    private static final String NON_TSDB_TEMPLATE = """
        {
            "index_patterns": ["k8s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 2
                    }
                },
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "metricset": {
                            "type": "keyword"
                        },
                        "k8s": {
                            "properties": {
                                "pod": {
                                    "properties": {
                                        "uid": {
                                            "type": "keyword"
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

    private static final String BULK =
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
            """;

    public void testTsdbDataStreams() throws Exception {
        // Create a template
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        var bulkRequest = new Request("POST", "/k8s/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstant(Instant.now())));
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
        String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        assertThat(firstBackingIndex, backingIndexEqualTo("k8s", 1));

        var indices = getIndex(firstBackingIndex);
        var escapedBackingIndex = firstBackingIndex.replace(".", "\\.");
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), equalTo("time_series"));
        String startTimeFirstBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time");
        assertThat(startTimeFirstBackingIndex, notNullValue());
        String endTimeFirstBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time");
        assertThat(endTimeFirstBackingIndex, notNullValue());

        var rolloverRequest = new Request("POST", "/k8s/_rollover");
        assertOK(client().performRequest(rolloverRequest));

        response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        dataStreams = entityAsMap(response);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(2));
        String secondBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.1.index_name");
        assertThat(secondBackingIndex, backingIndexEqualTo("k8s", 2));

        indices = getIndex(secondBackingIndex);
        escapedBackingIndex = secondBackingIndex.replace(".", "\\.");
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
        String startTimeSecondBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time");
        assertThat(startTimeSecondBackingIndex, equalTo(endTimeFirstBackingIndex));
        String endTimeSecondBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time");
        assertThat(endTimeSecondBackingIndex, notNullValue());

        var indexRequest = new Request("POST", "/k8s/_doc");
        Instant time = parseInstant(startTimeFirstBackingIndex);
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(time)));
        response = client().performRequest(indexRequest);
        assertOK(response);
        assertThat(entityAsMap(response).get("_index"), equalTo(firstBackingIndex));

        indexRequest = new Request("POST", "/k8s/_doc");
        time = parseInstant(endTimeSecondBackingIndex).minusMillis(1);
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(time)));
        response = client().performRequest(indexRequest);
        assertOK(response);
        assertThat(entityAsMap(response).get("_index"), equalTo(secondBackingIndex));
    }

    public void testTsdbDataStreamsNanos() throws Exception {
        // Create a template
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE.replace("date", "date_nanos"));
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        var bulkRequest = new Request("POST", "/k8s/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstantNanos(Instant.now())));
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
        String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        assertThat(firstBackingIndex, backingIndexEqualTo("k8s", 1));

        var indices = getIndex(firstBackingIndex);
        var escapedBackingIndex = firstBackingIndex.replace(".", "\\.");
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), equalTo("time_series"));
        String startTimeFirstBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time");
        assertThat(startTimeFirstBackingIndex, notNullValue());
        String endTimeFirstBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time");
        assertThat(endTimeFirstBackingIndex, notNullValue());

        var rolloverRequest = new Request("POST", "/k8s/_rollover");
        assertOK(client().performRequest(rolloverRequest));

        response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        dataStreams = entityAsMap(response);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(2));
        String secondBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.1.index_name");
        assertThat(secondBackingIndex, backingIndexEqualTo("k8s", 2));

        indices = getIndex(secondBackingIndex);
        escapedBackingIndex = secondBackingIndex.replace(".", "\\.");
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
        String startTimeSecondBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time");
        assertThat(startTimeSecondBackingIndex, equalTo(endTimeFirstBackingIndex));
        String endTimeSecondBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time");
        assertThat(endTimeSecondBackingIndex, notNullValue());

        var indexRequest = new Request("POST", "/k8s/_doc");
        Instant time = parseInstant(startTimeFirstBackingIndex);
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstantNanos(time)));
        response = client().performRequest(indexRequest);
        assertOK(response);
        assertThat(entityAsMap(response).get("_index"), equalTo(firstBackingIndex));

        indexRequest = new Request("POST", "/k8s/_doc");
        time = parseInstant(endTimeSecondBackingIndex).minusMillis(1);
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstantNanos(time)));
        response = client().performRequest(indexRequest);
        assertOK(response);
        assertThat(entityAsMap(response).get("_index"), equalTo(secondBackingIndex));
    }

    public void testSimulateTsdbDataStreamTemplate() throws Exception {
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        var simulateIndexTemplateRequest = new Request("POST", "/_index_template/_simulate_index/k8s");
        var response = client().performRequest(simulateIndexTemplateRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index"), aMapWithSize(6));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.number_of_shards"), equalTo("2"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.number_of_replicas"), equalTo("0"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.mode"), equalTo("time_series"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.time_series.start_time"), notNullValue());
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.time_series.end_time"), notNullValue());
        assertThat(
            ObjectPath.evaluate(responseBody, "template.settings.index.routing_path"),
            contains("metricset", "time_series_dimension")
        );
        assertThat(ObjectPath.evaluate(responseBody, "overlapping"), empty());
    }

    public void testSubsequentRollovers() throws Exception {
        // Create a template
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        var createDataStreamRequest = new Request("PUT", "/_data_stream/k8s");
        assertOK(client().performRequest(createDataStreamRequest));

        int numRollovers = 16;
        for (int i = 0; i < numRollovers; i++) {
            var rolloverRequest = new Request("POST", "/k8s/_rollover?pretty");
            var rolloverResponse = client().performRequest(rolloverRequest);
            assertOK(rolloverResponse);
            var rolloverResponseBody = entityAsMap(rolloverResponse);
            assertThat(rolloverResponseBody.get("rolled_over"), is(true));

            var oldIndex = getIndex((String) rolloverResponseBody.get("old_index"));
            var newIndex = getIndex((String) rolloverResponseBody.get("new_index"));
            assertThat(getEndTime(oldIndex), equalTo(getStartTime(newIndex)));
            assertThat(getStartTime(oldIndex).isBefore(getEndTime(oldIndex)), is(true));
            assertThat(getEndTime(newIndex).isAfter(getStartTime(newIndex)), is(true));
        }
    }

    public void testMigrateRegularDataStreamToTsdbDataStream() throws Exception {
        // Create a non tsdb template
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(NON_TSDB_TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        // Index a few docs and sometimes rollover
        int numRollovers = 4;
        int numDocs = 32;
        var currentTime = Instant.now();
        var currentMinus30Days = currentTime.minus(30, ChronoUnit.DAYS);
        Set<Instant> times = new HashSet<>();
        for (int i = 0; i < numRollovers; i++) {
            for (int j = 0; j < numDocs; j++) {
                var indexRequest = new Request("POST", "/k8s/_doc");
                var time = randomValueOtherThanMany(
                    times::contains,
                    () -> Instant.ofEpochMilli(randomLongBetween(currentMinus30Days.toEpochMilli(), currentTime.toEpochMilli()))
                );
                times.add(time);
                indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(time)));
                var response = client().performRequest(indexRequest);
                assertOK(response);
                var responseBody = entityAsMap(response);
                // i rollovers and +1 offset:
                assertThat((String) responseBody.get("_index"), backingIndexEqualTo("k8s", i + 1));
            }
            var rolloverRequest = new Request("POST", "/k8s/_rollover");
            var rolloverResponse = client().performRequest(rolloverRequest);
            assertOK(rolloverResponse);
            var rolloverResponseBody = entityAsMap(rolloverResponse);
            assertThat(rolloverResponseBody.get("rolled_over"), is(true));
        }

        var getDataStreamsRequest = new Request("GET", "/_data_stream");
        var getDataStreamResponse = client().performRequest(getDataStreamsRequest);
        assertOK(getDataStreamResponse);
        var dataStreams = entityAsMap(getDataStreamResponse);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(5));
        for (int i = 0; i < 5; i++) {
            String backingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices." + i + ".index_name");
            assertThat(backingIndex, backingIndexEqualTo("k8s", i + 1));
            var indices = getIndex(backingIndex);
            var escapedBackingIndex = backingIndex.replace(".", "\\.");
            assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
            assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), nullValue());
            assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time"), nullValue());
            assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time"), nullValue());
        }

        // Update template
        putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        var rolloverRequest = new Request("POST", "/k8s/_rollover");
        var rolloverResponse = client().performRequest(rolloverRequest);
        assertOK(rolloverResponse);
        var rolloverResponseBody = entityAsMap(rolloverResponse);
        assertThat(rolloverResponseBody.get("rolled_over"), is(true));
        var newIndex = (String) rolloverResponseBody.get("new_index");
        assertThat(newIndex, backingIndexEqualTo("k8s", 6));

        // Ingest documents that will land in the new tsdb backing index:
        var t = currentTime;
        for (int i = 0; i < numDocs; i++) {
            var indexRequest = new Request("POST", "/k8s/_doc");
            indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(t)));
            var response = client().performRequest(indexRequest);
            assertOK(response);
            var responseBody = entityAsMap(response);
            assertThat((String) responseBody.get("_index"), backingIndexEqualTo("k8s", 6));
            t = t.plusMillis(1000);
        }

        // Fail if documents target older non tsdb backing index:
        var indexRequest = new Request("POST", "/k8s/_doc");
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(currentMinus30Days)));
        var e = expectThrows(ResponseException.class, () -> client().performRequest(indexRequest));
        assertThat(e.getMessage(), containsString("is outside of ranges of currently writable indices"));
    }

    public void testChangeTemplateIndexMode() throws Exception {
        // Create a template
        {
            var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
            putComposableIndexTemplateRequest.setJsonEntity(TEMPLATE);
            assertOK(client().performRequest(putComposableIndexTemplateRequest));
        }
        {
            var indexRequest = new Request("POST", "/k8s/_doc");
            var time = Instant.now();
            indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(time)));
            var response = client().performRequest(indexRequest);
            assertOK(response);
        }
        {
            var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
            putComposableIndexTemplateRequest.setJsonEntity(NON_TSDB_TEMPLATE);
            var e = expectThrows(ResponseException.class, () -> client().performRequest(putComposableIndexTemplateRequest));
            assertThat(
                e.getMessage(),
                containsString(
                    "composable template [1] with index patterns [k8s*], priority [null],"
                        + " index_mode [null] would cause tsdb data streams [k8s] to no longer match a data stream template"
                        + " with a time_series index_mode"
                )
            );
        }
    }

    private static Map<?, ?> getIndex(String indexName) throws IOException {
        var getIndexRequest = new Request("GET", "/" + indexName + "?human");
        var response = client().performRequest(getIndexRequest);
        assertOK(response);
        return entityAsMap(response);
    }

    private static Instant getStartTime(Map<?, ?> getIndexResponse) throws IOException {
        assert getIndexResponse.keySet().size() == 1;
        String topLevelKey = (String) getIndexResponse.keySet().iterator().next();
        String val = ObjectPath.evaluate(getIndexResponse.get(topLevelKey), "settings.index.time_series.start_time");
        return Instant.from(DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).parse(val));
    }

    private static Instant getEndTime(Map<?, ?> getIndexResponse) throws IOException {
        assert getIndexResponse.keySet().size() == 1;
        String topLevelKey = (String) getIndexResponse.keySet().iterator().next();
        String val = ObjectPath.evaluate(getIndexResponse.get(topLevelKey), "settings.index.time_series.end_time");
        return Instant.from(DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).parse(val));
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    static String formatInstantNanos(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME_NANOS.getName()).format(instant);
    }

    static Instant parseInstant(String input) {
        return Instant.from(DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).parse(input));
    }

}
