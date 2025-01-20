/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class TsdbDataStreamRestIT extends DisabledSecurityDataStreamTestCase {

    private static final String COMPONENT_TEMPLATE = """
        {
            "template": {
                "settings": {}
            }
        }
        """;

    private static final String TEMPLATE = """
        {
            "index_patterns": ["k8s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 1,
                        "number_of_shards": 2,
                        "mode": "time_series"
                        SOURCEMODE
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
            },
            "composed_of": ["custom_template"],
            "data_stream": {
            }
        }""";

    private static final String NON_TSDB_TEMPLATE = """
        {
            "index_patterns": ["k8s*"],
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 1,
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

    private static String getTemplate() {
        return TEMPLATE.replace("SOURCEMODE", randomFrom("", """
            , "mapping": { "source": { "mode": "stored" } }""", """
            , "mapping": { "source": { "mode": "synthetic" } }"""));
    }

    private static boolean trialStarted = false;

    @Before
    public void setup() throws IOException {
        if (trialStarted == false) {
            // Start trial to support synthetic source.
            Request startTrial = new Request("POST", "/_license/start_trial");
            startTrial.addParameter("acknowledge", "true");
            try {
                client().performRequest(startTrial);
            } catch (Exception e) {
                // Ignore failures, the API is not present in Serverless.
            }
            trialStarted = true;
        }

        // Add component template:
        var request = new Request("POST", "/_component_template/custom_template");
        request.setJsonEntity(COMPONENT_TEMPLATE);
        assertOK(client().performRequest(request));
        // Add composable index template
        request = new Request("POST", "/_index_template/1");
        request.setJsonEntity(getTemplate());
        assertOK(client().performRequest(request));
    }

    public void testTsdbDataStreams() throws Exception {
        assertTsdbDataStream();
    }

    public void testTsdbDataStreamsNanos() throws Exception {
        // Overwrite template to use date_nanos field type:
        var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
        putComposableIndexTemplateRequest.setJsonEntity(getTemplate().replace("date", "date_nanos"));
        assertOK(client().performRequest(putComposableIndexTemplateRequest));

        assertTsdbDataStream();
    }

    public void testTsbdDataStreamComponentTemplateWithAllSettingsAndMappings() throws Exception {
        // Different component and index template. All settings and mapping are in component template.
        final String COMPONENT_TEMPLATE_WITH_SETTINGS_AND_MAPPINGS = """
            {
                "template": {
                    "settings":{
                        "index": {
                            "mode": "time_series",
                            "routing_path": ["metricset", "k8s.pod.uid"]
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
                }
            }
            """;
        final String DELEGATE_TEMPLATE = """
            {
                "index_patterns": ["k8s*"],
                "composed_of": ["custom_template"],
                "data_stream": {
                }
            }""";

        // Delete and add new the templates:
        var deleteRequest = new Request("DELETE", "/_index_template/1");
        assertOK(client().performRequest(deleteRequest));
        deleteRequest = new Request("DELETE", "/_component_template/custom_template");
        assertOK(client().performRequest(deleteRequest));
        var request = new Request("POST", "/_component_template/custom_template");
        request.setJsonEntity(COMPONENT_TEMPLATE_WITH_SETTINGS_AND_MAPPINGS);
        assertOK(client().performRequest(request));
        request = new Request("POST", "/_index_template/1");
        request.setJsonEntity(DELEGATE_TEMPLATE);
        assertOK(client().performRequest(request));

        // Ensure everything behaves the same, regardless of the fact that all settings and mappings are in component template:
        assertTsdbDataStream();
    }

    private void assertTsdbDataStream() throws IOException {
        var bulkRequest = new Request("POST", "/k8s/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstantNanos(Instant.now())));
        bulkRequest.addParameter("refresh", "true");
        var response = client().performRequest(bulkRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));

        var getDataStreamsRequest = new Request("GET", "/_data_stream");
        response = client().performRequest(getDataStreamsRequest);
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
        indexRequest.addParameter("refresh", "true");
        Instant time = parseInstant(startTimeFirstBackingIndex);
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstantNanos(time)));
        response = client().performRequest(indexRequest);
        assertOK(response);
        assertThat(entityAsMap(response).get("_index"), equalTo(firstBackingIndex));

        indexRequest = new Request("POST", "/k8s/_doc");
        indexRequest.addParameter("refresh", "true");
        time = parseInstant(endTimeSecondBackingIndex).minusMillis(1);
        indexRequest.setJsonEntity(DOC.replace("$time", formatInstantNanos(time)));
        response = client().performRequest(indexRequest);
        assertOK(response);
        assertThat(entityAsMap(response).get("_index"), equalTo(secondBackingIndex));

        var searchRequest = new Request("GET", "k8s/_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "range":{
                        "@timestamp":{
                            "gte": "now-7d",
                            "lte": "now+7d"
                        }
                    }
                },
                "sort": [
                    {
                        "@timestamp": {
                            "order": "desc"
                        }
                    }
                ]
            }
            """);
        response = client().performRequest(searchRequest);
        assertOK(response);
        responseBody = entityAsMap(response);
        try {
            assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), equalTo(10));
            assertThat(ObjectPath.evaluate(responseBody, "hits.total.relation"), equalTo("eq"));
            assertThat(ObjectPath.evaluate(responseBody, "hits.hits.0._index"), equalTo(secondBackingIndex));
            assertThat(ObjectPath.evaluate(responseBody, "hits.hits.1._index"), equalTo(firstBackingIndex));
        } catch (Exception | AssertionError e) {
            logger.error("search response body causing assertion error [" + responseBody + "]", e);
            throw e;
        }
    }

    public void testSimulateTsdbDataStreamTemplate() throws Exception {
        var simulateIndexTemplateRequest = new Request("POST", "/_index_template/_simulate_index/k8s");
        var response = client().performRequest(simulateIndexTemplateRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.number_of_shards"), equalTo("2"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.number_of_replicas"), equalTo("1"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.mode"), equalTo("time_series"));
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.time_series.start_time"), notNullValue());
        assertThat(ObjectPath.evaluate(responseBody, "template.settings.index.time_series.end_time"), notNullValue());
        assertThat(
            ObjectPath.evaluate(responseBody, "template.settings.index.routing_path"),
            containsInAnyOrder("metricset", "k8s.pod.uid", "pod.labels.*")
        );
        assertThat(ObjectPath.evaluate(responseBody, "overlapping"), empty());
    }

    public void testSubsequentRollovers() throws Exception {
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
        putComposableIndexTemplateRequest.setJsonEntity(getTemplate());
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

    public void testDowngradeTsdbDataStreamToRegularDataStream() throws Exception {
        var time = Instant.now();
        {
            var indexRequest = new Request("POST", "/k8s/_doc");
            indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(time)));
            var response = client().performRequest(indexRequest);
            assertOK(response);
        }
        {
            var putComposableIndexTemplateRequest = new Request("POST", "/_index_template/1");
            putComposableIndexTemplateRequest.setJsonEntity(NON_TSDB_TEMPLATE);
            client().performRequest(putComposableIndexTemplateRequest);
        }
        {
            {
                // check prior to rollover
                var getDataStreamsRequest = new Request("GET", "/_data_stream");
                var getDataStreamResponse = client().performRequest(getDataStreamsRequest);
                assertOK(getDataStreamResponse);
                var dataStreams = entityAsMap(getDataStreamResponse);
                assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
                assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
                assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.time_series"), notNullValue());
            }
            var rolloverRequest = new Request("POST", "/k8s/_rollover");
            var rolloverResponse = client().performRequest(rolloverRequest);
            assertOK(rolloverResponse);
            var rolloverResponseBody = entityAsMap(rolloverResponse);
            assertThat(rolloverResponseBody.get("rolled_over"), is(true));
            {
                // Data stream is no longer a tsdb data stream
                var getDataStreamsRequest = new Request("GET", "/_data_stream");
                var getDataStreamResponse = client().performRequest(getDataStreamsRequest);
                assertOK(getDataStreamResponse);
                var dataStreams = entityAsMap(getDataStreamResponse);
                assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
                assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(2));
                assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.time_series"), nullValue());
            }
            {
                // old index remains a tsdb index
                var oldIndex = (String) rolloverResponseBody.get("old_index");
                assertThat(oldIndex, backingIndexEqualTo("k8s", 1));
                var indices = getIndex(oldIndex);
                var escapedBackingIndex = oldIndex.replace(".", "\\.");
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), equalTo("time_series"));
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time"), notNullValue());
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time"), notNullValue());
            }
            {
                // new index is a regular index
                var newIndex = (String) rolloverResponseBody.get("new_index");
                assertThat(newIndex, backingIndexEqualTo("k8s", 2));
                var indices = getIndex(newIndex);
                var escapedBackingIndex = newIndex.replace(".", "\\.");
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("k8s"));
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), nullValue());
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time"), nullValue());
                assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time"), nullValue());
            }
        }
        {
            // All documents should be ingested into the most recent backing index:
            // (since the data stream is no longer a tsdb data stream)
            Instant[] timestamps = new Instant[] {
                time,
                time.plusSeconds(1),
                time.plusSeconds(5),
                time.minus(30, ChronoUnit.DAYS),
                time.plus(30, ChronoUnit.DAYS) };
            for (Instant timestamp : timestamps) {
                var indexRequest = new Request("POST", "/k8s/_doc");
                indexRequest.setJsonEntity(DOC.replace("$time", formatInstant(timestamp)));
                var response = client().performRequest(indexRequest);
                assertOK(response);
                var responseBody = entityAsMap(response);
                assertThat((String) responseBody.get("_index"), backingIndexEqualTo("k8s", 2));
            }
        }
    }

    public void testUpdateComponentTemplateDoesNotFailIndexTemplateValidation() throws IOException {
        var request = new Request("POST", "/_component_template/custom_template");
        request.setJsonEntity("""
            {
                "template": {
                    "settings": {
                        "index": {
                            "number_of_replicas": 1
                        }
                    }
                }
            }
            """);
        client().performRequest(request);
    }

    public void testLookBackTime() throws IOException {
        // Create template that uses index.look_back_time index setting:
        String template = """
            {
                "index_patterns": ["test*"],
                "template": {
                    "settings":{
                        "index": {
                            "look_back_time": "24h",
                            "number_of_replicas": 1,
                            "mode": "time_series"
                        }
                    },
                    "mappings":{
                        "properties": {
                            "@timestamp" : {
                                "type": "date"
                            },
                            "field": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                },
                "data_stream": {}
            }""";
        var putIndexTemplateRequest = new Request("PUT", "/_index_template/2");
        putIndexTemplateRequest.setJsonEntity(template);
        assertOK(client().performRequest(putIndexTemplateRequest));

        // Create data stream:
        var createDataStreamRequest = new Request("PUT", "/_data_stream/test123");
        assertOK(client().performRequest(createDataStreamRequest));

        // Check data stream has been created:
        var getDataStreamsRequest = new Request("GET", "/_data_stream");
        var response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        var dataStreams = entityAsMap(response);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("test123"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.generation"), equalTo(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.template"), equalTo("2"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        String firstBackingIndex = ObjectPath.evaluate(dataStreams, "data_streams.0.indices.0.index_name");
        assertThat(firstBackingIndex, backingIndexEqualTo("test123", 1));

        // Check the backing index:
        // 2023-08-15T04:35:50.000Z
        var indices = getIndex(firstBackingIndex);
        var escapedBackingIndex = firstBackingIndex.replace(".", "\\.");
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".data_stream"), equalTo("test123"));
        assertThat(ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.mode"), equalTo("time_series"));
        String startTimeFirstBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.start_time");
        assertThat(startTimeFirstBackingIndex, notNullValue());
        Instant now = Instant.now();
        Instant startTime = DateFormatters.from(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.parse(startTimeFirstBackingIndex)).toInstant();
        assertTrue(now.minus(24, ChronoUnit.HOURS).isAfter(startTime));
        String endTimeFirstBackingIndex = ObjectPath.evaluate(indices, escapedBackingIndex + ".settings.index.time_series.end_time");
        assertThat(endTimeFirstBackingIndex, notNullValue());
    }

    public void testReindexTsdbDataStream() throws Exception {
        var deleteRequest = new Request("DELETE", "/_index_template/1");
        assertOK(client().performRequest(deleteRequest));
        deleteRequest = new Request("DELETE", "/_component_template/custom_template");
        assertOK(client().performRequest(deleteRequest));

        final int SECONDS_PER_DAY = 24 * 60 * 60;
        final String CUSTOM_TEMPLATE_WITH_START_END_TIME = """
            {
                "template": {
                    "settings":{
                        "index": {
                            "number_of_replicas": 1,
                            "number_of_shards": 4,
                            "mode": "time_series",
                            "routing_path": ["metricset", "k8s.pod.uid"],
                            "time_series": {
                                "start_time": "$start",
                                "end_time": "$end"
                            }
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
                }
            }
            """;

        // Create a data stream that's one week old.
        var request = new Request("POST", "/_component_template/source_template");
        request.setJsonEntity(
            CUSTOM_TEMPLATE_WITH_START_END_TIME.replace("$start", formatInstantNanos(Instant.now().minusSeconds(8 * SECONDS_PER_DAY)))
                .replace("$end", formatInstantNanos(Instant.now().minusSeconds(6 * SECONDS_PER_DAY)))
        );
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_index_template/1");
        request.setJsonEntity("""
            {
                "index_patterns": ["k8s*"],
                "composed_of": ["source_template"],
                "data_stream": {
                }
            }""");
        assertOK(client().performRequest(request));

        // Add some docs to it.
        var bulkRequest = new Request("POST", "/k8s/_bulk");
        bulkRequest.setJsonEntity(BULK.replace("$now", formatInstantNanos(Instant.now().minusSeconds(7 * SECONDS_PER_DAY))));
        bulkRequest.addParameter("refresh", "true");
        var response = client().performRequest(bulkRequest);
        assertOK(response);
        var responseBody = entityAsMap(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), equalTo(false));

        // Clone the old data stream.
        request = new Request("POST", "/_component_template/destination_template");
        request.setJsonEntity(
            CUSTOM_TEMPLATE_WITH_START_END_TIME.replace("$start", formatInstantNanos(Instant.now().minusSeconds(8 * SECONDS_PER_DAY)))
                .replace("$end", formatInstantNanos(Instant.now().minusSeconds(6 * SECONDS_PER_DAY)))
        );
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_index_template/2");
        request.setJsonEntity("""
            {
                "index_patterns": ["k9s*"],
                "composed_of": ["destination_template"],
                "data_stream": {
                }
            }""");
        assertOK(client().performRequest(request));

        // Reindex.
        request = new Request("POST", "/_reindex");
        request.setJsonEntity("""
            {
                "source": {
                    "index": "k8s"
                  },
                  "dest": {
                    "index": "k9s",
                    "op_type": "create"
                  }
            }
            """);
        assertOK(client().performRequest(request));

        var getDataStreamsRequest = new Request("GET", "/_data_stream");
        response = client().performRequest(getDataStreamsRequest);
        assertOK(response);
        var dataStreams = entityAsMap(response);
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams"), hasSize(2));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.name"), equalTo("k8s"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.0.indices"), hasSize(1));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.1.name"), equalTo("k9s"));
        assertThat(ObjectPath.evaluate(dataStreams, "data_streams.1.indices"), hasSize(1));

        // Update the start and end time of the new data stream.
        request = new Request("POST", "/_component_template/destination_template");
        request.setJsonEntity(
            CUSTOM_TEMPLATE_WITH_START_END_TIME.replace("$start", formatInstantNanos(Instant.now().minusSeconds(SECONDS_PER_DAY)))
                .replace("$end", formatInstantNanos(Instant.now().plusSeconds(SECONDS_PER_DAY)))
        );
        assertOK(client().performRequest(request));

        // Rollover to create a new index with the new settings.
        request = new Request("POST", "/k9s/_rollover");
        client().performRequest(request);

        // Insert a doc with a current timestamp.
        request = new Request("POST", "/k9s/_doc");
        request.setJsonEntity(DOC.replace("$time", formatInstantNanos(Instant.now())));
        assertOK(client().performRequest(request));

        request = new Request("POST", "_refresh");
        assertOK(client().performRequest(request));

        var searchRequest = new Request("GET", "k9s/_search");
        response = client().performRequest(searchRequest);
        assertOK(response);
        responseBody = entityAsMap(response);
        try {
            assertThat(ObjectPath.evaluate(responseBody, "hits.total.value"), equalTo(9));
            assertThat(ObjectPath.evaluate(responseBody, "hits.total.relation"), equalTo("eq"));
        } catch (Exception | AssertionError e) {
            logger.error("search response body causing assertion error [" + responseBody + "]", e);
            throw e;
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
