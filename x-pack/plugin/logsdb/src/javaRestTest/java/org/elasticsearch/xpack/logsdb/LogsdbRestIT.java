/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LogsdbRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testFeatureUsageWithLogsdbIndex() throws IOException {
        {
            if (randomBoolean()) {
                createIndex("test-index", Settings.builder().put("index.mode", "logsdb").build());
            } else if (randomBoolean()) {
                String mapping = """
                    {
                        "properties": {
                            "field1": {
                                "type": "keyword",
                                "time_series_dimension": true
                            }
                        }
                    }
                    """;
                var settings = Settings.builder().put("index.mode", "time_series").put("index.routing_path", "field1").build();
                createIndex("test-index", settings, mapping);
            } else {
                String mapping = """
                    {
                        "_source": {
                            "mode": "synthetic"
                        }
                    }
                    """;
                createIndex("test-index", Settings.EMPTY, mapping);
            }
            var response = getAsMap("/_license/feature_usage");
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> features = (List<Map<?, ?>>) response.get("features");
            logger.info("response's features: {}", features);
            assertThat(features, Matchers.not(Matchers.empty()));
            Map<?, ?> feature = features.stream().filter(map -> "mappings".equals(map.get("family"))).findFirst().get();
            assertThat(feature.get("name"), equalTo("synthetic-source"));
            assertThat(feature.get("license_level"), equalTo("enterprise"));

            var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings("test-index").get("test-index")).get("settings");
            assertNull(settings.get("index.mapping.source.mode"));  // Default, no downgrading.
        }
    }

    public void testLogsdbSourceModeForLogsIndex() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{ \"transient\": { \"cluster.logsdb.enabled\": true } }");
        assertOK(client().performRequest(request));

        request = new Request("POST", "/_index_template/1");
        request.setJsonEntity("""
            {
                "index_patterns": ["logs-test-*"],
                "data_stream": {
                }
            }
            """);
        assertOK(client().performRequest(request));

        request = new Request("POST", "/logs-test-foo/_doc");
        request.setJsonEntity("""
            {
                "@timestamp": "2020-01-01T00:00:00.000Z",
                "host.name": "foo",
                "message": "bar"
            }
            """);
        assertOK(client().performRequest(request));

        String index = DataStream.getDefaultBackingIndexName("logs-test-foo", 1);
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertEquals("logsdb", settings.get("index.mode"));
        assertNull(settings.get("index.mapping.source.mode"));
    }

    public void testEsqlRuntimeFields() throws IOException {
        String mappings = """
            {
                "runtime": {
                    "message_length": {
                        "type": "long"
                    },
                    "log.offset": {
                        "type": "long"
                    }
                },
                "dynamic": false,
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "log" : {
                        "properties": {
                            "level": {
                                "type": "keyword"
                            },
                            "file": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }
            """;
        String indexName = "test-foo";
        createIndex(indexName, Settings.builder().put("index.mode", "logsdb").build(), mappings);

        int numDocs = 500;
        var sb = new StringBuilder();
        var now = Instant.now();

        var expectedMinTimestamp = now;
        for (int i = 0; i < numDocs; i++) {
            String level = randomBoolean() ? "info" : randomBoolean() ? "warning" : randomBoolean() ? "error" : "fatal";
            String msg = randomAlphaOfLength(20);
            String path = randomAlphaOfLength(8);
            String messageLength = Integer.toString(msg.length());
            String offset = Integer.toString(randomNonNegativeInt());
            sb.append("{ \"create\": {} }").append('\n');
            if (randomBoolean()) {
                sb.append(
                    """
                        {"@timestamp":"$now","message":"$msg","message_length":$l,"file":{"level":"$level","offset":5,"file":"$path"}}
                        """.replace("$now", formatInstant(now))
                        .replace("$level", level)
                        .replace("$msg", msg)
                        .replace("$path", path)
                        .replace("$l", messageLength)
                        .replace("$o", offset)
                );
            } else {
                sb.append("""
                    {"@timestamp": "$now", "message": "$msg", "message_length": $l}
                    """.replace("$now", formatInstant(now)).replace("$msg", msg).replace("$l", messageLength));
            }
            sb.append('\n');
            if (i != numDocs - 1) {
                now = now.plusSeconds(1);
            }
        }
        var expectedMaxTimestamp = now;

        var bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(sb.toString());
        bulkRequest.addParameter("refresh", "true");
        var bulkResponse = client().performRequest(bulkRequest);
        var bulkResponseBody = responseAsMap(bulkResponse);
        assertThat(bulkResponseBody, Matchers.hasEntry("errors", false));

        var forceMergeRequest = new Request("POST", "/" + indexName + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        var forceMergeResponse = client().performRequest(forceMergeRequest);
        assertOK(forceMergeResponse);

        String query = "FROM test-foo | STATS count(*), min(@timestamp), max(@timestamp), min(message_length), max(message_length)"
            + " ,sum(message_length), avg(message_length), min(log.offset), max(log.offset) | LIMIT 1";
        final Request esqlRequest = new Request("POST", "/_query");
        esqlRequest.setJsonEntity("""
            {
                "query": "$query"
            }
            """.replace("$query", query));
        var esqlResponse = client().performRequest(esqlRequest);
        assertOK(esqlResponse);
        Map<String, Object> esqlResponseBody = responseAsMap(esqlResponse);

        List<?> values = (List<?>) esqlResponseBody.get("values");
        assertThat(values, Matchers.not(Matchers.empty()));
        var count = ((List<?>) values.get(0)).get(0);
        assertThat(count, equalTo(numDocs));
        logger.warn("VALUES: {}", values);

        var minTimestamp = ((List<?>) values.get(0)).get(1);
        assertThat(minTimestamp, equalTo(formatInstant(expectedMinTimestamp)));
        var maxTimestamp = ((List<?>) values.get(0)).get(2);
        assertThat(maxTimestamp, equalTo(formatInstant(expectedMaxTimestamp)));

        var minLength = ((List<?>) values.get(0)).get(3);
        assertThat(minLength, equalTo(20));
        var maxLength = ((List<?>) values.get(0)).get(4);
        assertThat(maxLength, equalTo(20));
        var sumLength = ((List<?>) values.get(0)).get(5);
        assertThat(sumLength, equalTo(20 * numDocs));
    }

    public void testSyntheticSourceRuntimeFieldQueries() throws IOException {
        String mappings = """
            {
                "runtime": {
                    "message_length": {
                        "type": "long"
                    }
                },
                "dynamic": false,
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "log" : {
                        "properties": {
                            "level": {
                                "type": "keyword"
                            }
                        }
                    }
                }
            }
            """;
        String indexName = "test-foo";
        createIndex(indexName, Settings.builder().put("index.mode", "logsdb").build(), mappings);

        int numDocs = 1000;
        var sb = new StringBuilder();
        var now = Instant.now();
        for (int i = 0; i < numDocs; i++) {
            String level = randomBoolean() ? "info" : randomBoolean() ? "warning" : randomBoolean() ? "error" : "fatal";
            String msg = randomAlphaOfLength(20);
            String messageLength = Integer.toString(msg.length());
            sb.append("{ \"create\": {} }").append('\n');
            if (randomBoolean()) {
                sb.append("""
                    {"@timestamp":"$now","message":"$msg","message_length":$l,"log":{"level":"$level"}}
                    """.replace("$now", formatInstant(now)).replace("$level", level).replace("$msg", msg).replace("$l", messageLength));
            } else {
                sb.append("""
                    {"@timestamp": "$now", "message": "$msg", "message_length": $l}
                    """.replace("$now", formatInstant(now)).replace("$msg", msg).replace("$l", messageLength));
            }
            sb.append('\n');
            if (i != numDocs - 1) {
                now = now.plusSeconds(1);
            }
        }

        var bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(sb.toString());
        bulkRequest.addParameter("refresh", "true");
        var bulkResponse = client().performRequest(bulkRequest);
        var bulkResponseBody = responseAsMap(bulkResponse);
        assertThat(bulkResponseBody, Matchers.hasEntry("errors", false));

        var forceMergeRequest = new Request("POST", "/" + indexName + "/_forcemerge");
        var forceMergeResponse = client().performRequest(forceMergeRequest);
        assertOK(forceMergeResponse);

        var searchRequest = new Request("POST", "/" + indexName + "/_search");

        searchRequest.setJsonEntity("""
            {
                "size": 1,
                "query": {
                    "bool": {
                        "should": [
                            {
                                "range": {
                                    "message_length": {
                                        "gte": 1,
                                        "lt": 900000
                                    }
                                }
                            },
                            {
                                "range": {
                                    "message_length": {
                                        "gte": 900000,
                                        "lt": 1000000
                                    }
                                }
                            }
                        ],
                        "minimum_should_match": "1",
                        "must_not": [
                            {
                                "range": {
                                    "message_length": {
                                        "lt": 0
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            """);
        var searchResponse = client().performRequest(searchRequest);
        assertOK(searchResponse);
        var searchResponseBody = responseAsMap(searchResponse);
        int totalHits = (int) XContentMapValues.extractValue("hits.total.value", searchResponseBody);
        assertThat(totalHits, equalTo(numDocs));

        var shardsHeader = (Map<?, ?>) searchResponseBody.get("_shards");
        assertThat(shardsHeader.get("failed"), equalTo(0));
        assertThat(shardsHeader.get("successful"), equalTo(1));
        assertThat(shardsHeader.get("skipped"), equalTo(0));
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

}
