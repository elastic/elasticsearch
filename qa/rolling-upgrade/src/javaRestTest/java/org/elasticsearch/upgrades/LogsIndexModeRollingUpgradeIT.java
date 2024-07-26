/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;

public class LogsIndexModeRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    @ClassRule()
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("constant-keyword")
        .module("data-streams")
        .module("mapper-extras")
        .module("x-pack-aggregate-metric")
        .module("x-pack-stack")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("cluster.logsdb.enabled", "true")
        .setting("stack.templates.enabled", "false")
        .build();

    public LogsIndexModeRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static final String BULK_INDEX_REQUEST = """
        { "create": {} }
        { "@timestamp": "%s", "host.name": "%s", "method": "%s", "ip.address": "%s", "message": "%s" }
        """;

    private static final String STANDARD_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 500,
          "template": {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "host.name": {
                  "type": "keyword"
                },
                "method": {
                  "type": "keyword"
                },
                "message": {
                  "type": "text"
                },
                "ip.address": {
                  "type": "ip"
                }
              }
            }
          }
        }""";

    private static final String LOGS_TEMPLATE = """
        {
          "index_patterns": [ "logs-*-*" ],
          "data_stream": {},
          "priority": 500,
          "template": {
            "settings": {
              "index": {
                "mode": "logsdb"
              }
            },
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "host.name": {
                  "type": "keyword"
                },
                "method": {
                  "type": "keyword"
                },
                "message": {
                  "type": "text"
                },
                "ip.address": {
                  "type": "ip"
                }
              }
            }
          }
        }""";

    public void testLogsIndexing() throws IOException {
        if (isOldCluster()) {
            assertOK(client().performRequest(putTemplate(client(), "logs-template", STANDARD_TEMPLATE)));
            assertOK(client().performRequest(createDataStream("logs-apache-production")));
            final Response bulkIndexResponse = client().performRequest(bulkIndex("logs-apache-production", () -> {
                final StringBuilder sb = new StringBuilder();
                for (int i = 0; i < randomIntBetween(10, 20); i++) {
                    sb.append(
                        String.format(
                            BULK_INDEX_REQUEST,
                            DateFormatter.forPattern(FormatNames.DATE_TIME.getName()).format(Instant.now()),
                            randomFrom("foo", "bar"),
                            randomFrom("PUT", "POST", "GET"),
                            InetAddresses.toAddrString(randomIp(randomBoolean())),
                            randomIntBetween(20, 50)
                        )
                    );
                    sb.append("\n");
                }
                return sb.toString();
            }));
            assertOK(bulkIndexResponse);
            assertThat(entityAsMap(bulkIndexResponse).get("errors"), Matchers.is(false));
        } else if (isMixedCluster()) {
            assertOK(client().performRequest(rolloverDataStream(client(), "logs-apache-production")));
            final Response bulkIndexResponse = client().performRequest(bulkIndex("logs-apache-production", () -> {
                final StringBuilder sb = new StringBuilder();
                for (int i = 0; i < randomIntBetween(10, 20); i++) {
                    sb.append(
                        String.format(
                            BULK_INDEX_REQUEST,
                            DateFormatter.forPattern(FormatNames.DATE_TIME.getName()).format(Instant.now()),
                            randomFrom("foo", "bar"),
                            randomFrom("PUT", "POST", "GET"),
                            InetAddresses.toAddrString(randomIp(randomBoolean())),
                            randomIntBetween(20, 50)
                        )
                    );
                    sb.append("\n");
                }
                return sb.toString();
            }));
            assertOK(bulkIndexResponse);
            assertThat(entityAsMap(bulkIndexResponse).get("errors"), Matchers.is(false));
        } else if (isUpgradedCluster()) {
            assertOK(client().performRequest(putTemplate(client(), "logs-template", LOGS_TEMPLATE)));
            assertOK(client().performRequest(rolloverDataStream(client(), "logs-apache-production")));
            final Response bulkIndexResponse = client().performRequest(bulkIndex("logs-apache-production", () -> {
                final StringBuilder sb = new StringBuilder();
                for (int i = 0; i < randomIntBetween(10, 20); i++) {
                    sb.append(
                        String.format(
                            BULK_INDEX_REQUEST,
                            DateFormatter.forPattern(FormatNames.DATE_TIME.getName()).format(Instant.now()),
                            randomFrom("foo", "bar"),
                            randomFrom("PUT", "POST", "GET"),
                            InetAddresses.toAddrString(randomIp(randomBoolean())),
                            randomIntBetween(20, 50)
                        )
                    );
                    sb.append("\n");
                }
                return sb.toString();
            }));
            assertOK(bulkIndexResponse);
            assertThat(entityAsMap(bulkIndexResponse).get("errors"), Matchers.is(false));

            assertIndexMappingsAndSettings(0, Matchers.nullValue(), matchesMap().extraOk());
            assertIndexMappingsAndSettings(1, Matchers.nullValue(), matchesMap().extraOk());
            assertIndexMappingsAndSettings(2, Matchers.nullValue(), matchesMap().extraOk());
            assertIndexMappingsAndSettings(
                3,
                Matchers.equalTo("logsdb"),
                matchesMap().extraOk().entry("_source", Map.of("mode", "synthetic"))
            );
        }
    }

    private void assertIndexMappingsAndSettings(int backingIndex, final Matcher<Object> indexModeMatcher, final MapMatcher mappingsMatcher)
        throws IOException {
        assertThat(
            getSettings(client(), getWriteBackingIndex(client(), "logs-apache-production", backingIndex)).get("index.mode"),
            indexModeMatcher
        );
        assertMap(getIndexMappingAsMap(getWriteBackingIndex(client(), "logs-apache-production", backingIndex)), mappingsMatcher);
    }

    private static Request createDataStream(final String dataStreamName) {
        return new Request("PUT", "/_data_stream/" + dataStreamName);
    }

    private static Request bulkIndex(final String dataStreamName, final Supplier<String> bulkIndexRequestSupplier) {
        final Request request = new Request("POST", dataStreamName + "/_bulk");
        request.setJsonEntity(bulkIndexRequestSupplier.get());
        request.addParameter("refresh", "true");
        return request;
    }

    private static Request putTemplate(final RestClient client, final String templateName, final String mappings) throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + templateName);
        request.setJsonEntity(mappings);
        return request;
    }

    private static Request rolloverDataStream(final RestClient client, final String dataStreamName) throws IOException {
        return new Request("POST", "/" + dataStreamName + "/_rollover");
    }

    @SuppressWarnings("unchecked")
    private static String getWriteBackingIndex(final RestClient client, final String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        return backingIndices.get(backingIndex).get("index_name");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getSettings(final RestClient client, final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        return ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get("settings");
    }
}
