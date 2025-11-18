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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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

/**
 * This test starts with LogsDB disabled, performs an upgrade, enables LogsDB and indexes some documents.
 */
public class StandardToLogsDbIndexModeRollingUpgradeIT extends AbstractRollingUpgradeWithSecurityTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    private static final String LOGS_TEMPLATE = "logs-template";
    private static final String DATA_STREAM = "logs-apache-production";

    @ClassRule()
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .user(USER, PASS)
        .module("constant-keyword")
        .module("data-streams")
        .module("mapper-extras")
        .module("x-pack-aggregate-metric")
        .module("x-pack-stack")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.license.self_generated.type", initTestSeed().nextBoolean() ? "trial" : "basic")
        // LogsDB is enabled by default for data streams matching the logs-*-* pattern, and since we upgrade from standard to logsdb,
        // we need to start with logsdb disabled, then later enable it and rollover
        .setting("cluster.logsdb.enabled", "false")
        .setting("stack.templates.enabled", "false")
        .build();

    public StandardToLogsDbIndexModeRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static final String BULK_INDEX_REQUEST_TEMPLATE = """
        { "create": {} }
        { "@timestamp": "$timestamp", "host.name": "$hostname", "method": "$method", "ip.address": "$ip", "message": "$message" }
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

    public void testLogsIndexing() throws IOException {
        if (isOldCluster()) {
            // given - create a template and data stream
            putTemplate();
            createDataStream();

            // when/then - index some documents and ensure no issues occurred
            bulkIndex(this::bulkIndexRequestBody);

            // then continued - verify that the created data stream uses the created template
            LogsdbIndexingRollingUpgradeIT.assertDataStream(DATA_STREAM, LOGS_TEMPLATE);

        } else if (isMixedCluster()) {
            // when/then - index more documents
            bulkIndex(this::bulkIndexRequestBody);

        } else if (isUpgradedCluster()) {
            // when/then - index some more documents
            bulkIndex(this::bulkIndexRequestBody);

            // given - enable logsdb and rollover
            enableLogsdbByDefault();
            rolloverDataStream();

            // when/then
            bulkIndex(this::bulkIndexRequestBody);

            // then continued - verify that only the latest write index has logsdb enabled
            assertIndexSettings(0, Matchers.nullValue());
            assertIndexSettings(1, Matchers.equalTo("logsdb"));
        }
    }

    static void enableLogsdbByDefault() throws IOException {
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

    private String bulkIndexRequestBody() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < randomIntBetween(10, 20); i++) {
            sb.append(
                BULK_INDEX_REQUEST_TEMPLATE.replace(
                    "$timestamp",
                    DateFormatter.forPattern(FormatNames.DATE_TIME.getName()).format(Instant.now())
                )
                    .replace("$hostname", randomFrom("potato.host", "tomato.host"))
                    .replace("$method", randomFrom("PUT", "POST", "GET"))
                    .replace("$ip", NetworkAddress.format(randomIp(randomBoolean())))
                    .replace("$message", randomAlphaOfLength(128))
            );
            sb.append("\n");
        }
        return sb.toString();
    }

    private void assertIndexSettings(int backingIndex, final Matcher<Object> indexModeMatcher) throws IOException {
        assertThat(getSettings(client(), getWriteBackingIndex(client(), DATA_STREAM, backingIndex)).get("index.mode"), indexModeMatcher);
    }

    private static void createDataStream() throws IOException {
        final Request request = new Request("PUT", "/_data_stream/" + DATA_STREAM);
        final Response response = client().performRequest(request);
        assertOK(response);
    }

    private static void bulkIndex(final Supplier<String> bulkIndexRequestSupplier) throws IOException {
        final Request request = new Request("POST", DATA_STREAM + "/_bulk");
        request.setJsonEntity(bulkIndexRequestSupplier.get());
        request.addParameter("refresh", "true");

        final Response response = client().performRequest(request);
        final var responseBody = entityAsMap(response);

        // then - ensure no issues
        assertOK(response);
        assertThat("errors in response:\n " + responseBody, responseBody.get("errors"), Matchers.is(false));
    }

    private static void putTemplate() throws IOException {
        final Request request = new Request("PUT", "/_index_template/" + LOGS_TEMPLATE);
        request.setJsonEntity(STANDARD_TEMPLATE);
        final Response response = client().performRequest(request);
        assertOK(response);
    }

    private static void rolloverDataStream() throws IOException {
        final Request request = new Request("POST", "/" + DATA_STREAM + "/_rollover");
        final Response response = client().performRequest(request);
        assertOK(response);
    }

    @SuppressWarnings("unchecked")
    static String getWriteBackingIndex(final RestClient client, final String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", "_data_stream/" + dataStreamName);
        final List<Object> DATA_STREAMs = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> DATA_STREAM = (Map<String, Object>) DATA_STREAMs.get(0);
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) DATA_STREAM.get("indices");
        return backingIndices.get(backingIndex).get("index_name");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getSettings(final RestClient client, final String indexName) throws IOException {
        final Request request = new Request("GET", "/" + indexName + "/_settings?flat_settings");
        return ((Map<String, Map<String, Object>>) entityAsMap(client.performRequest(request)).get(indexName)).get("settings");
    }
}
