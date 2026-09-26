/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TsdbDataStreamWithSecurityIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";
    private static final String TSDB_DATA_STREAM_NAME = "metrics-tsdb-test";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("data_stream.past_tsdb_index_creation_enabled", "true")
        .user("test_admin", PASSWORD, "superuser", false)
        .user("tsdb_writer", PASSWORD, "tsdb_writer", false)
        .user("tsdb_limited_writer", PASSWORD, "tsdb_limited_writer", false)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        // If this test is running in a test framework that handles its own authorization, we don't want to overwrite it.
        if (super.restClientSettings().keySet().contains(ThreadContext.PREFIX + ".Authorization")) {
            return super.restClientSettings();
        } else {
            String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    private Settings tsdbWriterRestClientSettings() {
        // Note: This user is assigned the role "tsdb_writer". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("tsdb_writer", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private Settings tsdbLimitedWriterRestClientSettings() {
        // Note: This user is assigned the role "tsdb_limited_writer". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("tsdb_limited_writer", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * The test asserts that backfill writes to a TSDB data stream can create new past-generation indices only when:
     * (a) the writer has the necessary security privilege
     * (b) the cluster feature flag is enabled and
     * (c) the timestamp falls within the configured DLM retention window.violating any one of these produces the
     *     correct distinct error (security_exception vs timestamp_error).
     * Violating any one of these produces the correct distinct error (security_exception vs timestamp_error). More
     * specifically, the test uses 3 different users:
     * - an admin for cluster management tasks, flag updating and index template creation
     * - a writer that can create past indices when needed
     * - a limited writer that cannot create past indices, this path might give a security exception.
     * The feature is enabled and we try to index the following timestamps (we use unique dimensions to ensure we will not create
     * a document conflict):
     * - 30 minutes and 1 hour ago, both writers should be able to index these docs and the target index should be the first one,
     *   so generation is 1.
     * - 30 days ago, the limited writer got a security exception, while the other writer creates it second, so generation is 2.
     * - 6 days ago, the writer created it third in line, so generation is 3.
     * - 32 days ago, comes after the lifecycle has been updated and falls out of the write window so the writer gets a timestamp_error.
     * - We disable the feature.
     * - 3 days ago, both users get a timestamp_error since the feature cannot create past indices.
     * - However, both can keep indexing on the past indices already created before the feature was disabled.
     */
    @SuppressWarnings("unchecked")
    public void testTsdbBackfillWriteWindowEnforcedByLimitedUser() throws Exception {
        Request putTemplateRequest = new Request("POST", "/_index_template/metrics-tsdb-test-template");
        putTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["metrics-tsdb-test*"],
              "template": {
                "settings": {
                  "index": {
                    "mode": "time_series",
                    "routing_path": ["pod_name"],
                    "number_of_replicas": 0,
                    "number_of_shards": 1
                  }
                },
                "mappings": {
                  "properties": {
                    "@timestamp": { "type": "date" },
                    "pod_name":   { "type": "keyword", "time_series_dimension": true }
                  }
                }
              },
              "data_stream": {},
              "priority": 500
            }
            """);
        assertAcknowledged(adminClient().performRequest(putTemplateRequest));
        assertAcknowledged(adminClient().performRequest(new Request("PUT", "/_data_stream/" + TSDB_DATA_STREAM_NAME)));

        Instant now = Instant.now();
        String thirtyTwoDaysAgo = now.minus(32, ChronoUnit.DAYS).toString();
        String thirtyDaysAgo = now.minus(30, ChronoUnit.DAYS).toString();
        String sixDaysAgo = now.minus(6, ChronoUnit.DAYS).toString();
        String threeDaysAgo = now.minus(3, ChronoUnit.DAYS).toString();
        String oneHourAgo = now.minus(1, ChronoUnit.HOURS).toString();
        String halfHourAgo = now.minus(30, ChronoUnit.MINUTES).toString();

        String writeIndexName;
        String sixDaysAgoIndexName;
        String thirtyDaysAgoIndexName;
        try (var limitedWriterClient = buildClient(tsdbLimitedWriterRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            // No retention configured — but the writer doesn't have enough privileges to create past indices
            Map<String, Object> response = entityAsMap(
                limitedWriterClient.performRequest(bulkCreateRequest(thirtyDaysAgo, oneHourAgo, halfHourAgo))
            );
            assertThat(response.get("errors"), is(true));
            List<Map<String, Object>> responseItems = getResponseItems(response);
            assertThat(responseItems.size(), is(3));
            assertThat(((Map<String, String>) responseItems.get(0).get("error")).get("type"), equalTo("security_exception"));
            writeIndexName = (String) responseItems.get(1).get("_index");
            assertThat(writeIndexName, endsWith("-000001"));
            assertThat((String) responseItems.get(2).get("_index"), equalTo(writeIndexName));
        }

        try (var writerClient = buildClient(tsdbWriterRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            // No retention configured — the write window is unlimited so documents 30 days ago are accepted.
            Map<String, Object> response = entityAsMap(
                writerClient.performRequest(bulkCreateRequest(thirtyDaysAgo, oneHourAgo, halfHourAgo))
            );
            assertThat(response.get("errors"), is(false));
            List<Map<String, Object>> responseItems = getResponseItems(response);
            assertThat(responseItems.size(), is(3));
            thirtyDaysAgoIndexName = (String) responseItems.get(0).get("_index");
            assertThat(thirtyDaysAgoIndexName, endsWith("-000002"));
            assertThat((String) responseItems.get(1).get("_index"), equalTo(writeIndexName));
            assertThat((String) responseItems.get(2).get("_index"), equalTo(writeIndexName));

            // Set 7-day DLM retention.
            Request putLifecycle = new Request("PUT", "/_data_stream/" + TSDB_DATA_STREAM_NAME + "/_lifecycle");
            putLifecycle.setJsonEntity("""
                { "data_retention": "7d" }
                """);
            assertAcknowledged(adminClient().performRequest(putLifecycle));

            // 6 days should be accepted, but 32 days should be rejected; 30 days has already been created so it can be accepted.
            response = entityAsMap(writerClient.performRequest(bulkCreateRequest(sixDaysAgo, thirtyDaysAgo, thirtyTwoDaysAgo)));
            assertThat(response.get("errors"), is(true));
            responseItems = getResponseItems(response);
            assertThat(responseItems.size(), is(3));
            sixDaysAgoIndexName = (String) responseItems.get(0).get("_index");
            assertThat(sixDaysAgoIndexName, endsWith("-000003"));
            assertThat((String) responseItems.get(1).get("_index"), equalTo(thirtyDaysAgoIndexName));
            assertThat(((Map<String, String>) responseItems.get(2).get("error")).get("type"), equalTo("timestamp_error"));

            // Disable past index creation
            updateClusterSettings(adminClient(), Settings.builder().put("data_stream.past_tsdb_index_creation_enabled", false).build());

            // 3 and 32 days should be rejected; 6 and 30 days has already been created so it can be accepted.
            // Half-hour is current, so it should be accepted.
            response = entityAsMap(
                writerClient.performRequest(bulkCreateRequest(threeDaysAgo, sixDaysAgo, thirtyDaysAgo, halfHourAgo, thirtyTwoDaysAgo))
            );
            assertThat(response.get("errors"), is(true));
            responseItems = getResponseItems(response);
            assertThat(responseItems.size(), is(5));
            assertThat(((Map<String, String>) responseItems.get(0).get("error")).get("type"), equalTo("timestamp_error"));
            assertThat((String) responseItems.get(1).get("_index"), equalTo(sixDaysAgoIndexName));
            assertThat((String) responseItems.get(2).get("_index"), equalTo(thirtyDaysAgoIndexName));
            assertThat((String) responseItems.get(3).get("_index"), equalTo(writeIndexName));
            assertThat(((Map<String, String>) responseItems.get(4).get("error")).get("type"), equalTo("timestamp_error"));
        }

        try (var limitedWriterClient = buildClient(tsdbLimitedWriterRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            // This time the limited writer can write in previously created indices. And since the feature is disabled,
            // the error is a timestamp error.
            Map<String, Object> response = entityAsMap(
                limitedWriterClient.performRequest(bulkCreateRequest(threeDaysAgo, sixDaysAgo, thirtyDaysAgo, halfHourAgo))
            );
            assertThat(response.get("errors"), is(true));
            List<Map<String, Object>> responseItems = getResponseItems(response);
            assertThat(responseItems.size(), is(4));
            assertThat(((Map<String, String>) responseItems.get(0).get("error")).get("type"), equalTo("timestamp_error"));
            assertThat((String) responseItems.get(1).get("_index"), equalTo(sixDaysAgoIndexName));
            assertThat((String) responseItems.get(2).get("_index"), equalTo(thirtyDaysAgoIndexName));
            assertThat((String) responseItems.get(3).get("_index"), equalTo(writeIndexName));
        }

        // Now let's change the DLM poll interval so it can apply retention
        updateClusterSettings(adminClient(), Settings.builder().put("data_streams.lifecycle.poll_interval", "1s").build());
        awaitIndexDoesNotExist(thirtyDaysAgoIndexName);
        List<String> backingIndexNames = getDataStreamBackingIndexNames(TSDB_DATA_STREAM_NAME);
        assertThat(backingIndexNames.size(), is(2));
        assertThat(backingIndexNames, containsInAnyOrder(writeIndexName, sixDaysAgoIndexName));
    }

    private static Request bulkCreateRequest(String... timestamps) {
        Request request = new Request("POST", "/" + TSDB_DATA_STREAM_NAME + "/_bulk");
        StringBuilder payloadBuilder = new StringBuilder();
        for (String timestamp : timestamps) {
            payloadBuilder.append(String.format(Locale.ROOT, """
                {"create":{}}
                {"@timestamp":"%s","pod_name":"%s"}
                """, timestamp, randomAlphaOfLength(10)));
        }
        request.setJsonEntity(payloadBuilder.toString());
        return request;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getResponseItems(Map<String, Object> response) {
        return ((List<Map<String, Object>>) response.get("items")).stream().map(item -> (Map<String, Object>) item.get("create")).toList();
    }
}
