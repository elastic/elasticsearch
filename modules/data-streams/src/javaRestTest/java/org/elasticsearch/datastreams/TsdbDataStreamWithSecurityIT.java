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
        .setting("data_streams.time_series.create_past_indices_enabled", "true")
        .user("test_admin", PASSWORD, "superuser", false)
        .user("tsdb_writer", PASSWORD, "tsdb_limited_writer", false)
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
        // Note: This user is assigned the role "tsdb_limited_writer". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("tsdb_writer", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Verifies that a user with only {@code auto_configure} and {@code index} privileges can backfill a TSDB data stream
     * up to the point allowed by the data lifecycle retention, and that documents with timestamps older than the retention
     * window are rejected.
     * <p>
     * Sequence:
     * <ol>
     *   <li>Create TSDB data stream (admin).</li>
     *   <li>Index a document 30 days in the past with the limited user — accepted, no retention is configured.</li>
     *   <li>Configure a 7-day DLM retention (admin).</li>
     *   <li>Index a document 6 days in the past with the limited user — accepted, within the 7-day window.</li>
     *   <li>Index a document 32 days in the past with the limited user — rejected, outside the 7-day window.</li>
     * </ol>
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

        try (var writerClient = buildClient(tsdbWriterRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            String thirtyTwoDaysAgo = Instant.now().minus(32, ChronoUnit.DAYS).toString();
            String thirtyDaysAgo = Instant.now().minus(30, ChronoUnit.DAYS).toString();
            String sixDaysAgo = Instant.now().minus(6, ChronoUnit.DAYS).toString();

            // No retention configured — the write window is unlimited so documents 30 days ago are accepted.
            Map<String, Object> response30Before = entityAsMap(writerClient.performRequest(bulkCreateRequest(thirtyDaysAgo)));
            assertThat(response30Before.get("errors"), is(false));

            // Set 7-day DLM retention.
            Request putLifecycle = new Request("PUT", "/_data_stream/" + TSDB_DATA_STREAM_NAME + "/_lifecycle");
            putLifecycle.setJsonEntity("""
                { "data_retention": "7d" }
                """);
            assertAcknowledged(adminClient().performRequest(putLifecycle));

            // 6 days ago is within the 7-day write window, so the document is accepted.
            Map<String, Object> response6d = entityAsMap(writerClient.performRequest(bulkCreateRequest(sixDaysAgo)));
            assertThat(response6d.get("errors"), is(false));

            // 32 days ago is outside the 7-day write window — the item must fail.
            Map<String, Object> response32d = entityAsMap(writerClient.performRequest(bulkCreateRequest(thirtyTwoDaysAgo)));
            assertThat(response32d.get("errors"), equalTo(true));
            Map<String, Object> firstItem = ((List<Map<String, Object>>) response32d.get("items")).getFirst();
            Map<String, Object> create = (Map<String, Object>) firstItem.get("create");
            Map<String, Object> error = (Map<String, Object>) create.get("error");
            assertThat(error.get("type"), equalTo("timestamp_error"));
        }
    }

    private static Request bulkCreateRequest(String timestamp) {
        Request request = new Request("POST", "/" + TSDB_DATA_STREAM_NAME + "/_bulk");
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"create":{}}
            {"@timestamp":"%s","pod_name":"test-pod"}
            """, timestamp));
        return request;
    }

}
