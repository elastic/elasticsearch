/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class DataStreamRestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("indices.lifecycle.history_index_enabled", "false")
        .keystore("bootstrap.password", "x-pack-test-password")
        .user("x_pack_rest_user", "x-pack-test-password")
        .systemProperty("es.queryable_built_in_roles_enabled", "false")
        .build();

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    public void testDSXpackInfo() {
        Map<?, ?> features = (Map<?, ?>) getLocation("/_xpack").get("features");
        assertNotNull(features);
        Map<?, ?> dataStreams = (Map<?, ?>) features.get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
    }

    @SuppressWarnings("unchecked")
    public void testDSXpackUsage() throws Exception {
        Map<?, ?> dataStreams = (Map<?, ?>) getLocation("/_xpack/usage").get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
        assertThat(dataStreams.get("data_streams"), equalTo(0));
        assertThat(dataStreams, hasKey("failure_store"));
        Map<String, Integer> failureStoreStats = (Map<String, Integer>) dataStreams.get("failure_store");
        assertThat(failureStoreStats.get("explicitly_enabled_count"), equalTo(0));
        assertThat(failureStoreStats.get("effectively_enabled_count"), equalTo(0));
        assertThat(failureStoreStats.get("failure_indices_count"), equalTo(0));
        assertBusy(() -> {
            Map<?, ?> logsTemplate = (Map<?, ?>) ((List<?>) getLocation("/_index_template/logs").get("index_templates")).get(0);
            assertThat(logsTemplate, notNullValue());
            assertThat(logsTemplate.get("name"), equalTo("logs"));
            assertThat(((Map<?, ?>) logsTemplate.get("index_template")).get("data_stream"), notNullValue());
        });
        putFailureStoreTemplate();

        // Create a data stream
        Request indexRequest = new Request("POST", "/logs-mysql-default/_doc");
        indexRequest.setJsonEntity("{\"@timestamp\": \"2020-01-01\"}");
        client().performRequest(indexRequest);

        // Roll over the data stream
        Request rollover = new Request("POST", "/logs-mysql-default/_rollover");
        client().performRequest(rollover);

        // Create failure store data stream
        indexRequest = new Request("POST", "/fs/_doc");
        indexRequest.setJsonEntity("{\"@timestamp\": \"2020-01-01\"}");
        client().performRequest(indexRequest);
        // Initialize the failure store
        rollover = new Request("POST", "/fs::failures/_rollover");
        client().performRequest(rollover);

        dataStreams = (Map<?, ?>) getLocation("/_xpack/usage").get("data_streams");
        assertNotNull(dataStreams);
        assertTrue((boolean) dataStreams.get("available"));
        assertTrue((boolean) dataStreams.get("enabled"));
        assertThat("got: " + dataStreams, dataStreams.get("data_streams"), equalTo(2));
        assertThat("got: " + dataStreams, dataStreams.get("indices_count"), equalTo(3));
        failureStoreStats = (Map<String, Integer>) dataStreams.get("failure_store");
        assertThat(failureStoreStats.get("explicitly_enabled_count"), equalTo(1));
        assertThat(failureStoreStats.get("effectively_enabled_count"), equalTo(1));
        assertThat(failureStoreStats.get("failure_indices_count"), equalTo(1));

        // Enable the failure store for logs-mysql-default using the cluster setting...
        updateClusterSettings(
            Settings.builder()
                .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), "logs-mysql-default")
                .build()
        );
        // ...and assert that it counts towards effectively_enabled_count but not explicitly_enabled_count:
        dataStreams = (Map<?, ?>) getLocation("/_xpack/usage").get("data_streams");
        failureStoreStats = (Map<String, Integer>) dataStreams.get("failure_store");
        assertThat(failureStoreStats.get("explicitly_enabled_count"), equalTo(1));
        assertThat(failureStoreStats.get("effectively_enabled_count"), equalTo(2));
    }

    Map<String, Object> getLocation(String path) {
        try {
            Response executeResponse = client().performRequest(new Request("GET", path));
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY,
                    EntityUtils.toByteArray(executeResponse.getEntity())
                )
            ) {
                return parser.map();
            }
        } catch (Exception e) {
            fail("failed to execute GET request to " + path + " - got: " + e);
            throw new RuntimeException(e);
        }
    }

    private void putFailureStoreTemplate() {
        try {
            Request request = new Request("PUT", "/_index_template/fs-template");
            request.setJsonEntity("""
                  {
                  "index_patterns": ["fs*"],
                  "data_stream": {},
                  "template": {
                    "data_stream_options": {
                      "failure_store": {
                        "enabled": true
                      }
                    }
                  }
                }
                """);
            assertAcknowledged(client().performRequest(request));
        } catch (Exception e) {
            fail("failed to insert index template with failure store enabled - got: " + e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
