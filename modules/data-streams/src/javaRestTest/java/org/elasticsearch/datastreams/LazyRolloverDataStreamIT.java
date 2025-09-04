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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class LazyRolloverDataStreamIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";
    private static final String DATA_STREAM_NAME = "lazy-ds";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", PASSWORD, "superuser", false)
        .user("test_simple_user", PASSWORD, "under_privilged", false)
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
            // Note: We use the admin user because the other one is too unprivileged, so it breaks the initialization of the test
            String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
    }

    private Settings simpleUserRestClientSettings() {
        // Note: This user is assigned the role "under_privilged". That role is defined in roles.yml.
        String token = basicAuthHeaderValue("test_simple_user", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Request putComposableIndexTemplateRequest = new Request("POST", "/_index_template/lazy-ds-template");
        putComposableIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns": ["lazy-ds*"],
              "data_stream": {}
            }
            """);
        assertOK(adminClient().performRequest(putComposableIndexTemplateRequest));
        assertOK(adminClient().performRequest(new Request("PUT", "/_data_stream/" + DATA_STREAM_NAME)));
    }

    @SuppressWarnings("unchecked")
    public void testLazyRollover() throws Exception {
        try (var simpleUserClient = buildClient(simpleUserRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");
            assertOK(simpleUserClient.performRequest(createDocRequest));
            refresh(DATA_STREAM_NAME);

            {
                ResponseException responseError = expectThrows(
                    ResponseException.class,
                    () -> simpleUserClient.performRequest(new Request("POST", "/" + DATA_STREAM_NAME + "/_rollover?lazy"))
                );
                assertThat(responseError.getResponse().getStatusLine().getStatusCode(), is(403));
                assertThat(
                    responseError.getMessage(),
                    containsString("action [indices:admin/rollover] is unauthorized for user [test_simple_user]")
                );
            }
            final Response rolloverResponse = adminClient().performRequest(new Request("POST", "/" + DATA_STREAM_NAME + "/_rollover?lazy"));
            Map<String, Object> rolloverResponseMap = entityAsMap(rolloverResponse);
            assertThat((String) rolloverResponseMap.get("old_index"), startsWith(".ds-lazy-ds-"));
            assertThat((String) rolloverResponseMap.get("old_index"), endsWith("-000001"));
            assertThat((String) rolloverResponseMap.get("new_index"), startsWith(".ds-lazy-ds-"));
            assertThat((String) rolloverResponseMap.get("new_index"), endsWith("-000002"));
            assertThat(rolloverResponseMap.get("lazy"), equalTo(true));
            assertThat(rolloverResponseMap.get("dry_run"), equalTo(false));
            assertThat(rolloverResponseMap.get("acknowledged"), equalTo(true));
            assertThat(rolloverResponseMap.get("rolled_over"), equalTo(false));
            assertThat(rolloverResponseMap.get("conditions"), equalTo(Map.of()));

            {
                final Response dataStreamResponse = adminClient().performRequest(new Request("GET", "/_data_stream/" + DATA_STREAM_NAME));
                List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
                assertThat(dataStream.get("rollover_on_write"), is(true));
                assertThat(((List<Object>) dataStream.get("indices")).size(), is(1));
            }

            createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-23\", \"a\": 2 }");
            assertOK(simpleUserClient.performRequest(createDocRequest));
            refresh(DATA_STREAM_NAME);

            {
                final Response dataStreamResponse = simpleUserClient.performRequest(
                    new Request("GET", "/_data_stream/" + DATA_STREAM_NAME)
                );
                List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
                assertThat(dataStream.get("rollover_on_write"), is(false));
                assertThat(((List<Object>) dataStream.get("indices")).size(), is(2));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testLazyRolloverFailsIndexing() throws Exception {
        try (var simpleUserClient = buildClient(simpleUserRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");
            assertOK(simpleUserClient.performRequest(createDocRequest));
            refresh(DATA_STREAM_NAME);

            Request updateClusterSettingsRequest = new Request("PUT", "_cluster/settings");
            updateClusterSettingsRequest.setJsonEntity("""
                {
                  "persistent": {
                    "cluster.max_shards_per_node": 1
                  }
                }""");
            assertAcknowledged(adminClient().performRequest(updateClusterSettingsRequest));

            final Response rolloverResponse = adminClient().performRequest(new Request("POST", "/" + DATA_STREAM_NAME + "/_rollover?lazy"));
            Map<String, Object> rolloverResponseMap = entityAsMap(rolloverResponse);
            assertThat((String) rolloverResponseMap.get("old_index"), startsWith(".ds-lazy-ds-"));
            assertThat((String) rolloverResponseMap.get("old_index"), endsWith("-000001"));
            assertThat((String) rolloverResponseMap.get("new_index"), startsWith(".ds-lazy-ds-"));
            assertThat((String) rolloverResponseMap.get("new_index"), endsWith("-000002"));
            assertThat(rolloverResponseMap.get("lazy"), equalTo(true));
            assertThat(rolloverResponseMap.get("dry_run"), equalTo(false));
            assertThat(rolloverResponseMap.get("acknowledged"), equalTo(true));
            assertThat(rolloverResponseMap.get("rolled_over"), equalTo(false));
            assertThat(rolloverResponseMap.get("conditions"), equalTo(Map.of()));

            {
                final Response dataStreamResponse = simpleUserClient.performRequest(
                    new Request("GET", "/_data_stream/" + DATA_STREAM_NAME)
                );
                List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
                assertThat(dataStream.get("rollover_on_write"), is(true));
                assertThat(((List<Object>) dataStream.get("indices")).size(), is(1));
            }

            try {
                createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
                createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-23\", \"a\": 2 }");
                simpleUserClient.performRequest(createDocRequest);
                fail("Indexing should have failed.");
            } catch (ResponseException responseException) {
                assertThat(responseException.getMessage(), containsString("this action would add [2] shards"));
            }

            updateClusterSettingsRequest = new Request("PUT", "_cluster/settings");
            updateClusterSettingsRequest.setJsonEntity("""
                {
                  "persistent": {
                    "cluster.max_shards_per_node": null
                  }
                }""");
            assertAcknowledged(adminClient().performRequest(updateClusterSettingsRequest));
            createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-23\", \"a\": 2 }");
            assertOK(simpleUserClient.performRequest(createDocRequest));
            refresh(DATA_STREAM_NAME);
            {
                final Response dataStreamResponse = simpleUserClient.performRequest(
                    new Request("GET", "/_data_stream/" + DATA_STREAM_NAME)
                );
                List<Object> dataStreams = (List<Object>) entityAsMap(dataStreamResponse).get("data_streams");
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStream = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStream.get("name"), equalTo(DATA_STREAM_NAME));
                assertThat(dataStream.get("rollover_on_write"), is(false));
                assertThat(((List<Object>) dataStream.get("indices")).size(), is(2));
            }
        }
    }

    public void testLazyRolloverWithConditions() throws Exception {
        try (var simpleUserClient = buildClient(simpleUserRestClientSettings(), getClusterHosts().toArray(new HttpHost[0]))) {
            Request createDocRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_doc");
            createDocRequest.setJsonEntity("{ \"@timestamp\": \"2020-10-22\", \"a\": 1 }");
            assertOK(simpleUserClient.performRequest(createDocRequest));
            refresh(DATA_STREAM_NAME);

            Request rolloverRequest = new Request("POST", "/" + DATA_STREAM_NAME + "/_rollover?lazy");
            rolloverRequest.setJsonEntity("{\"conditions\": {\"max_docs\": 1}}");
            ResponseException responseError = expectThrows(ResponseException.class, () -> adminClient().performRequest(rolloverRequest));
            assertThat(responseError.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(responseError.getMessage(), containsString("only without any conditions"));
        }
    }
}
