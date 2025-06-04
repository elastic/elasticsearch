/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.WarningsHandler.PERMISSIVE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test checks that a Monitoring's HTTP exporter correctly exports to a monitoring cluster
 * protected by security with HTTPS/SSL.
 *
 * It sets up a cluster with Monitoring and Security configured with SSL. Once started,
 * an HTTP exporter is activated and it exports data locally over HTTPS/SSL. The test
 * then uses a rest client to check that the data have been correctly received and
 * indexed in the cluster.
 */
public class SmokeTestMonitoringWithSecurityIT extends ESRestTestCase {

    private static final String USER = "test_user";
    private static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());
    private static final String MONITORING_PATTERN = ".monitoring-*";

    static Path trustStore;

    @BeforeClass
    public static void getKeyStore() {
        try {
            trustStore = PathUtils.get(SmokeTestMonitoringWithSecurityIT.class.getResource("/testnode.crt").toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the truststore", e);
        }
        if (Files.exists(trustStore) == false) {
            throw new IllegalStateException("Truststore file [" + trustStore + "] does not exist.");
        }
    }

    @AfterClass
    public static void clearKeyStore() {
        trustStore = null;
    }

    @Override
    protected String getProtocol() {
        return "https";
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ESRestTestCase.CERTIFICATE_AUTHORITIES, trustStore)
            .build();
    }

    @Before
    public void enableExporter() throws Exception {
        Settings exporterSettings = Settings.builder()
            .put("xpack.monitoring.collection.enabled", true)
            .put("xpack.monitoring.exporters._http.enabled", true)
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "https://" + randomNodeHttpAddress())
            .build();
        updateClusterSettingsIgnoringWarnings(client(), exporterSettings);
    }

    public static void updateClusterSettingsIgnoringWarnings(RestClient client, Settings settings) throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(PERMISSIVE).build());
        String entity = "{ \"persistent\":" + Strings.toString(settings) + "}";
        request.setJsonEntity(entity);
        Response response = client.performRequest(request);
        assertOK(response);
    }

    @After
    public void disableExporter() throws IOException {
        Settings exporterSettings = Settings.builder()
            .putNull("xpack.monitoring.collection.enabled")
            .putNull("xpack.monitoring.exporters._http.enabled")
            .putNull("xpack.monitoring.exporters._http.type")
            .putNull("xpack.monitoring.exporters._http.host")
            .putNull("xpack.monitoring.exporters._http.auth.username")
            .putNull("xpack.monitoring.exporters._http.ssl.verification_mode")
            .putNull("xpack.monitoring.exporters._http.ssl.certificate_authorities")
            .build();
        updateClusterSettingsIgnoringWarnings(client(), exporterSettings);
    }

    @SuppressWarnings("unchecked")
    private boolean getMonitoringUsageExportersDefined() throws Exception {
        Map<String, Object> monitoringUsage = (Map<String, Object>) getAsMap("/_xpack/usage").get("monitoring");
        assertThat("Monitoring feature set does not exist", monitoringUsage, notNullValue());

        @SuppressWarnings("unchecked")
        Map<String, Object> exporters = (Map<String, Object>) monitoringUsage.get("enabled_exporters");
        return exporters != null && exporters.isEmpty() == false;
    }

    public void testHTTPExporterWithSSL() throws Exception {
        // Ensures that the exporter is actually on
        assertBusy(() -> assertThat("[_http] exporter is not defined", getMonitoringUsageExportersDefined(), is(true)));

        // Checks that the monitoring index templates have been installed
        Request templateRequest = new Request("GET", "/_index_template/" + MONITORING_PATTERN);
        assertBusy(() -> {
            try {
                var response = responseAsMap(client().performRequest(templateRequest));
                List<?> templates = ObjectPath.evaluate(response, "index_templates");
                assertThat(templates.size(), greaterThanOrEqualTo(2));
            } catch (Exception e) {
                fail("template not ready yet: " + e.getMessage());
            }
        });

        Request indexRequest = new Request("HEAD", MONITORING_PATTERN);
        // Waits for monitoring indices to be created
        assertBusy(() -> {
            try {
                Response response = client().performRequest(indexRequest);
                assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            } catch (Exception e) {
                fail("monitoring index not created yet: " + e.getMessage());
            }
        });

        // Waits for indices to be ready
        ensureHealth(MONITORING_PATTERN, (request) -> {
            request.addParameter("wait_for_status", "yellow");
            request.addParameter("wait_for_events", "languid");
            request.addParameter("wait_for_no_relocating_shards", "true");
            request.addParameter("wait_for_no_initializing_shards", "true");
        });

        // Checks that the HTTP exporter has successfully exported some data
        final Request searchRequest = new Request("POST", "/" + MONITORING_PATTERN + "/_search");
        searchRequest.setJsonEntity("""
            {"size":0}
            """);

        assertBusy(() -> {
            try {
                final Response searchResponse = client().performRequest(searchRequest);
                final ObjectPath path = ObjectPath.createFromResponse(searchResponse);
                assertThat(path.evaluate("hits.total.value"), greaterThan(0));
            } catch (Exception e) {
                fail("monitoring date not exported yet: " + e.getMessage());
            }
        });
    }

    public void testSettingsFilter() throws IOException {
        final Request request = new Request("GET", "/_cluster/settings");
        final Response response = client().performRequest(request);
        final ObjectPath path = ObjectPath.createFromResponse(response);
        final Map<String, Object> settings = path.evaluate("persistent.xpack.monitoring.exporters._http");
        assertThat(settings, hasKey("type"));
        assertThat(settings, not(hasKey("auth")));
        assertThat(settings, not(hasKey("ssl")));
    }

    @SuppressWarnings("unchecked")
    private String randomNodeHttpAddress() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_nodes"));
        assertOK(response);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        List<String> httpAddresses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : nodesAsMap.entrySet()) {
            Map<String, Object> nodeDetails = (Map<String, Object>) entry.getValue();
            Map<String, Object> httpInfo = (Map<String, Object>) nodeDetails.get("http");
            httpAddresses.add((String) httpInfo.get("publish_address"));
        }
        assertThat(httpAddresses.size(), greaterThan(0));
        return randomFrom(httpAddresses);
    }
}
