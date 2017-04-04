/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;
import org.elasticsearch.xpack.ml.MachineLearningTemplateRegistry;
import org.elasticsearch.xpack.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

/** Runs rest tests against external cluster */
public class XPackRestIT extends XPackRestTestCase {

    @After
    public void clearMlState() throws IOException {
        new MlRestTestStateCleaner(logger, adminClient(), this).clearMlMetadata();
    }

    public XPackRestIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    /**
     * Waits for the Security template to be created by the {@link SecurityLifecycleService} and
     * the Machine Learning templates to be created by {@link MachineLearningTemplateRegistry}
     */
    @Before
    public void waitForTemplates() throws Exception {
        List<String> templates = new ArrayList<>();
        templates.add(SecurityLifecycleService.SECURITY_TEMPLATE_NAME);
        templates.addAll(Arrays.asList(MachineLearningTemplateRegistry.TEMPLATE_NAMES));

        for (String template : templates) {
            awaitCallApi("indices.exists_template", singletonMap("name", template), emptyList(),
                    response -> true,
                    () -> "Exception when waiting for [" + template + "] template to be created");
        }
    }

    /**
     * Enable monitoring and waits for monitoring documents to be collected and indexed in
     * monitoring indices.This is the signal that the local exporter is started and ready
     * for the tests.
     */
    @Before
    public void enableMonitoring() throws Exception {
        if (isMonitoringTest()) {
            final Map<String, Object> settings = new HashMap<>();
            settings.put("xpack.monitoring.collection.interval", "3s");
            settings.put("xpack.monitoring.exporters._local.enabled", true);

            awaitCallApi("cluster.put_settings", emptyMap(),
                    singletonList(singletonMap("transient", settings)),
                    response -> {
                        Object acknowledged = response.evaluate("acknowledged");
                        return acknowledged != null && (Boolean) acknowledged;
                    },
                    () -> "Exception when enabling monitoring");
            awaitCallApi("search", singletonMap("index", ".monitoring-*"), emptyList(),
                    response -> ((Number) response.evaluate("hits.total")).intValue() > 0,
                    () -> "Exception when waiting for monitoring documents to be indexed");
        }
    }

    /**
     * Disable monitoring
     */
    @After
    public void disableMonitoring() throws Exception {
        if (isMonitoringTest()) {
            final Map<String, Object> settings = new HashMap<>();
            settings.put("xpack.monitoring.collection.interval", (String) null);
            settings.put("xpack.monitoring.exporters._local.enabled", (String) null);

            awaitCallApi("cluster.put_settings", emptyMap(),
                    singletonList(singletonMap("transient", settings)),
                    response -> {
                        Object acknowledged = response.evaluate("acknowledged");
                        return acknowledged != null && (Boolean) acknowledged;
                    },
                    () -> "Exception when disabling monitoring");

            // Now the local exporter is disabled, we try to check if the monitoring indices are
            // re created by an inflight bulk request. We try this 10 times or 10 seconds.
            final CountDown retries = new CountDown(10);
            awaitBusy(() -> {
                try {
                    Map<String, String> params = new HashMap<>();
                    params.put("index", ".monitoring-*");
                    params.put("allow_no_indices", "false");

                    ClientYamlTestResponse response =
                            callApi("indices.exists", params, emptyList());
                    if (response.getStatusCode() == HttpStatus.SC_OK) {
                        params = singletonMap("index", ".monitoring-*");
                        callApi("indices.delete", params, emptyList());
                        return false;
                    }
                } catch (ClientYamlTestResponseException e) {
                    ResponseException exception = e.getResponseException();
                    if (exception != null) {
                        Response response = exception.getResponse();
                        if (response != null) {
                            int responseCode = response.getStatusLine().getStatusCode();
                            if (responseCode == HttpStatus.SC_NOT_FOUND) {
                                return retries.countDown();
                            }
                        }
                    }
                    throw new ElasticsearchException("Failed to delete monitoring indices: ", e);
                } catch (IOException e) {
                    throw new ElasticsearchException("Failed to delete monitoring indices: ", e);
                }
                return retries.countDown();
            });
        }
    }

    /**
     * Executes an API call using the admin context, waiting for it to succeed.
     */
    private void awaitCallApi(String apiName,
                              Map<String, String> params,
                              List<Map<String, Object>> bodies,
                              CheckedFunction<ClientYamlTestResponse, Boolean, IOException> success,
                              Supplier<String> error) throws Exception {

        AtomicReference<IOException> exceptionHolder = new AtomicReference<>();
        awaitBusy(() -> {
            try {
                ClientYamlTestResponse response = callApi(apiName, params, bodies);
                if (response.getStatusCode() == HttpStatus.SC_OK) {
                    exceptionHolder.set(null);
                    return success.apply(response);
                }
                return false;
            } catch (IOException e) {
                exceptionHolder.set(e);
            }
            return false;
        });

        IOException exception = exceptionHolder.get();
        if (exception != null) {
            throw new IllegalStateException(error.get(), exception);
        }
    }

    private ClientYamlTestResponse callApi(String apiName,
                                           Map<String, String> params,
                                           List<Map<String, Object>> bodies) throws IOException {
        return getAdminExecutionContext().callApi(apiName, params, bodies, emptyMap());
    }

    private boolean isMonitoringTest() {
        String testName = getTestName();
        return testName != null && testName.contains("=monitoring/");
    }
}
