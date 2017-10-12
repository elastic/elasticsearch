/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import org.apache.http.HttpStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.xpack.ml.MachineLearningTemplateRegistry;
import org.elasticsearch.xpack.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
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
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/** Runs rest tests against external cluster */
public class XPackRestIT extends XPackRestTestCase {

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
        templates.addAll(Arrays.asList(WatcherIndexTemplateRegistry.TEMPLATE_NAMES));

        for (String template : templates) {
            awaitCallApi("indices.exists_template", singletonMap("name", template), emptyList(),
                    response -> true,
                    () -> "Exception when waiting for [" + template + "] template to be created");
        }

        // ensure watcher is started, so that a test can stop watcher and everything still works fine
        if (isWatcherTest()) {
            assertBusy(() -> {
                try {
                    ClientYamlTestResponse response =
                            getAdminExecutionContext().callApi("xpack.watcher.stats", emptyMap(), emptyList(), emptyMap());
                    String state = (String) response.evaluate("stats.0.watcher_state");
                    if ("started".equals(state) == false) {
                        getAdminExecutionContext().callApi("xpack.watcher.start", emptyMap(), emptyList(), emptyMap());
                    }
                    // assertion required to exit the assertBusy lambda
                    assertThat(state, is("started"));
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            });
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
            final ClientYamlTestResponse xpackUsage =
                    callApi("xpack.usage", singletonMap("filter_path", "monitoring.enabled_exporters"), emptyList());

            @SuppressWarnings("unchecked")
            final Map<String, Object> exporters = (Map<String, Object>) xpackUsage.evaluate("monitoring.enabled_exporters");
            assertNotNull("List of monitoring exporters must not be null", exporters);
            assertThat("List of enabled exporters must be empty before enabling monitoring",
                    XContentMapValues.extractRawValues("monitoring.enabled_exporters", exporters), hasSize(0));

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
     * Cleanup after tests.
     *
     * Feature-specific cleanup methods should be called from here rather than using
     * separate @After annotated methods to ensure there is a well-defined cleanup order.
     */
    @After
    public void cleanup() throws Exception {
        disableMonitoring();
        // This also generically waits for pending tasks to complete, so must go last (otherwise
        // it could be waiting for pending tasks while monitoring is still running).
        // TODO: consider moving the bit that waits for pending tasks into a general X-Pack component
        clearMlState();
    }

    /**
     * Disable monitoring
     */
    private void disableMonitoring() throws Exception {
        if (isMonitoringTest()) {
            final Map<String, Object> settings = new HashMap<>();
            settings.put("xpack.monitoring.collection.interval", null);
            settings.put("xpack.monitoring.exporters._local.enabled", null);

            awaitCallApi("cluster.put_settings", emptyMap(),
                    singletonList(singletonMap("transient", settings)),
                    response -> {
                        Object acknowledged = response.evaluate("acknowledged");
                        return acknowledged != null && (Boolean) acknowledged;
                    },
                    () -> "Exception when disabling monitoring");

            awaitBusy(() -> {
                try {
                    ClientYamlTestResponse response =
                            callApi("xpack.usage", singletonMap("filter_path", "monitoring.enabled_exporters"), emptyList());

                    @SuppressWarnings("unchecked")
                    final Map<String, ?> exporters = (Map<String, ?>) response.evaluate("monitoring.enabled_exporters");
                    if (exporters.isEmpty() == false) {
                        return false;
                    }

                    final Map<String, String> params = new HashMap<>();
                    params.put("node_id", "_local");
                    params.put("metric", "thread_pool");
                    params.put("filter_path", "nodes.*.thread_pool.bulk.active");
                    response = callApi("nodes.stats", params, emptyList());

                    @SuppressWarnings("unchecked")
                    final Map<String, Object> nodes = (Map<String, Object>) response.evaluate("nodes");
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();

                    @SuppressWarnings("unchecked")
                    final Number activeBulks = (Number) extractValue("thread_pool.bulk.active", node);
                    return activeBulks != null && activeBulks.longValue() == 0L;
                } catch (Exception e) {
                    throw new ElasticsearchException("Failed to wait for monitoring exporters to stop:", e);
                }
            });
        }
    }

    /**
     * Delete any left over machine learning datafeeds and jobs.
     *
     * Also waits for pending tasks to complete (which is not really an ML-specific
     * thing and could be moved into a general X-Pack method at some point).
     */
    private void clearMlState() throws Exception {
        if (isMachineLearningTest()) {
            new MlRestTestStateCleaner(logger, adminClient(), this).clearMlMetadata();
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
        return testName != null && (testName.contains("=monitoring/") || testName.contains("=monitoring\\"));
    }

    private boolean isWatcherTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("=watcher/") || testName.contains("=watcher\\"));
    }

    private boolean isMachineLearningTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("=ml/") || testName.contains("=ml\\"));
    }
}
