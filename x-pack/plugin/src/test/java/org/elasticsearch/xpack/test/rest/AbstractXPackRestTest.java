/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.test.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpStatus;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.plugins.MetadataUpgrader;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponseException;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.xpack.core.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;

/** Runs rest tests against external cluster */
// TODO: Remove this timeout increase once this test suite is broken up
@TimeoutSuite(millis = 60 * TimeUnits.MINUTE)
public abstract class AbstractXPackRestTest extends ESClientYamlSuiteTestCase {
    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    public AbstractXPackRestTest(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE).build();
    }

    @Before
    public void setupForTests() throws Exception {
        waitForTemplates();
    }

    /**
     * Waits for Machine Learning templates to be created by the {@link MetadataUpgrader}
     */
    private void waitForTemplates() {
        if (installTemplates()) {
            List<String> templates = Arrays.asList(
                NotificationsIndex.NOTIFICATIONS_INDEX,
                AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
                AnomalyDetectorsIndex.jobResultsIndexPrefix()
            );

            for (String template : templates) {
                awaitCallApi(
                    "indices.exists_index_template",
                    singletonMap("name", template),
                    emptyList(),
                    response -> true,
                    () -> "Exception when waiting for [" + template + "] template to be created"
                );
            }
        }
    }

    /**
     * Waits for the cluster's self-generated license to be created and installed
     */
    protected void waitForLicense() {
        // GET _licence returns a 404 status up until the license exists
        awaitCallApi(
            "license.get",
            Map.of(),
            List.of(),
            response -> true,
            () -> "Exception when waiting for initial license to be generated",
            30 // longer wait time to accommodate slow-running CI release builds
        );
    }

    /**
     * Cleanup after tests.
     *
     * Feature-specific cleanup methods should be called from here rather than using
     * separate @After annotated methods to ensure there is a well-defined cleanup order.
     */
    @After
    public void cleanup() throws Exception {
        clearMlState();
        if (isWaitForPendingTasks()) {
            // This waits for pending tasks to complete, so must go last (otherwise
            // it could be waiting for pending tasks while monitoring is still running).
            waitForPendingTasks(adminClient(), task -> {
                // Don't check rollup jobs because we clear them in the superclass.
                return task.contains(RollupJob.NAME);
            });
        }
    }

    /**
     * Delete any left over machine learning datafeeds and jobs.
     */
    private void clearMlState() throws Exception {
        if (isMachineLearningTest()) {
            new MlRestTestStateCleaner(logger, adminClient()).resetFeatures();
        }
    }

    /**
     * Executes an API call using the admin context, waiting for it to succeed.
     */
    private void awaitCallApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        CheckedFunction<ClientYamlTestResponse, Boolean, IOException> success,
        Supplier<String> error
    ) {
        awaitCallApi(apiName, params, bodies, success, error, 10);
    }

    private void awaitCallApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        CheckedFunction<ClientYamlTestResponse, Boolean, IOException> success,
        Supplier<String> error,
        long maxWaitTimeInSeconds
    ) {
        try {
            final AtomicReference<ClientYamlTestResponse> response = new AtomicReference<>();
            assertBusy(() -> {
                try {
                    // The actual method call that sends the API requests returns a Future, but we immediately
                    // call .get() on it so there's no need for this method to do any other awaiting.
                    response.set(callApi(apiName, params, bodies, getApiCallHeaders()));
                    assertEquals(HttpStatus.SC_OK, response.get().getStatusCode());
                } catch (ClientYamlTestResponseException e) {
                    // Convert to an AssertionError so that "assertBusy" treats it as a failed assertion (and tries again)
                    // rather than a runtime failure (which terminates the loop)
                    throw new AssertionError("Failed to call API " + apiName, e);
                }
            }, maxWaitTimeInSeconds, TimeUnit.SECONDS);
            success.apply(response.get());
        } catch (Exception e) {
            throw new IllegalStateException(error.get(), e);
        }
    }

    private ClientYamlTestResponse callApi(
        String apiName,
        Map<String, String> params,
        List<Map<String, Object>> bodies,
        Map<String, String> headers
    ) throws IOException {
        return getAdminExecutionContext().callApi(apiName, params, bodies, headers);
    }

    protected Map<String, String> getApiCallHeaders() {
        return Collections.emptyMap();
    }

    protected boolean installTemplates() {
        return true;
    }

    protected boolean isMachineLearningTest() {
        String testName = getTestName();
        return testName != null && (testName.contains("=ml/") || testName.contains("=ml\\"));
    }

    /**
     * Should each test wait for pending tasks to finish after execution?
     * @return Wait for pending tasks
     */
    protected boolean isWaitForPendingTasks() {
        return true;
    }

}
