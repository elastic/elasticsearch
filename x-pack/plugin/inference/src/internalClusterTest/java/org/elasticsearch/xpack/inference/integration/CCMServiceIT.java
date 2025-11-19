/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.AUTH_TASK_ACTION;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.assertChatCompletionEndpointExists;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.getEisEndpoints;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.removeEisPreconfiguredEndpoints;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.waitForAuthorizationToComplete;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.waitForNoTask;
import static org.elasticsearch.xpack.inference.integration.AuthorizationTaskExecutorIT.waitForTask;
import static org.elasticsearch.xpack.inference.integration.ModelRegistryIT.buildElserModelConfig;
import static org.elasticsearch.xpack.inference.registry.ModelRegistryTests.assertStoreModel;
import static org.hamcrest.Matchers.empty;

public class CCMServiceIT extends CCMSingleNodeIT {
    private static final AtomicReference<CCMService> ccmService = new AtomicReference<>();

    private static final MockWebServer webServer = new MockWebServer();
    private static String gatewayUrl;

    private AuthorizationTaskExecutor authorizationTaskExecutor;
    private ModelRegistry modelRegistry;

    public CCMServiceIT() {
        super(new Provider() {
            @Override
            public void store(CCMModel ccmModel, ActionListener<Void> listener) {
                ccmService.get().storeConfiguration(ccmModel, listener);
            }

            @Override
            public void get(ActionListener<CCMModel> listener) {
                ccmService.get().getConfiguration(listener);
            }

            @Override
            public void delete(ActionListener<Void> listener) {
                ccmService.get().disableCCM(listener);
            }
        });
    }

    @BeforeClass
    public static void initClass() throws IOException {
        webServer.start();
        gatewayUrl = getUrl(webServer);
    }

    @Before
    public void createComponents() {
        ccmService.set(node().injector().getInstance(CCMService.class));
        modelRegistry = node().injector().getInstance(ModelRegistry.class);
        authorizationTaskExecutor = node().injector().getInstance(AuthorizationTaskExecutor.class);
    }

    @After
    public void shutdown() {
        // disable CCM to clean up any stored configuration
        disableCCM();

        removeEisPreconfiguredEndpoints(modelRegistry);
    }

    private void disableCCM() {
        var listener = new PlainActionFuture<Void>();
        ccmService.get().disableCCM(listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);
    }

    @AfterClass
    public static void cleanUpClass() {
        webServer.close();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(CCMSettings.CCM_SUPPORTED_ENVIRONMENT.getKey(), true)
            .put(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), gatewayUrl)
            // Ensure that the polling logic only occurs once so we can deterministically control when an authorization response is
            // received
            .put(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), false)
            .build();
    }

    public void testIsEnabled_ReturnsFalse_WhenNoCCMConfigurationStored() {
        var listener = new PlainActionFuture<Boolean>();
        ccmService.get().isEnabled(listener);

        assertFalse(listener.actionGet(TimeValue.THIRTY_SECONDS));
    }

    public void testIsEnabled_ReturnsTrue_WhenCCMConfigurationIsPresent() {
        assertStoreCCMConfiguration();

        var listener = new PlainActionFuture<Boolean>();
        ccmService.get().isEnabled(listener);

        assertTrue(listener.actionGet(TimeValue.THIRTY_SECONDS));
    }

    public void testCreatesEisChatCompletionEndpoint() throws Exception {
        disableCCM();
        waitForNoTask(AUTH_TASK_ACTION, admin());

        var eisEndpoints = getEisEndpoints(modelRegistry);
        assertThat(eisEndpoints, empty());

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(AUTHORIZED_RAINBOW_SPRINKLES_RESPONSE));
        var listener = new TestPlainActionFuture<Void>();
        ccmService.get().storeConfiguration(new CCMModel(new SecureString("secret".toCharArray())), listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        // Force a cluster state update to ensure the authorization task is created
        forceClusterUpdate();

        waitForTask(AUTH_TASK_ACTION, admin());
        waitForAuthorizationToComplete(authorizationTaskExecutor);

        assertChatCompletionEndpointExists(modelRegistry);
    }

    private void forceClusterUpdate() {
        var model = buildElserModelConfig("test-store-model", TaskType.SPARSE_EMBEDDING);
        assertStoreModel(modelRegistry, model);
    }

    public void testDisableCCM_RemovesAuthorizationTask() throws Exception {
        disableCCM();
        waitForNoTask(AUTH_TASK_ACTION, admin());

        var listener = new TestPlainActionFuture<Void>();
        ccmService.get().storeConfiguration(new CCMModel(new SecureString("secret".toCharArray())), listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        // Force a cluster state update to ensure the authorization task is created
        forceClusterUpdate();

        waitForTask(AUTH_TASK_ACTION, admin());
        waitForAuthorizationToComplete(authorizationTaskExecutor);

        disableCCM();
        waitForNoTask(AUTH_TASK_ACTION, admin());
    }
}
