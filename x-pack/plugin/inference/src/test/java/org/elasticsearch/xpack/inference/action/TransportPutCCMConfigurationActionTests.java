/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.CCMEnabledActionResponse;
import org.elasticsearch.xpack.core.inference.action.PutCCMConfigurationAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.xpack.inference.InferenceFeatures.INFERENCE_CCM_ENABLEMENT_SERVICE;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.action.TransportPutCCMConfigurationAction.FAILED_VALIDATION_MESSAGE;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_FORBIDDEN_EXCEPTION;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature.CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisRainbowSprinklesAuthorizationResponse;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPutCCMConfigurationActionTests extends ESTestCase {

    private static final SecureString API_KEY = new SecureString("secret".toCharArray());

    public static final String ERROR_MESSAGE = "error occurred";
    public static final String ERROR_RESPONSE = Strings.format("""
        {
            "error": "%s"
        }
        """, ERROR_MESSAGE);

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private CCMFeature ccmFeature;
    private HttpClientManager clientManager;
    private CCMService ccmService;
    private FeatureService featureService;
    private TransportPutCCMConfigurationAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        webServer.start();
        var webServerUrl = getUrl(webServer);

        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        ccmFeature = mock(CCMFeature.class);

        ccmService = mock(CCMService.class);
        doAnswer(invocation -> {
            ActionListener<Void> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return Void.TYPE;
        }).when(ccmService).storeConfiguration(any(), any());

        var settings = mock(ElasticInferenceServiceSettings.class);
        when(settings.getElasticInferenceServiceUrl()).thenReturn(webServerUrl);

        featureService = mock(FeatureService.class);

        action = new TransportPutCCMConfigurationAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            ccmService,
            mock(ProjectResolver.class),
            ccmFeature,
            senderFactory.createSender(),
            settings,
            featureService
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testUnsupportedCCMEnvironment_ReturnsError() {
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(false);

        var listener = new TestPlainActionFuture<CCMEnabledActionResponse>();
        action.masterOperation(
            mock(Task.class),
            new PutCCMConfigurationAction.Request(API_KEY, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(Exception.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception, is(CCM_FORBIDDEN_EXCEPTION));
    }

    public void testSuccessfulValidation() {
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);
        when(featureService.clusterHasFeature(any(), eq(INFERENCE_CCM_ENABLEMENT_SERVICE))).thenReturn(true);

        var rainbowSprinklesResponseBody = getEisRainbowSprinklesAuthorizationResponse("url").responseJson();
        webServer.enqueue(new MockResponse().setResponseCode(RestStatus.OK.getStatus()).setBody(rainbowSprinklesResponseBody));

        var listener = new TestPlainActionFuture<CCMEnabledActionResponse>();
        action.masterOperation(
            mock(Task.class),
            new PutCCMConfigurationAction.Request(API_KEY, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS),
            ClusterState.EMPTY_STATE,
            listener
        );

        var response = listener.actionGet(TimeValue.THIRTY_SECONDS);
        assertTrue(response.isEnabled());
    }

    public void testValidationFailure401() {
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);
        when(featureService.clusterHasFeature(any(), eq(INFERENCE_CCM_ENABLEMENT_SERVICE))).thenReturn(true);
        webServer.enqueue(new MockResponse().setResponseCode(RestStatus.UNAUTHORIZED.getStatus()).setBody(ERROR_RESPONSE));

        var listener = new TestPlainActionFuture<CCMEnabledActionResponse>();
        action.masterOperation(
            mock(Task.class),
            new PutCCMConfigurationAction.Request(API_KEY, TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS),
            ClusterState.EMPTY_STATE,
            listener
        );

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));
        assertThat(exception.getMessage(), containsString(FAILED_VALIDATION_MESSAGE));
        assertThat(exception.status(), is(RestStatus.UNAUTHORIZED));
        verify(ccmService, never()).storeConfiguration(any(), any());
    }

    public void testEnablementService_NotSupported() {
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(true);
        when(featureService.clusterHasFeature(any(), eq(INFERENCE_CCM_ENABLEMENT_SERVICE))).thenReturn(false);

        var listener = new TestPlainActionFuture<CCMEnabledActionResponse>();
        action.masterOperation(null, null, ClusterState.EMPTY_STATE, listener);
        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));

        assertThat(exception, is(CCM_UNSUPPORTED_UNTIL_UPGRADED_EXCEPTION));
    }
}
