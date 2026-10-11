/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.UpdateInferenceModelAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.EmbeddingFloatResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.SenderService;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.inference.Utils.getPersistedConfigMap;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Transport-level secret-rotation tests for {@link TransportUpdateInferenceModelAction}.
 *
 * <p>Each parameterized case builds a real service instance, registers a persisted model, then calls
 * {@code masterOperation} with an update body that contains only rotated secret fields inside
 * {@code service_settings}.  The test asserts that:
 * <ol>
 *   <li>The request succeeds (no {@code unknown field} exception).</li>
 *   <li>{@code modelRegistry.updateModelTransaction} is invoked with the rotated secrets.</li>
 * </ol>
 *
 * <p>Before the fix in this PR, services converted to {@code ObjectParser}-based settings would reject
 * secret keys as unknown fields and the future would complete with an exception.
 *
 * <h2>When to add a new entry in {@link SecretRotationTestCases}</h2>
 * <ul>
 *   <li>A <em>new service</em> is added that has secrets.</li>
 *   <li>A <em>new task type</em> is added to an existing service — each task type has its own
 *       {@code ServiceSettings} class and therefore its own update parser.</li>
 * </ul>
 * Converting an already-listed service to {@code ObjectParser} does <em>not</em> require a new entry:
 * the existing rows already drive the new parser and will fail if secrets are not declared.
 */
public class TransportUpdateInferenceModelActionSecretRotationTests extends ESTestCase {

    /**
     * Descriptor for one parameterized test case.
     *
     * @param serviceName        value returned by {@link org.elasticsearch.inference.InferenceService#name()}
     * @param taskType           the task type of the endpoint being updated
     * @param serviceFactory     creates a real service (may be spied upon in the harness)
     * @param persistedServiceSettings non-secret service settings stored in {@code .inference}
     * @param persistedTaskSettings    task settings stored in {@code .inference}
     * @param initialSecretSettings    secret settings stored in {@code .inference-secrets} (the "old" credential)
     * @param rotatedSecretSettings    the new credential supplied in the update body's {@code service_settings}
     */
    public record TestCase(
        String serviceName,
        TaskType taskType,
        BiFunction<ThreadPool, HttpClientManager, SenderService<?>> serviceFactory,
        Map<String, Object> persistedServiceSettings,
        Map<String, Object> persistedTaskSettings,
        Map<String, Object> initialSecretSettings,
        Map<String, Object> rotatedSecretSettings
    ) {
        @Override
        public String toString() {
            return serviceName + "/" + taskType;
        }
    }

    private static final String INFERENCE_ENTITY_ID = "test-inference-entity-id";

    private ThreadPool threadPool;
    private HttpClientManager clientManager;
    private final TestCase testCase;

    public TransportUpdateInferenceModelActionSecretRotationTests(TestCase testCase) {
        this.testCase = testCase;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return SecretRotationTestCases.all().stream().map(tc -> new Object[] { tc }).toList();
    }

    @Before
    public void startHttpClient() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdownHttpClient() throws Exception {
        clientManager.close();
        terminate(threadPool);
    }

    public void testMasterOperation_SecretRotation_Succeeds() throws Exception {
        // Build a real service and spy on it so we can stub the network-touching validation entry
        // points while keeping all parsing/building logic real.
        var realService = testCase.serviceFactory().apply(threadPool, clientManager);
        var spyService = spy(realService);
        stubValidationEntryPoints(spyService);

        var mockModelRegistry = mock(ModelRegistry.class);
        // SenderService.parsePersistedConfig mutates the config and secrets maps (removes service_settings,
        // secret_settings, and individual secret keys). Return fresh UnparsedModel instances built from
        // independent copies of the leaf-level maps on every call.
        mockGetModelWithSecrets(
            mockModelRegistry,
            testCase.taskType(),
            testCase.serviceName(),
            testCase.persistedServiceSettings(),
            testCase.persistedTaskSettings(),
            testCase.initialSecretSettings()
        );
        mockGetModel(
            mockModelRegistry,
            testCase.taskType(),
            testCase.serviceName(),
            testCase.persistedServiceSettings(),
            testCase.persistedTaskSettings(),
            testCase.initialSecretSettings()
        );

        // Capture the model passed to updateModelTransaction so we can assert on its secrets.
        var capturedNewModel = ArgumentCaptor.forClass(Model.class);
        doAnswer(inv -> {
            ActionListener<Boolean> listener = inv.getArgument(2);
            listener.onResponse(true);
            return null;
        }).when(mockModelRegistry).updateModelTransaction(capturedNewModel.capture(), any(), any());

        var mockServiceRegistry = mock(InferenceServiceRegistry.class);
        when(mockServiceRegistry.getService(testCase.serviceName())).thenReturn(Optional.of(spyService));

        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(any(LicensedFeature.class))).thenReturn(true);

        var action = buildAction(licenseState, mockModelRegistry, mockServiceRegistry);

        var updateBody = buildUpdateBody(testCase.rotatedSecretSettings());
        var future = callMasterOperation(action, testCase.taskType(), updateBody);

        future.actionGet(TEST_REQUEST_TIMEOUT);

        verify(mockModelRegistry).updateModelTransaction(any(), any(), any());
        var persistedModel = capturedNewModel.getValue();
        // The secret settings must reflect the rotated credential.
        assertSecretSettingsRotated(persistedModel, testCase.rotatedSecretSettings());
        // The service settings must be unchanged (rotating a secret must not perturb config).
        assertThat(persistedModel.getConfigurations().getService(), is(testCase.serviceName()));
        assertThat(persistedModel.getConfigurations().getTaskType(), is(testCase.taskType()));
    }

    public void testMasterOperation_GenuinelyUnknownServiceSetting_IsRejected() throws IOException {
        var realService = testCase.serviceFactory().apply(threadPool, clientManager);
        var spyService = spy(realService);
        stubValidationEntryPoints(spyService);

        var mockModelRegistry = mock(ModelRegistry.class);
        mockGetModelWithSecrets(
            mockModelRegistry,
            testCase.taskType(),
            testCase.serviceName(),
            testCase.persistedServiceSettings(),
            testCase.persistedTaskSettings(),
            testCase.initialSecretSettings()
        );
        mockGetModel(
            mockModelRegistry,
            testCase.taskType(),
            testCase.serviceName(),
            testCase.persistedServiceSettings(),
            testCase.persistedTaskSettings(),
            testCase.initialSecretSettings()
        );
        doAnswer(inv -> {
            ActionListener<Boolean> listener = inv.getArgument(2);
            listener.onResponse(true);
            return null;
        }).when(mockModelRegistry).updateModelTransaction(any(), any(), any());

        var mockServiceRegistry = mock(InferenceServiceRegistry.class);
        when(mockServiceRegistry.getService(testCase.serviceName())).thenReturn(Optional.of(spyService));

        var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(any(LicensedFeature.class))).thenReturn(true);

        var action = buildAction(licenseState, mockModelRegistry, mockServiceRegistry);

        // A field that no service declares — this must still be rejected.
        var updateBody = buildUpdateBody(Map.of("this_field_absolutely_does_not_exist", "value"));
        var future = callMasterOperation(action, testCase.taskType(), updateBody);

        var ex = expectThrows(Exception.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(ex.getMessage(), containsString("this_field_absolutely_does_not_exist"));
    }

    private TransportUpdateInferenceModelAction buildAction(
        MockLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
    ) {
        return new TransportUpdateInferenceModelAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            licenseState,
            modelRegistry,
            serviceRegistry,
            mock(Client.class),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
    }

    private static String buildUpdateBody(Map<String, Object> serviceSettingsFields) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(ModelConfigurations.SERVICE_SETTINGS, serviceSettingsFields);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private TestPlainActionFuture<UpdateInferenceModelAction.Response> callMasterOperation(
        TransportUpdateInferenceModelAction action,
        TaskType taskType,
        String requestBody
    ) {
        var future = new TestPlainActionFuture<UpdateInferenceModelAction.Response>();
        action.masterOperation(
            mock(Task.class),
            new UpdateInferenceModelAction.Request(
                INFERENCE_ENTITY_ID,
                new BytesArray(requestBody),
                XContentType.JSON,
                taskType,
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT
            ),
            ClusterState.EMPTY_STATE,
            future
        );
        return future;
    }

    /**
     * Stubs all service methods that contact external systems, so the update validation step does not
     * actually make HTTP calls.  Every entry point returns a minimal but valid result for its task type.
     */
    private static void stubValidationEntryPoints(SenderService<?> spyService) {
        // Stub onModelUpdated — Mockito does not invoke interface default methods.
        doAnswer(inv -> {
            ActionListener<Void> listener = inv.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(spyService).onModelUpdated(any(), any(), any());

        // Stub the infer/unifiedCompletionInfer/rerankInfer/embeddingInfer entry points.
        // Each validator routes to exactly one of these; we stub all so the test is robust to
        // service-level getServiceIntegrationValidator() overrides.
        doAnswer(inv -> {
            ActionListener<InferenceServiceResults> listener = inv.getArgument(6);
            listener.onResponse(
                new DenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.2f, 0.3f })))
            );
            return null;
        }).when(spyService).infer(any(), any(), any(Boolean.class), any(), any(), any(), any());

        doAnswer(inv -> {
            ActionListener<InferenceServiceResults> listener = inv.getArgument(3);
            listener.onResponse(
                new DenseEmbeddingFloatResults(List.of(new EmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.2f, 0.3f })))
            );
            return null;
        }).when(spyService).embeddingInfer(any(), any(), any(), any());

        doAnswer(inv -> {
            ActionListener<InferenceServiceResults> listener = inv.getArgument(3);
            listener.onResponse(new RankedDocsResults(List.of(new RankedDocsResults.RankedDoc(0, 1.0f, "test"))));
            return null;
        }).when(spyService).rerankInfer(any(), any(), any(), any());

        doAnswer(inv -> {
            ActionListener<InferenceServiceResults> listener = inv.getArgument(3);
            listener.onResponse(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("hello"))));
            return null;
        }).when(spyService).unifiedCompletionInfer(any(), any(), any(), any());

        // updateModelWithEmbeddingDetails: return the model unchanged so we can assert on its secrets.
        // Use doAnswer (not when/thenAnswer) because spying calls the real method before the stub takes effect.
        doAnswer(inv -> inv.getArgument(0)).when(spyService).updateModelWithEmbeddingDetails(any(), any(Integer.class));
    }

    /**
     * Stubs {@code getModelWithSecrets} and {@code getModel} to each return a fresh {@link UnparsedModel} on
     * every call, built from independent copies of the leaf-level maps.
     *
     * <p>{@link org.elasticsearch.xpack.inference.services.SenderService#parsePersistedConfig} mutates both the
     * top-level config/secrets maps (removes {@code service_settings} and {@code secret_settings}) and the
     * nested maps (removes individual setting keys such as {@code api_key}).  Reusing the same object across
     * calls would therefore cause the second parse to see empty maps.
     */
    private static void mockGetModelWithSecrets(
        ModelRegistry registry,
        TaskType taskType,
        String serviceName,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {
        doAnswer(inv -> {
            ActionListener<UnparsedModel> listener = inv.getArgument(1);
            var freshConfig = getPersistedConfigMap(
                new HashMap<>(serviceSettings),
                new HashMap<>(taskSettings),
                new HashMap<>(secretSettings)
            );
            listener.onResponse(new UnparsedModel(INFERENCE_ENTITY_ID, taskType, serviceName, freshConfig.config(), freshConfig.secrets()));
            return null;
        }).when(registry).getModelWithSecrets(eq(INFERENCE_ENTITY_ID), any());
    }

    /**
     * Stubs {@code getModel} (re-fetch after persist) to return a fresh {@link UnparsedModel} built from the
     * original persisted config maps.  The re-fetch result is parsed again to produce the response, so it must
     * contain the full config.
     */
    private static void mockGetModel(
        ModelRegistry registry,
        TaskType taskType,
        String serviceName,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings
    ) {
        doAnswer(inv -> {
            ActionListener<UnparsedModel> listener = inv.getArgument(1);
            var freshConfig = getPersistedConfigMap(
                new HashMap<>(serviceSettings),
                new HashMap<>(taskSettings),
                new HashMap<>(secretSettings)
            );
            listener.onResponse(new UnparsedModel(INFERENCE_ENTITY_ID, taskType, serviceName, freshConfig.config(), freshConfig.secrets()));
            return null;
        }).when(registry).getModel(eq(INFERENCE_ENTITY_ID), any());
    }

    /**
     * Asserts that the persisted model's secret settings reflect the rotated values.
     *
     * <p>We match by checking that the string representation of each secret field equals the value
     * in the rotation map.  This avoids coupling the assertion to a specific {@code SecretSettings}
     * implementation while still exercising the full parse+equality path.
     */
    private static void assertSecretSettingsRotated(Model model, Map<String, Object> rotatedSecretSettings) {
        var secretSettings = model.getSecretSettings();
        assertNotNull(secretSettings);

        // For DefaultSecretSettings (api_key): the model must carry the rotated api_key.
        if (rotatedSecretSettings.containsKey(DefaultSecretSettings.API_KEY)) {
            assertThat(secretSettings, instanceOf(DefaultSecretSettings.class));
            var rotated = (DefaultSecretSettings) secretSettings;
            assertThat(rotated.apiKey().toString(), is(rotatedSecretSettings.get(DefaultSecretSettings.API_KEY).toString()));
        }

        // For AwsSecretSettings (access_key + secret_key):
        if (rotatedSecretSettings.containsKey(AmazonBedrockConstants.ACCESS_KEY_FIELD)) {
            var rotated = (AwsSecretSettings) secretSettings;
            assertThat(rotated.accessKey().toString(), is(rotatedSecretSettings.get(AmazonBedrockConstants.ACCESS_KEY_FIELD).toString()));
            assertThat(rotated.secretKey().toString(), is(rotatedSecretSettings.get(AmazonBedrockConstants.SECRET_KEY_FIELD).toString()));
        }
    }
}
