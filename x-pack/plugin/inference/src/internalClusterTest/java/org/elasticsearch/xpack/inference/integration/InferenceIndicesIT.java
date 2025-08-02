/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class InferenceIndicesIT extends ESIntegTestCase {

    private static final String INDEX_ROUTER_ATTRIBUTE = "node.attr.index_router";
    private static final String CONFIG_ROUTER = "config";
    private static final String SECRETS_ROUTER = "secrets";

    private static final Map<String, Object> BBQ_COMPATIBLE_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    private final Map<String, TaskType> inferenceIds = new HashMap<>();

    public static class LocalStateIndexSettingsInferencePlugin extends LocalStateCompositeXPackPlugin {
        private final InferencePlugin inferencePlugin;

        public LocalStateIndexSettingsInferencePlugin(final Settings settings, final Path configPath) throws Exception {
            super(settings, configPath);
            var thisVar = this;
            this.inferencePlugin = new InferencePlugin(settings) {
                @Override
                protected SSLService getSslService() {
                    return thisVar.getSslService();
                }

                @Override
                protected XPackLicenseState getLicenseState() {
                    return thisVar.getLicenseState();
                }

                @Override
                public List<InferenceServiceExtension.Factory> getInferenceServiceFactories() {
                    return List.of(
                        TestSparseInferenceServiceExtension.TestInferenceService::new,
                        TestDenseInferenceServiceExtension.TestInferenceService::new
                    );
                }

                @Override
                public Settings getIndexSettings() {
                    return InferenceIndex.settingsForTesting(
                        Settings.builder().put("index.routing.allocation.require.index_router", "config").build()
                    );
                }

                @Override
                public Settings getSecretsIndexSettings() {
                    return InferenceSecretsIndex.settingsForTesting(
                        Settings.builder().put("index.routing.allocation.require.index_router", "secrets").build()
                    );
                }
            };
            plugins.add(inferencePlugin);
        }

    }

    @After
    public void cleanUp() {
        // for (var entry : inferenceIds.entrySet()) {
        // assertAcked(
        // safeGet(
        // client().execute(
        // DeleteInferenceEndpointAction.INSTANCE,
        // new DeleteInferenceEndpointAction.Request(entry.getKey(), entry.getValue(), true, false)
        // )
        // )
        // );
        // }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateIndexSettingsInferencePlugin.class, TestInferenceServicePlugin.class);
    }

    public void testRetrievingInferenceEndpoint_ThrowsException_WhenIndexNodeIsNotAvailable() throws Exception {
        final Settings configIndexNodeAttributes = Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, CONFIG_ROUTER).build();

        internalCluster().startMasterOnlyNode(configIndexNodeAttributes);
        final String configIndexDataNodes = internalCluster().startDataOnlyNode(configIndexNodeAttributes);

        internalCluster().startDataOnlyNode(Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, SECRETS_ROUTER).build());

        final String inferenceId = "test-index-id";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BBQ_COMPATIBLE_SERVICE_SETTINGS);

        // Ensure the inference indices are created and we can retrieve the inference endpoint
        GetInferenceModelAction.Request getInferenceEndpointRequest = new GetInferenceModelAction.Request(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            true
        );
        var responseFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getEndpoints().get(0).getInferenceEntityId(), equalTo(inferenceId));

        // stop the node that holds the inference index
        internalCluster().stopNode(configIndexDataNodes);

        var responseFailureFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        var exception = expectThrows(SearchPhaseExecutionException.class, () -> responseFailureFuture.actionGet(TEST_REQUEST_TIMEOUT));

        assertThat(exception.toString(), containsString("all shards failed"));
        assertThat(exception.toString(), containsString("Node not connected"));
        assertThat(exception.toString(), containsString(".inference"));
    }

    public void testRetrievingInferenceEndpoint_ThrowsException_WhenSecretsIndexNodeIsNotAvailable() throws Exception {
        final Settings configIndexNodeAttributes = Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, CONFIG_ROUTER).build();
        internalCluster().startMasterOnlyNode(configIndexNodeAttributes);
        internalCluster().startDataOnlyNode(configIndexNodeAttributes);

        String secretIndexDataNodes = internalCluster().startDataOnlyNode(
            Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, SECRETS_ROUTER).build()
        );

        final String inferenceId = "test-secrets-index-id";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, BBQ_COMPATIBLE_SERVICE_SETTINGS);

        // Ensure the inference indices are created and we can retrieve the inference endpoint
        GetInferenceModelAction.Request getInferenceEndpointRequest = new GetInferenceModelAction.Request(
            inferenceId,
            TaskType.TEXT_EMBEDDING,
            true
        );
        var responseFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getEndpoints().get(0).getInferenceEntityId(), equalTo(inferenceId));

        // stop the node that holds the inference secrets index
        internalCluster().stopNode(secretIndexDataNodes);

        // We should not be able to create a new inference endpoint because the secrets index is not available
        final String inferenceIdFailing = "test-secrets-index-id2";
        var responseFailureFuture = createInferenceEndpointAsync(
            TaskType.TEXT_EMBEDDING,
            inferenceIdFailing,
            BBQ_COMPATIBLE_SERVICE_SETTINGS
        );
        var exception = expectThrows(SearchPhaseExecutionException.class, () -> responseFailureFuture.actionGet(TEST_REQUEST_TIMEOUT));

        assertThat(exception.toString(), containsString("all shards failed"));
        assertThat(exception.toString(), containsString("Node not connected"));
        assertThat(exception.toString(), containsString(".inference-secrets"));
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        var responseFuture = createInferenceEndpointAsync(taskType, inferenceId, serviceSettings);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));

        inferenceIds.put(inferenceId, taskType);
    }

    private ActionFuture<PutInferenceModelAction.Response> createInferenceEndpointAsync(
        TaskType taskType,
        String inferenceId,
        Map<String, Object> serviceSettings
    ) throws IOException {
        final String service = switch (taskType) {
            case TEXT_EMBEDDING -> TestDenseInferenceServiceExtension.TestInferenceService.NAME;
            case SPARSE_EMBEDDING -> TestSparseInferenceServiceExtension.TestInferenceService.NAME;
            default -> throw new IllegalArgumentException("Unhandled task type [" + taskType + "]");
        };

        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", service);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        PutInferenceModelAction.Request request = new PutInferenceModelAction.Request(
            taskType,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );

        return client().execute(PutInferenceModelAction.INSTANCE, request);
    }
}
