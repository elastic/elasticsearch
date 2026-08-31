/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.CircuitBreakerPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferenceIndexMappingManager;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.InferenceSecretsIndex;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class InferenceIndicesIT extends ESIntegTestCase {

    private static final String INDEX_ROUTER_ATTRIBUTE = "node.attr.index_router";
    private static final String CONFIG_ROUTER = "config";
    private static final String SECRETS_ROUTER = "secrets";

    private static final Map<String, Object> TEST_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    public static class LocalStateIndexSettingsInferencePlugin extends LocalStateCompositeXPackPlugin implements CircuitBreakerPlugin {
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
                    return InferenceIndex.builder()
                        .put(Settings.builder().put("index.routing.allocation.require.index_router", "config").build())
                        .build();
                }

                @Override
                public Settings getSecretsIndexSettings() {
                    return InferenceSecretsIndex.builder()
                        .put(Settings.builder().put("index.routing.allocation.require.index_router", "secrets").build())
                        .build();
                }

                @Override
                public void loadExtensions(ExtensionLoader loader) {
                    // nothing, else it would clash with super class which already loads inference services
                }
            };
            plugins.add(inferencePlugin);
        }

        @Override
        public BreakerSettings getCircuitBreaker(Settings settings) {
            return inferencePlugin.getCircuitBreaker(settings);
        }

        @Override
        public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
            inferencePlugin.setCircuitBreaker(circuitBreaker);
        }
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
        final var configIndexNodeAttributes = Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, CONFIG_ROUTER).build();

        internalCluster().startMasterOnlyNode(configIndexNodeAttributes);
        final var configIndexDataNodes = internalCluster().startDataOnlyNode(configIndexNodeAttributes);

        internalCluster().startDataOnlyNode(Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, SECRETS_ROUTER).build());

        final var inferenceId = "test-index-id";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, TEST_SERVICE_SETTINGS);

        // Ensure the inference indices are created and we can retrieve the inference endpoint
        var getInferenceEndpointRequest = new GetInferenceModelAction.Request(inferenceId, TaskType.TEXT_EMBEDDING, true);
        var responseFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getEndpoints().get(0).getInferenceEntityId(), equalTo(inferenceId));

        // stop the node that holds the inference index
        internalCluster().stopNode(configIndexDataNodes);

        var responseFailureFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        var exception = expectThrows(ElasticsearchException.class, () -> responseFailureFuture.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.toString(), containsString("Failed to load inference endpoint [test-index-id]"));

        var causeException = exception.getCause();
        assertThat(causeException, instanceOf(SearchPhaseExecutionException.class));
    }

    public void testRetrievingInferenceEndpoint_ThrowsException_WhenIndexNodeIsNotAvailable_ForInferenceAction() throws Exception {
        final var configIndexNodeAttributes = Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, CONFIG_ROUTER).build();

        internalCluster().startMasterOnlyNode(configIndexNodeAttributes);
        final var configIndexDataNodes = internalCluster().startDataOnlyNode(configIndexNodeAttributes);

        internalCluster().startDataOnlyNode(Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, SECRETS_ROUTER).build());

        final var inferenceId = "test-index-id-2";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, TEST_SERVICE_SETTINGS);

        // Ensure the inference indices are created and we can retrieve the inference endpoint
        var getInferenceEndpointRequest = new GetInferenceModelAction.Request(inferenceId, TaskType.TEXT_EMBEDDING, true);
        var responseFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getEndpoints().get(0).getInferenceEntityId(), equalTo(inferenceId));

        // stop the node that holds the inference index
        internalCluster().stopNode(configIndexDataNodes);

        var proxyResponse = sendInferenceProxyRequest(inferenceId);
        var exception = expectThrows(ElasticsearchException.class, () -> proxyResponse.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.toString(), containsString("Failed to load inference endpoint with secrets [test-index-id-2]"));

        var causeException = exception.getCause();
        assertThat(causeException, instanceOf(SearchPhaseExecutionException.class));
    }

    public void testRetrievingInferenceEndpoint_ThrowsException_WhenSecretsIndexNodeIsNotAvailable() throws Exception {
        final var configIndexNodeAttributes = Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, CONFIG_ROUTER).build();
        internalCluster().startMasterOnlyNode(configIndexNodeAttributes);
        internalCluster().startDataOnlyNode(configIndexNodeAttributes);

        var secretIndexDataNodes = internalCluster().startDataOnlyNode(
            Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, SECRETS_ROUTER).build()
        );

        final var inferenceId = "test-secrets-index-id";
        createInferenceEndpoint(TaskType.TEXT_EMBEDDING, inferenceId, TEST_SERVICE_SETTINGS);

        // Ensure the inference indices are created and we can retrieve the inference endpoint
        var getInferenceEndpointRequest = new GetInferenceModelAction.Request(inferenceId, TaskType.TEXT_EMBEDDING, true);
        var responseFuture = client().execute(GetInferenceModelAction.INSTANCE, getInferenceEndpointRequest);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getEndpoints().get(0).getInferenceEntityId(), equalTo(inferenceId));

        // stop the node that holds the inference secrets index
        internalCluster().stopNode(secretIndexDataNodes);

        var proxyResponse = sendInferenceProxyRequest(inferenceId);

        var exception = expectThrows(ElasticsearchException.class, () -> proxyResponse.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.toString(), containsString("Failed to load inference endpoint with secrets [test-secrets-index-id]"));

        var causeException = exception.getCause();

        assertThat(causeException, instanceOf(SearchPhaseExecutionException.class));
    }

    /**
     * Verifies the end-to-end behaviour of {@link InferenceIndexMappingManager#withUpToDateMappings} for the
     * stale-mapping upgrade scenario that arises during a rolling upgrade.
     *
     * <p>A live outdated cluster state cannot be captured reliably: in a single-version test cluster
     * the index is created with current mappings right away. Instead, a synthetic {@link ClusterState}
     * is built that reports the index as having outdated mappings while the real cluster index is
     * already up to date. This lets us exercise the upgrade code path in {@code withUpToDateMappings}
     * without a timing dependency:
     *
     * <ol>
     *   <li>Create {@code .inference} on demand through {@code withUpToDateMappings} (the index is
     *       not created automatically).</li>
     *   <li>Build a synthetic cluster state that reports {@code .inference} with outdated mappings.</li>
     *   <li>{@code withUpToDateMappings} detects the outdated version in the provided state and issues
     *       a {@code PutMapping} request. The {@code PutMapping} is idempotent because the real index
     *       is already up to date; it succeeds and the listener is called.</li>
     *   <li>The index accepts documents with the {@code doc_type} field without a
     *       {@code strict_dynamic_mapping_exception}.</li>
     * </ol>
     */
    public void testWithUpToDateMappings_upgradesStaleMappingsAndAllowsV4Fields() throws Exception {
        var setup = startNodesAndCreateInferenceIndex();

        // Build a synthetic cluster state that reports .inference at outdated mappings. This simulates
        // the cluster state a node would see during a rolling upgrade before migration completes.
        // SystemIndexMappingUpdateService only updates mappings for existing indices; using a synthetic
        // state avoids any race with it and makes the test deterministic.
        ClusterState syntheticOutdatedState = buildSyntheticV3ClusterState();

        // withUpToDateMappings reads the outdated version from the provided state, detects that it is
        // below the current version, and issues a PutMapping to upgrade the index. The PutMapping is
        // idempotent because the real index is already up to date; it succeeds and the listener is called.
        var upgradeFuture = new PlainActionFuture<Void>();
        setup.manager().withUpToDateMappings(syntheticOutdatedState, upgradeFuture);
        upgradeFuture.actionGet(TEST_REQUEST_TIMEOUT);

        // The real cluster must report the current mapping version after the upgrade call completes.
        var indexMeta = setup.clusterService().state().metadata().getProject().index(InferenceIndex.INDEX_NAME);
        assertNotNull("Expected .inference to exist in cluster state", indexMeta);
        @SuppressWarnings("unchecked")
        var meta = (Map<String, Object>) indexMeta.mapping().sourceAsMap().get("_meta");
        // Compare against the descriptor actually registered with the node rather than rebuilding one:
        // this test's plugin overrides the index settings, so a rebuilt descriptor would not match it.
        var registeredDescriptor = internalCluster().getCurrentMasterNodeInstance(SystemIndices.class)
            .findMatchingDescriptor(InferenceIndex.INDEX_NAME);
        assertThat(
            "Expected managed_index_mappings_version to be current after withUpToDateMappings completed",
            meta.get(SystemIndexDescriptor.VERSION_META_KEY),
            equalTo(registeredDescriptor.getMappingsVersion().version())
        );

        // Verify that a document containing the doc_type field can be indexed without a
        // strict_dynamic_mapping_exception. With outdated mappings (dynamic: strict, no doc_type field)
        // this would fail; with current mappings it must succeed.
        new OriginSettingClient(client(), ClientHelper.INFERENCE_ORIGIN).index(
            new IndexRequest(InferenceIndex.INDEX_NAME).source(
                Map.of("doc_type", "model", "model_id", "test-upgrade-model", "task_type", "text_embedding", "service", "test-service")
            )
        ).actionGet();
    }

    /**
     * Verifies that {@link InferenceIndexMappingManager#withUpToDateMappings} completes when the
     * provided cluster state already shows {@code .inference} with current mappings, and that
     * documents containing the {@code doc_type} field can be indexed afterwards. (That the fast path
     * issues no I/O is asserted by the unit test
     * {@code InferenceIndexMappingManagerTests#testIndexAtCurrentVersion_immediateCallbackNoIO}.)
     */
    public void testWithUpToDateMappings_immediateCallbackWhenCurrentAndAllowsV4Fields() throws Exception {
        var setup = startNodesAndCreateInferenceIndex();

        // Re-read the live state now that .inference exists with current mappings. Pass it to
        // withUpToDateMappings; the manager should detect that mappings are already current and
        // complete the listener.
        var future = new PlainActionFuture<Void>();
        setup.manager().withUpToDateMappings(setup.clusterService().state(), future);
        future.actionGet(TEST_REQUEST_TIMEOUT);

        // Verify that the doc_type field can be indexed — confirms current mappings are active.
        new OriginSettingClient(client(), ClientHelper.INFERENCE_ORIGIN).index(
            new IndexRequest(InferenceIndex.INDEX_NAME).source(
                Map.of("doc_type", "model", "model_id", "test-noop-model", "task_type", "text_embedding", "service", "test-service")
            )
        ).actionGet();
    }

    private record InferenceIndexSetup(InferenceIndexMappingManager manager, ClusterService clusterService) {}

    /**
     * Starts a master and a data node carrying the routing attribute this test class's plugin
     * requires for {@code .inference} shards to allocate, then creates the index on demand through
     * {@code withUpToDateMappings} using the live cluster state — the index is not created
     * automatically.
     */
    private InferenceIndexSetup startNodesAndCreateInferenceIndex() {
        final var configAttr = Settings.builder().put(INDEX_ROUTER_ATTRIBUTE, CONFIG_ROUTER).build();
        internalCluster().startMasterOnlyNode(configAttr);
        internalCluster().startDataOnlyNode(configAttr);

        ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        InferenceIndexMappingManager manager = internalCluster().getCurrentMasterNodeInstance(InferenceIndexMappingManager.class);

        var createFuture = new PlainActionFuture<Void>();
        manager.withUpToDateMappings(clusterService.state(), createFuture);
        createFuture.actionGet(TEST_REQUEST_TIMEOUT);
        return new InferenceIndexSetup(manager, clusterService);
    }

    /**
     * Constructs a synthetic {@link ClusterState} that reports the {@code .inference} index as having
     * v3 mappings. Used by {@link #testWithUpToDateMappings_upgradesStaleMappingsAndAllowsV4Fields()}
     * to simulate the cluster state seen on an older node during a rolling upgrade.
     *
     * <p>The real cluster index is not modified; this state is only passed to
     * {@link InferenceIndexMappingManager#withUpToDateMappings} so that the manager detects the version
     * mismatch and issues a {@code PutMapping} against the real cluster.
     */
    private static ClusterState buildSyntheticV3ClusterState() {
        var v3MappingMap = XContentHelper.convertToMap(new BytesArray(InferenceIndex.mappingsV3()), false, XContentType.JSON).v2();
        var v3IndexMeta = IndexMetadata.builder(InferenceIndex.INDEX_NAME)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", v3MappingMap))
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(v3IndexMeta, false).build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();
    }

    private ActionFuture<InferenceAction.Response> sendInferenceProxyRequest(String inferenceId) throws IOException {
        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("input", List.of("test input"));
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        var inferenceRequest = new InferenceActionProxy.Request(
            TaskType.TEXT_EMBEDDING,
            inferenceId,
            content,
            XContentType.JSON,
            TimeValue.THIRTY_SECONDS,
            false,
            InferenceContext.EMPTY_INSTANCE
        );

        return client().execute(InferenceActionProxy.INSTANCE, inferenceRequest);
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
        var responseFuture = createInferenceEndpointAsync(taskType, inferenceId, serviceSettings);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));
    }

    private ActionFuture<PutInferenceModelAction.Response> createInferenceEndpointAsync(
        TaskType taskType,
        String inferenceId,
        Map<String, Object> serviceSettings
    ) throws IOException {
        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", TestDenseInferenceServiceExtension.TestInferenceService.NAME);
            builder.field("service_settings", serviceSettings);
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        var request = new PutInferenceModelAction.Request(taskType, inferenceId, content, XContentType.JSON, TEST_REQUEST_TIMEOUT);
        return client().execute(PutInferenceModelAction.INSTANCE, request);
    }
}
