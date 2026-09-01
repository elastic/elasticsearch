/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.results.ModelStoreResponse;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalService;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration test that verifies {@code GET _inference/_all} (and related paths) behave correctly
 * when ML is disabled and endpoints persisted with {@code "service": "elasticsearch"} or the
 * pre-8.13 alias {@code "service": "elser"} are present in the index.
 *
 * <p>The node starts with {@code xpack.ml.enabled=false}, so {@link ElasticsearchInternalService}
 * is never registered. Models are written directly to the registry (bypassing the PUT API, which
 * would reject an unregistered service) to simulate a cluster that had ML enabled in the past.
 */
public class GetInferenceModelMlDisabledIT extends ESSingleNodeTestCase {

    private static final String ES_ENDPOINT_ID = "es-endpoint";
    private static final String ELSER_ENDPOINT_ID = "elser-endpoint";
    private static final String TEST_ENDPOINT_ID = "test-endpoint";

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ReindexPlugin.class, LocalStateInferencePlugin.class);
    }

    public void testGetAllModels_SkipsElasticsearchAndElserEndpoints() {
        var registry = modelRegistry();
        storeModel(registry, esModel());
        storeModel(registry, elserModel());
        storeModel(registry, testServiceModel());

        var response = executeGetAll();

        assertThat(response.getEndpoints(), hasSize(1));
        assertThat(response.getEndpoints().get(0).getInferenceEntityId(), equalTo(TEST_ENDPOINT_ID));
    }

    public void testGetAllModels_ReturnsEmpty_WhenOnlyElasticsearchEndpointsExist() {
        // Regression test for the empty-GroupedActionListener bug: when every persisted endpoint
        // belongs to the skipped elasticsearch service, the response must be empty, not a 500.
        var registry = modelRegistry();
        storeModel(registry, esModel());
        storeModel(registry, elserModel());

        var response = executeGetAll();

        assertThat(response.getEndpoints(), empty());
    }

    public void testGetModel_ByIdStillFails_ForElasticsearchEndpoint() {
        // The single-endpoint path does NOT apply the skip — asking for a specific id should surface
        // the problem rather than succeed silently.
        storeModel(modelRegistry(), esModel());

        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        client().execute(GetInferenceModelAction.INSTANCE, new GetInferenceModelAction.Request(ES_ENDPOINT_ID, TaskType.ANY), future);

        expectThrows(ElasticsearchStatusException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
    }

    private ModelRegistry modelRegistry() {
        return node().injector().getInstance(ModelRegistry.class);
    }

    private void storeModel(ModelRegistry registry, org.elasticsearch.inference.Model model) {
        var future = new TestPlainActionFuture<List<ModelStoreResponse>>();
        registry.storeModels(List.of(model), false, future, TimeValue.THIRTY_SECONDS);
        future.actionGet(TEST_REQUEST_TIMEOUT);
    }

    private GetInferenceModelAction.Response executeGetAll() {
        var future = new TestPlainActionFuture<GetInferenceModelAction.Response>();
        client().execute(GetInferenceModelAction.INSTANCE, new GetInferenceModelAction.Request("_all", TaskType.ANY, false), future);
        return future.actionGet(TEST_REQUEST_TIMEOUT);
    }

    /**
     * A model persisted with {@code "service": "elasticsearch"} — as created by the pre-change code
     * on a cluster with ML enabled.
     */
    private static org.elasticsearch.inference.Model esModel() {
        return ModelRegistryIT.createModel(ES_ENDPOINT_ID, TaskType.SPARSE_EMBEDDING, ElasticsearchInternalService.NAME);
    }

    /**
     * A model persisted with {@code "service": "elser"} — as created before the 8.13 service rename.
     */
    private static org.elasticsearch.inference.Model elserModel() {
        return ModelRegistryIT.createModel(
            ELSER_ENDPOINT_ID,
            TaskType.SPARSE_EMBEDDING,
            ElasticsearchInternalService.OLD_ELSER_SERVICE_NAME
        );
    }

    /**
     * A model whose service ({@code test_service}) IS registered in {@link LocalStateInferencePlugin}.
     */
    private static org.elasticsearch.inference.Model testServiceModel() {
        return new TestSparseInferenceServiceExtension.TestSparseModel(
            TEST_ENDPOINT_ID,
            new TestSparseInferenceServiceExtension.TestServiceSettings("test-model", null, false)
        );
    }
}
