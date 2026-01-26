/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.GetInferenceDiagnosticsAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ClearInferenceEndpointCacheActionTests extends ESSingleNodeTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final String INFERENCE_ENDPOINT_ID = "1";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateInferencePlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    public void testCacheEviction() throws Exception {
        storeGoodEndpoint();
        invokeEndpoint();

        var stats = cacheStats();
        assertThat(stats.entryCount(), equalTo(1));
        assertThat(stats.hits(), equalTo(0L));
        assertThat(stats.misses(), equalTo(1L));
        assertThat(stats.evictions(), equalTo(0L));

        var listener = new PlainActionFuture<AcknowledgedResponse>();
        clusterAdmin().execute(ClearInferenceEndpointCacheAction.INSTANCE, new ClearInferenceEndpointCacheAction.Request(), listener);
        assertTrue(listener.actionGet(TIMEOUT).isAcknowledged());

        assertBusy(() -> {
            var nextStats = cacheStats();
            assertThat(nextStats.entryCount(), equalTo(0));
            assertThat(nextStats.hits(), equalTo(0L));
            assertThat(nextStats.misses(), equalTo(1L));
            assertThat(nextStats.evictions(), equalTo(1L));
        }, 10, TimeUnit.SECONDS);

        invokeEndpoint();
        stats = cacheStats();
        assertThat(stats.entryCount(), equalTo(1));
        assertThat(stats.hits(), equalTo(0L));
        assertThat(stats.misses(), equalTo(2L));
        assertThat(stats.evictions(), equalTo(1L));
    }

    private void storeGoodEndpoint() throws IOException {
        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", TestSparseInferenceServiceExtension.TestInferenceService.NAME);
            builder.field("service_settings", Map.of("model", "model", "api_key", "1234"));
            builder.endObject();

            content = BytesReference.bytes(builder);
        }

        var request = new PutInferenceModelAction.Request(
            TaskType.SPARSE_EMBEDDING,
            INFERENCE_ENDPOINT_ID,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        client().execute(PutInferenceModelAction.INSTANCE, request).actionGet(TIMEOUT);
    }

    private void invokeEndpoint() {
        client().execute(
            InferenceAction.INSTANCE,
            new InferenceAction.Request(
                TaskType.SPARSE_EMBEDDING,
                INFERENCE_ENDPOINT_ID,
                null,
                null,
                null,
                List.of("hello"),
                null,
                InputType.INTERNAL_SEARCH,
                TIMEOUT,
                false
            )
        ).actionGet(TIMEOUT);
    }

    private GetInferenceDiagnosticsAction.NodeResponse.Stats cacheStats() {
        var diagnostics = client().execute(GetInferenceDiagnosticsAction.INSTANCE, new GetInferenceDiagnosticsAction.Request())
            .actionGet(TIMEOUT);

        assertThat(diagnostics.getNodes(), hasSize(1));
        return diagnostics.getNodes().getFirst().getInferenceEndpointRegistryStats();
    }
}
