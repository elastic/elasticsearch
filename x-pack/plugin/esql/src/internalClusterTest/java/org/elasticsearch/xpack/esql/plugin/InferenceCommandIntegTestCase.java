/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;

/**
 * Abstract base class for ESQL inference command integration tests.
 * Provides common setup for test infrastructure including:
 * - Plugin configuration (LocalStateInferencePlugin, TestInferenceServicePlugin)
 * - License settings (trial mode)
 * - Test index creation with sample data
 * - Inference endpoint management
 * - Cluster settings cleanup
 */
public abstract class InferenceCommandIntegTestCase extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = CollectionUtils.appendToCopy(super.nodePlugins(), LocalStateInferencePlugin.class);
        return CollectionUtils.appendToCopy(plugins, TestInferenceServicePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    // ============================================
    // Test Index Management
    // ============================================

    /**
     * Creates and populates a test index with sample documents for inference testing.
     * Each document has: id (integer), title (text), and content (text).
     *
     * @param indexName the name of the index to create
     */
    protected void createAndPopulateTestIndex(String indexName) {
        createAndPopulateTestIndex(indexName, 6);
    }

    /**
     * Creates and populates a test index with the specified number of documents for inference testing.
     * Each document has: id (integer), title (text), and content (text).
     *
     * @param indexName the name of the index to create
     * @param numDocs the number of documents to create
     */
    protected void createAndPopulateTestIndex(String indexName, int numDocs) {
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "title", "type=text", "content", "type=text");
        assertAcked(createRequest);

        var bulkRequest = client().prepareBulk();
        for (int i = 1; i <= numDocs; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).id(String.valueOf(i))
                    .source("id", i, "title", "Document " + i + " title", "content", "Document " + i + " content")
            );
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        ensureYellow(indexName);
    }

    /**
     * Indexes one extra document with the given title into an index already created by
     * {@link #createAndPopulateTestIndex(String, int)}, keeping the same field shape. Lets a test add a row whose text
     * triggers specific inference behaviour.
     */
    protected void indexDocumentWithTitle(String indexName, int id, String title) {
        client().prepareIndex(indexName)
            .setId(String.valueOf(id))
            .setSource("id", id, "title", title, "content", "Document " + id + " content")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    /**
     * Runs a query and returns its rows, collecting into {@code warnings} the {@code Warning} response headers the
     * coordinating node accumulated. ES|QL surfaces warnings as response headers rather than on the response body, so the
     * {@code run} helper cannot see them; this drives the transport action directly to reach the coordinator's thread context.
     *
     * @param warnings collector for the coordinator's warning headers; pass a thread-safe list
     * @return the query's rows
     */
    protected List<List<Object>> runCollectingWarnings(String query, List<String> warnings) throws Exception {
        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<List<Object>>> values = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();

        // The transport client owns the response's ref count, so the response must not be closed here.
        client(coordinator.getName()).execute(
            EsqlQueryAction.INSTANCE,
            EsqlQueryRequest.syncEsqlQueryRequest(query),
            ActionListener.wrap(response -> {
                try {
                    values.set(getValuesList(response));
                    TransportService transportService = internalCluster().getInstance(TransportService.class, coordinator.getName());
                    warnings.addAll(
                        transportService.getThreadPool().getThreadContext().getResponseHeaders().getOrDefault("Warning", List.of())
                    );
                } finally {
                    latch.countDown();
                }
            }, e -> {
                failure.set(e);
                latch.countDown();
            })
        );

        assertTrue("query did not complete within 30 seconds", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("query failed: " + query, failure.get());
        }
        return values.get();
    }

    // ============================================
    // Inference Endpoint Management
    // ============================================

    /**
     * Creates a test inference endpoint.
     *
     * @param modelId the ID for the inference model
     * @param taskType the task type (RERANK, COMPLETION, etc.)
     * @param serviceName the name of the test service
     * @throws IOException if endpoint creation fails
     */
    protected void createTestInferenceEndpoint(String modelId, TaskType taskType, String serviceName) throws IOException {
        createTestInferenceEndpoint(modelId, taskType, serviceName, null);
    }

    /**
     * Creates a test inference endpoint, optionally pinning the number of dimensions its embeddings carry.
     *
     * @param dimensions the embedding dimension count the mock service should produce, or {@code null} to leave the service
     *                   default in place. Two endpoints created with different dimension counts return vectors of different
     *                   widths, which lets a test tell which endpoint actually served a request.
     */
    protected void createTestInferenceEndpoint(String modelId, TaskType taskType, String serviceName, Integer dimensions)
        throws IOException {
        String dimensionsSetting = dimensions == null ? "" : String.format(Locale.ROOT, ",%n    \"dimensions\": %d", dimensions);
        String config = String.format(Locale.ROOT, """
            {
              "service": "%s",
              "service_settings": {
                "model_id": "test-%s",
                "api_key": "test-key"%s
              }
            }
            """, serviceName, taskType.toString().toLowerCase(Locale.ROOT), dimensionsSetting);

        try {
            client().execute(
                PutInferenceModelAction.INSTANCE,
                new PutInferenceModelAction.Request(taskType, modelId, new BytesArray(config), XContentType.JSON, TEST_REQUEST_TIMEOUT)
            ).actionGet();
        } catch (Exception e) {
            // May already exist or test service not available
            logger.warn("Could not create {} inference endpoint: {}", taskType.toString().toLowerCase(Locale.ROOT), e.getMessage());
        }
    }

    /**
     * Deletes a test inference endpoint.
     *
     * @param modelId the ID of the inference model to delete
     * @param taskType the task type of the model
     */
    protected void deleteTestInferenceEndpoint(String modelId, TaskType taskType) {
        try {
            client().execute(
                DeleteInferenceEndpointAction.INSTANCE,
                new DeleteInferenceEndpointAction.Request(modelId, taskType, false, false)
            ).actionGet();
        } catch (Exception e) {
            // Ignore if model doesn't exist
            logger.debug("Could not delete inference endpoint {}: {}", modelId, e.getMessage());
        }
    }

    // ============================================
    // Cluster Settings Management
    // ============================================

    /**
     * Cleans up persistent cluster settings by resetting them to null.
     * This prevents "test leaves persistent cluster metadata behind" errors.
     *
     * @param settings the settings to clean up (will be set to null)
     */
    protected void cleanupClusterSettings(Setting<?>... settings) {
        if (settings.length == 0) {
            return;
        }

        try {
            var builder = Settings.builder();
            for (Setting<?> setting : settings) {
                builder.putNull(setting.getKey());
            }
            updateClusterSettings(builder);
        } catch (Exception e) {
            // Ignore if settings are already cleaned up
            logger.debug("Could not cleanup cluster settings: {}", e.getMessage());
        }
    }
}
