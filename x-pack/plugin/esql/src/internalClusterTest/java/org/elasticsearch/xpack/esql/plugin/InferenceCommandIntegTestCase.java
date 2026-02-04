/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

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
        String config = String.format(Locale.ROOT, """
            {
              "service": "%s",
              "service_settings": {
                "model_id": "test-%s",
                "api_key": "test-key"
              }
            }
            """, serviceName, taskType.toString().toLowerCase(Locale.ROOT));

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
