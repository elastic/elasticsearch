/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.integration.IntegrationTestUtils.createInferenceEndpoint;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class GetInferenceFieldsIT extends ESIntegTestCase {
    private static final Map<String, Object> SPARSE_EMBEDDING_SERVICE_SETTINGS = Map.of("model", "my_model", "api_key", "my_api_key");
    private static final Map<String, Object> TEXT_EMBEDDING_SERVICE_SETTINGS = Map.of(
        "model",
        "my_model",
        "dimensions",
        256,
        "similarity",
        "cosine",
        "api_key",
        "my_api_key"
    );

    private static final String SPARSE_EMBEDDING_INFERENCE_ID = "sparse-embedding-id";
    private static final String TEXT_EMBEDDING_INFERENCE_ID = "text-embedding-id";

    private static final String INDEX_1 = "index-1";
    private static final String INDEX_2 = "index-2";

    private static final String INFERENCE_FIELD_1 = "inference-field-1";
    private static final String INFERENCE_FIELD_2 = "inference-field-2";
    private static final String INFERENCE_FIELD_3 = "inference-field-3";
    private static final String TEXT_FIELD_1 = "text-field-1";
    private static final String TEXT_FIELD_2 = "text-field-2";

    private boolean clusterConfigured = false;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, FakeMlPlugin.class);
    }

    @Before
    public void setUpCluster() throws Exception {
        if (clusterConfigured == false) {
            createInferenceEndpoints();
            createIndices();
            clusterConfigured = true;
        }
    }

    public void testNullQuery() {
        // TODO: Implement
    }

    public void testNonNullQuery() {
        // TODO: Implement
    }

    public void testBlankQuery() {
        // TODO: Implement
    }

    public void testNoInferenceFields() {
        // TODO: Implement
    }

    public void testResolveWildcards() {
        // TODO: Implement
    }

    public void testUseDefaultFields() {
        // TODO: Implement
    }

    public void testMissingIndexName() {
        // TODO: Implement
    }

    public void testMissingFieldName() {
        // TODO: Implement
    }

    public void testInvalidRequest() {
        // TODO: Implement
    }

    private void createInferenceEndpoints() throws IOException {
        createInferenceEndpoint(client(), TaskType.SPARSE_EMBEDDING, SPARSE_EMBEDDING_INFERENCE_ID, SPARSE_EMBEDDING_SERVICE_SETTINGS);
        createInferenceEndpoint(client(), TaskType.TEXT_EMBEDDING, TEXT_EMBEDDING_INFERENCE_ID, TEXT_EMBEDDING_SERVICE_SETTINGS);
    }

    private void createIndices() throws IOException {
        createIndex(INDEX_1);
        createIndex(INDEX_2);
    }

    private void createIndex(String indexName) throws IOException {
        final String inferenceField1InferenceId = switch (indexName) {
            case INDEX_1 -> SPARSE_EMBEDDING_INFERENCE_ID;
            case INDEX_2 -> TEXT_EMBEDDING_INFERENCE_ID;
            default -> throw new AssertionError("Unhandled index name [" + indexName + "]");
        };
        final String inferenceField2InferenceId = switch (indexName) {
            case INDEX_1 -> TEXT_EMBEDDING_INFERENCE_ID;
            case INDEX_2 -> SPARSE_EMBEDDING_INFERENCE_ID;
            default -> throw new AssertionError("Unhandled index name [" + indexName + "]");
        };

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        addSemanticTextField(INFERENCE_FIELD_1, inferenceField1InferenceId, mapping);
        addSemanticTextField(INFERENCE_FIELD_2, inferenceField2InferenceId, mapping);
        addSemanticTextField(INFERENCE_FIELD_3, SPARSE_EMBEDDING_INFERENCE_ID, mapping);
        addTextField(TEXT_FIELD_1, mapping);
        addTextField(TEXT_FIELD_2, mapping);
        mapping.endObject().endObject();
    }

    private void addSemanticTextField(String fieldName, String inferenceId, XContentBuilder mapping) throws IOException {
        mapping.startObject(fieldName);
        mapping.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mapping.field("inference_id", inferenceId);
        mapping.endObject();
    }

    private void addTextField(String fieldName, XContentBuilder mapping) throws IOException {
        mapping.startObject(fieldName);
        mapping.field("type", TextFieldMapper.CONTENT_TYPE);
        mapping.endObject();
    }
}
