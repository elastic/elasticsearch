/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.MinimalServiceSettings.DIMENSIONS_FIELD;
import static org.elasticsearch.inference.MinimalServiceSettings.ELEMENT_TYPE_FIELD;
import static org.elasticsearch.inference.MinimalServiceSettings.SIMILARITY_FIELD;
import static org.elasticsearch.inference.SimilarityMeasure.COSINE;
import static org.elasticsearch.inference.SimilarityMeasure.L2_NORM;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
public class CreateInferenceEndpointIT extends ESIntegTestCase {

    public static final String INFERENCE_ID = "inference-id";
    public static final String SEMANTIC_TEXT_FIELD = "semantic-text-field";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class);
    }

    public void testCreateInferenceEndpoint_fails_whenSemanticTextFieldUsingTheInferenceIdExists_andDocumentsInIndex() throws IOException {
        Map<String, Object> serviceSettings = getRandomTextEmbeddingServiceSettings();
        String otherInferenceId = "some-other-inference-id";
        assertEndpointCreationSuccessful(serviceSettings, INFERENCE_ID);
        assertEndpointCreationSuccessful(serviceSettings, otherInferenceId);

        List<String> indicesUsingInferenceId = new ArrayList<>();
        List<String> indicesNotUsingInferenceId = new ArrayList<>();
        Set<String> allIndices = new HashSet<>();
        int indexPairsToCreate = 10;

        for (int i = 0; i < indexPairsToCreate; ++i) {
            String indexUsingInferenceId = createIndexWithSemanticTextMapping(INFERENCE_ID, allIndices);
            indexDocument(indexUsingInferenceId);
            indicesUsingInferenceId.add(indexUsingInferenceId);
            allIndices.add(indexUsingInferenceId);

            String indexNotUsingInferenceId = createIndexWithSemanticTextMapping(otherInferenceId, allIndices);
            indexDocument(indexNotUsingInferenceId);
            indicesNotUsingInferenceId.add(indexNotUsingInferenceId);
            allIndices.add(indexNotUsingInferenceId);
        }

        forceDeleteInferenceEndpoint();

        ElasticsearchStatusException statusException = expectThrows(
            ElasticsearchStatusException.class,
            () -> createTextEmbeddingEndpoint(serviceSettings, INFERENCE_ID).actionGet(TEST_REQUEST_TIMEOUT)
        );

        assertThat(statusException.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            statusException.getMessage(),
            containsString(
                "Inference endpoint [" + INFERENCE_ID + "] could not be created because it is being used in mappings for indices: ["
            )
        );

        // Make sure we only report the indices that were using the inference ID
        for (int i = 0; i < indexPairsToCreate; ++i) {
            assertThat(statusException.getMessage(), containsString(indicesUsingInferenceId.get(i)));
            assertThat(statusException.getMessage(), not(containsString(indicesNotUsingInferenceId.get(i))));
        }
    }

    public void testCreateInferenceEndpoint_succeeds_whenSemanticTextFieldUsingThatInferenceIdExists_andNoDocumentsInIndex()
        throws IOException {
        Map<String, Object> serviceSettings = getRandomTextEmbeddingServiceSettings();
        assertEndpointCreationSuccessful(serviceSettings, INFERENCE_ID);

        createIndexWithSemanticTextMapping();

        forceDeleteInferenceEndpoint();

        assertEndpointCreationSuccessful(serviceSettings, INFERENCE_ID);
    }

    public void testCreateInferenceEndpoint_succeeds_whenIndexIsCreatedBeforeInferenceEndpoint() throws IOException {
        createIndexWithSemanticTextMapping();

        assertEndpointCreationSuccessful(getRandomTextEmbeddingServiceSettings(), INFERENCE_ID);
    }

    private void assertEndpointCreationSuccessful(Map<String, Object> serviceSettings, String inferenceId) throws IOException {
        assertThat(
            createTextEmbeddingEndpoint(serviceSettings, inferenceId).actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(),
            equalTo(inferenceId)
        );
    }

    private ActionFuture<PutInferenceModelAction.Response> createTextEmbeddingEndpoint(
        Map<String, Object> serviceSettings,
        String inferenceId
    ) throws IOException {
        final BytesReference content;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("service", TestDenseInferenceServiceExtension.TestInferenceService.NAME);
            builder.field("service_settings", serviceSettings);
            builder.endObject();
            content = BytesReference.bytes(builder);
        }

        var request = new PutInferenceModelAction.Request(
            TaskType.TEXT_EMBEDDING,
            inferenceId,
            content,
            XContentType.JSON,
            TEST_REQUEST_TIMEOUT
        );
        return client().execute(PutInferenceModelAction.INSTANCE, request);
    }

    private void createIndexWithSemanticTextMapping() throws IOException {
        createIndexWithSemanticTextMapping(CreateInferenceEndpointIT.INFERENCE_ID, Set.of());
    }

    private String createIndexWithSemanticTextMapping(String inferenceId, Set<String> existingIndexNames) throws IOException {
        // Ensure that all index names are unique
        String indexName = randomValueOtherThanMany(
            existingIndexNames::contains,
            () -> ESTestCase.randomAlphaOfLength(10).toLowerCase(Locale.ROOT)
        );
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject(SEMANTIC_TEXT_FIELD);
        mapping.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mapping.field("inference_id", inferenceId);
        mapping.endObject().endObject().endObject();

        assertAcked(prepareCreate(indexName).setMapping(mapping));
        return indexName;
    }

    private static void indexDocument(String indexName) {
        Map<String, Object> source = Map.of(SEMANTIC_TEXT_FIELD, randomAlphaOfLength(10));
        DocWriteResponse response = client().prepareIndex(indexName).setSource(source).get(TEST_REQUEST_TIMEOUT);
        assertThat(response.getResult(), is(DocWriteResponse.Result.CREATED));
        client().admin().indices().prepareRefresh(indexName).get();
    }

    private static Map<String, Object> getRandomTextEmbeddingServiceSettings() {
        Map<String, Object> settings = new HashMap<>();
        settings.put("model", "my_model");
        settings.put("api_key", "my_api_key");
        // Always use a dimension that's a multiple of 8 because the BIT element type requires that
        settings.put(DIMENSIONS_FIELD, randomIntBetween(8, 128) * 8);
        if (randomBoolean()) {
            settings.put(ELEMENT_TYPE_FIELD, randomFrom(ElementType.values()).toString());
        }
        if (randomBoolean()) {
            // We can't use the DOT_PRODUCT similarity measure because it only works with unit-length vectors, which
            // the TestDenseInferenceServiceExtension does not produce
            settings.put(SIMILARITY_FIELD, randomFrom(COSINE, L2_NORM).toString());
        }
        // The only supported similarity measure for BIT vectors is L2_NORM
        if (ElementType.BIT.toString().equals(settings.get(ELEMENT_TYPE_FIELD))) {
            settings.put(SIMILARITY_FIELD, L2_NORM.toString());
        }
        return settings;
    }

    private void forceDeleteInferenceEndpoint() {
        var request = new DeleteInferenceEndpointAction.Request(INFERENCE_ID, TaskType.TEXT_EMBEDDING, true, false);
        var responseFuture = client().execute(DeleteInferenceEndpointAction.INSTANCE, request);
        responseFuture.actionGet(TEST_REQUEST_TIMEOUT);
    }
}
