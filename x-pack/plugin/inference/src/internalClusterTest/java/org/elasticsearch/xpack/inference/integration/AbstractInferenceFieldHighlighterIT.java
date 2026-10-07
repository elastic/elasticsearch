/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.inference.FakeMlPlugin;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * Base class for tests that highlight an inference field with the semantic highlighter, across every embedding task type the field mapper
 * supports. Searches are issued from a coordinating-only node so that highlights cross the transport layer.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
abstract class AbstractInferenceFieldHighlighterIT extends ESIntegTestCase {
    static final int VECTOR_DIMENSIONS = 128;  // Use a dimension count that is compatible with BIT element type
    static final List<SourceFieldMapper.Mode> SOURCE_MODES = List.of(SourceFieldMapper.Mode.STORED, SourceFieldMapper.Mode.SYNTHETIC);

    String indexName = null;
    private String inferenceId;
    private TaskType taskType;
    private final SourceFieldMapper.Mode sourceMode;

    AbstractInferenceFieldHighlighterIT(SourceFieldMapper.Mode sourceMode) {
        this.sourceMode = sourceMode;
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode)
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateInferencePlugin.class, TestInferenceServicePlugin.class, ReindexPlugin.class, FakeMlPlugin.class);
    }

    @Override
    protected int minimumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfShards() {
        return cluster().numDataNodes();
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    @Before
    private void createInferenceEndpoint() throws IOException {
        taskType = randomFrom(supportedTaskTypes());
        inferenceId = randomIdentifier();

        final Map<String, Object> serviceSettings = switch (taskType) {
            case SPARSE_EMBEDDING -> Map.of("model", "my_model", "api_key", "my_api_key");
            case TEXT_EMBEDDING, EMBEDDING -> generateDenseServiceSettings(randomFrom(DenseVectorFieldMapper.ElementType.values()));
            default -> throw new AssertionError("Unhandled task type [" + taskType + "]");
        };
        IntegrationTestUtils.createInferenceEndpoint(client(), taskType, inferenceId, serviceSettings);
    }

    @After
    private void cleanUp() {
        if (indexName != null) {
            IntegrationTestUtils.deleteIndex(client(), indexName);
        }
        if (inferenceId != null) {
            IntegrationTestUtils.deleteInferenceEndpoint(client(), taskType, inferenceId);
        }
    }

    abstract Set<TaskType> supportedTaskTypes();

    abstract void addInferenceFieldsToMapping(XContentBuilder mapping, Map<String, String> fieldNameToInferenceIdMap) throws IOException;

    /**
     * Whether the inference field type can be defined as a multi-field.
     */
    boolean supportsMultiFields() {
        return true;
    }

    public void testHighlightOwnAndCopyToValues() throws Exception {
        createChainedCopyToIndex();

        final String ownValue = "a cat on a windowsill";
        final String copiedValue = "a dog running in a park";
        final String chainedValue = "a bird on a branch";

        client().prepareIndex(indexName)
            .setSource("chained_source_field", chainedValue, "source_field", copiedValue, "inference_field", ownValue)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TEST_REQUEST_TIMEOUT);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .highlighter(
                new HighlightBuilder().field(new HighlightBuilder.Field("inference_field").highlighterType("semantic").numOfFragments(3))
            );

        // Use the coordinating-only node so that highlights are serialized over the wire (data node -> coordinating node)
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { indexName }, source)),
            response -> {
                assertHitCount(response, 1L);
                // Only one level of copy_to is followed: chained_source_field -> source_field -> inference_field does not make
                // chained_source_field's value part of inference_field. Fragments are in chunk order, with the field's own value first.
                assertHighlight(response, 0, "inference_field", 0, 2, equalTo(ownValue));
                assertHighlight(response, 0, "inference_field", 1, 2, equalTo(copiedValue));
            }
        );
    }

    public void testHighlightMultiFieldOfCopyToTarget() throws Exception {
        assumeTrue("Inference field type does not support multi-fields", supportsMultiFields());
        createMultiFieldCopyToIndex();

        final String ownValue = "a cat on a windowsill";
        final String copiedValue = "a dog running in a park";

        client().prepareIndex(indexName)
            .setSource("text_field", ownValue, "source_field", copiedValue)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TEST_REQUEST_TIMEOUT);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .highlighter(
                new HighlightBuilder().field(
                    new HighlightBuilder.Field("text_field.inference_field").highlighterType("semantic").numOfFragments(3)
                )
            );

        // Use the coordinating-only node so that highlights are serialized over the wire (data node -> coordinating node)
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { indexName }, source)),
            response -> {
                assertHitCount(response, 1L);
                // The multi-field's own value is the value of its parent text_field. Fragments are in chunk order, which for a
                // multi-field of a copy_to target has the copied value first.
                assertHighlight(response, 0, "text_field.inference_field", 0, 2, equalTo(copiedValue));
                assertHighlight(response, 0, "text_field.inference_field", 1, 2, equalTo(ownValue));
            }
        );
    }

    /**
     * Creates an index where {@code inference_field} is a {@code copy_to} target of {@code source_field}, which is itself a
     * {@code copy_to} target of {@code chained_source_field}.
     */
    private void createChainedCopyToIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        addInferenceFieldsToMapping(mapping, Map.of("inference_field", inferenceId));
        addTextField(mapping, "source_field", b -> b.field("copy_to", "inference_field"));
        addTextField(mapping, "chained_source_field", b -> b.field("copy_to", "source_field"));
        mapping.endObject().endObject();

        createIndex(mapping);
    }

    /**
     * Creates an index where {@code text_field.inference_field} is a multi-field of {@code text_field}, which is a {@code copy_to}
     * target of {@code source_field}.
     */
    private void createMultiFieldCopyToIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        addTextField(mapping, "text_field", b -> {
            b.startObject("fields");
            addInferenceFieldsToMapping(b, Map.of("inference_field", inferenceId));
            b.endObject();
        });
        addTextField(mapping, "source_field", b -> b.field("copy_to", "text_field"));
        mapping.endObject().endObject();

        createIndex(mapping);
    }

    private void createIndex(XContentBuilder mapping) {
        indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setMapping(mapping));
        ensureGreen(indexName);
    }

    /**
     * Adds a text field with randomly enabled {@code store}, so that stored copy_to sources are also covered.
     */
    private static void addTextField(XContentBuilder mapping, String name, CheckedConsumer<XContentBuilder, IOException> fieldBuilder)
        throws IOException {
        mapping.startObject(name).field("type", "text").field("store", randomBoolean());
        fieldBuilder.accept(mapping);
        mapping.endObject();
    }

    private static Map<String, Object> generateDenseServiceSettings(DenseVectorFieldMapper.ElementType elementType) {
        List<SimilarityMeasure> supportedSimilarities = new ArrayList<>(
            DenseVectorFieldMapperTestUtils.getSupportedSimilarities(elementType)
        );
        // Dot product requires unit vectors, which the mock inference services do not produce
        supportedSimilarities.remove(SimilarityMeasure.DOT_PRODUCT);

        return Map.of(
            "model",
            "my_model",
            "dimensions",
            VECTOR_DIMENSIONS,
            "similarity",
            randomFrom(supportedSimilarities),
            "element_type",
            elementType,
            "api_key",
            "my_api_key"
        );
    }
}
