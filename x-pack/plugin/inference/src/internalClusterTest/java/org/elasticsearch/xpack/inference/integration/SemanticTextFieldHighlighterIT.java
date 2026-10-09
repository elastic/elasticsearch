/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.integration;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceEndpointAction;
import org.elasticsearch.xpack.core.inference.action.PutInferenceModelAction;
import org.elasticsearch.xpack.inference.LocalStateInferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestInferenceServicePlugin;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.xpack.inference.mapper.SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests highlighting a {@code semantic_text} field with the semantic highlighter, across every embedding task type the field mapper
 * supports. Searches are issued from a coordinating-only node so that highlights cross the transport layer.
 */
@ESTestCase.WithoutEntitlements // due to dependency issue ES-12435
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 1, supportsDedicatedMasters = false)
public class SemanticTextFieldHighlighterIT extends ESIntegTestCase {
    private static final int VECTOR_DIMENSIONS = 128;  // Use a dimension count that is compatible with BIT element type
    private static final List<SourceFieldMapper.Mode> SOURCE_MODES = List.of(
        SourceFieldMapper.Mode.STORED,
        SourceFieldMapper.Mode.SYNTHETIC
    );

    private final SourceFieldMapper.Mode sourceMode;
    private final boolean useLegacyFormat;

    private String indexName = null;
    private String inferenceId;
    private TaskType taskType;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean useLegacyFormat : List.of(false, true)) {
            for (SourceFieldMapper.Mode sourceMode : SOURCE_MODES) {
                parameters.add(new Object[] { sourceMode, useLegacyFormat });
            }
        }
        return parameters;
    }

    public SemanticTextFieldHighlighterIT(SourceFieldMapper.Mode sourceMode, boolean useLegacyFormat) {
        this.sourceMode = sourceMode;
        this.useLegacyFormat = useLegacyFormat;
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // The legacy format requires setting the index version the index was created with
        return false;
    }

    @Override
    public Settings indexSettings() {
        Settings.Builder builder = Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode);
        if (useLegacyFormat) {
            builder.put(IndexMetadata.SETTING_VERSION_CREATED, getRandomCompatibleIndexVersion(true))
                .put(USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), true);
        }
        return builder.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            LocalStateInferencePlugin.class,
            TestInferenceServicePlugin.class,
            ReindexPlugin.class,
            SemanticTextIndexVersionIT.FakeMlPlugin.class
        );
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
        taskType = randomFrom(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING);
        inferenceId = randomIdentifier();

        final Map<String, Object> serviceSettings = switch (taskType) {
            case SPARSE_EMBEDDING -> Map.of("model", "my_model", "api_key", "my_api_key");
            case TEXT_EMBEDDING -> generateDenseServiceSettings(randomFrom(DenseVectorFieldMapper.ElementType.values()));
            default -> throw new AssertionError("Unhandled task type [" + taskType + "]");
        };
        createInferenceEndpoint(taskType, inferenceId, serviceSettings);
    }

    @After
    private void cleanUp() {
        if (inferenceId != null) {
            deleteInferenceEndpoint(taskType, inferenceId);
        }
    }

    public void testHighlightOwnAndCopyToValues() throws Exception {
        createChainedCopyToIndex();

        final List<String> ownValue = randomChunks(3);
        final List<String> copiedValue = randomChunks(3);
        final List<String> chainedValue = randomChunks(3);

        client().prepareIndex(indexName)
            .setSource("chained_source_field", chainedValue, "source_field", copiedValue, "inference_field", ownValue)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TEST_REQUEST_TIMEOUT);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .highlighter(
                new HighlightBuilder().field(new HighlightBuilder.Field("inference_field").highlighterType("semantic").numOfFragments(10))
            );

        // Use the coordinating-only node so that highlights are serialized over the wire (data node -> coordinating node)
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { indexName }, source)),
            response -> {
                assertHitCount(response, 1L);
                // Only one level of copy_to is followed: chained_source_field -> source_field -> inference_field does not make
                // chained_source_field's value part of inference_field. Fragments are in chunk order, with the field's own value first.
                assertThat(
                    highlightFragments(response.getHits().getAt(0), "inference_field"),
                    equalTo(CollectionUtils.concatLists(ownValue, copiedValue))
                );
            }
        );
    }

    public void testHighlightMultiFieldOfCopyToTarget() throws Exception {
        assumeFalse("The legacy format does not support semantic_text as a multi-field", useLegacyFormat);
        createMultiFieldCopyToIndex();

        final List<String> ownValue = randomChunks(3);
        final List<String> copiedValue = randomChunks(3);

        client().prepareIndex(indexName)
            .setSource("text_field", ownValue, "source_field", copiedValue)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get(TEST_REQUEST_TIMEOUT);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .highlighter(
                new HighlightBuilder().field(
                    new HighlightBuilder.Field("text_field.inference_field").highlighterType("semantic").numOfFragments(10)
                )
            );

        // Use the coordinating-only node so that highlights are serialized over the wire (data node -> coordinating node)
        assertNoFailuresAndResponse(
            internalCluster().coordOnlyNodeClient().search(new SearchRequest(new String[] { indexName }, source)),
            response -> {
                assertHitCount(response, 1L);
                // The multi-field's own value is the value of its parent text_field. Each source field's fragments are in chunk
                // order, but which source field comes first depends on the hash order of the field names, so accept either.
                assertThat(
                    highlightFragments(response.getHits().getAt(0), "text_field.inference_field"),
                    either(equalTo(CollectionUtils.concatLists(ownValue, copiedValue))).or(
                        equalTo(CollectionUtils.concatLists(copiedValue, ownValue))
                    )
                );
            }
        );
    }

    /**
     * Creates an index where {@code inference_field} is a {@code copy_to} target of {@code source_field}, which is itself a
     * {@code copy_to} target of {@code chained_source_field}.
     */
    private void createChainedCopyToIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        addSemanticTextField(mapping, "inference_field", inferenceId);
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
            addSemanticTextField(b, "inference_field", inferenceId);
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

    private static void addSemanticTextField(XContentBuilder mapping, String name, String inferenceId) throws IOException {
        mapping.startObject(name);
        mapping.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mapping.field("inference_id", inferenceId);
        mapping.endObject();
    }

    private void createInferenceEndpoint(TaskType taskType, String inferenceId, Map<String, Object> serviceSettings) throws IOException {
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
        var responseFuture = client().execute(PutInferenceModelAction.INSTANCE, request);
        assertThat(responseFuture.actionGet(TEST_REQUEST_TIMEOUT).getModel().getInferenceEntityId(), equalTo(inferenceId));
    }

    private void deleteInferenceEndpoint(TaskType taskType, String inferenceId) {
        assertAcked(
            safeGet(
                client().execute(
                    DeleteInferenceEndpointAction.INSTANCE,
                    new DeleteInferenceEndpointAction.Request(inferenceId, taskType, true, false)
                )
            )
        );
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

    /**
     * Generates between 1 and {@code maxChunks} chunks, each made of random alphanumeric words of random lengths.
     */
    private static List<String> randomChunks(int maxChunks) {
        return randomList(
            1,
            maxChunks,
            () -> String.join(" ", randomList(1, 5, () -> randomAlphanumericOfLength(randomIntBetween(1, 10))))
        );
    }

    private static List<String> highlightFragments(SearchHit hit, String field) {
        return Stream.of(hit.getHighlightFields().get(field).fragments()).map(Text::string).toList();
    }
}
