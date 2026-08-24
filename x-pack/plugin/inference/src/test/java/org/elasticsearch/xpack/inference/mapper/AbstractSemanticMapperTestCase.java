/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.IndexOptions;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EndpointClusterState;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.diskbbq.DiskBBQPlugin;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.index.IndexSettings.DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING;
import static org.elasticsearch.index.IndexVersions.NEW_SPARSE_VECTOR;
import static org.elasticsearch.index.IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQ_MIN_DIMS;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.defaultDenseVectorIndexOptions;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.getSupportedSimilarities;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.randomCompatibleDimensions;
import static org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper.INDEX_OPTIONS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKING_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Shared base class for {@link SemanticTextFieldMapperTests} and {@link SemanticFieldMapperTests}.
 * Holds the common setup infrastructure (thread pool, model registry, plugins) and the MapperTestCase
 * overrides that are identical for both semantic field types.
 */
abstract class AbstractSemanticMapperTestCase<T extends SemanticFieldMapper, U extends SemanticFieldMapper.SemanticFieldType> extends
    MapperTestCase {

    static class VariableLicenseDiskBBQPlugin extends DiskBBQPlugin {
        private static final Settings STATELESS_SETTINGS = Settings.builder()
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        static VariableLicenseDiskBBQPlugin BASIC = new VariableLicenseDiskBBQPlugin(
            STATELESS_SETTINGS,
            new XPackLicenseState(() -> 0L, new XPackLicenseStatus(License.OperationMode.BASIC, true, null))
        );
        static VariableLicenseDiskBBQPlugin ENTERPRISE = new VariableLicenseDiskBBQPlugin(
            STATELESS_SETTINGS,
            new XPackLicenseState(() -> 0L, new XPackLicenseStatus(License.OperationMode.ENTERPRISE, true, null))
        );

        private final XPackLicenseState licenseState;

        VariableLicenseDiskBBQPlugin(Settings settings, XPackLicenseState licenseState) {
            super(settings);
            this.licenseState = requireNonNull(licenseState);
        }

        @Override
        protected XPackLicenseState getLicenseState() {
            return licenseState;
        }
    }

    /** The kinds of incompatibility that can exist between two inference endpoint model settings. */
    protected enum IncompatibilityKind {
        TASK_TYPE,
        DIMENSIONS,
        SIMILARITY,
        ELEMENT_TYPE,
        DOES_NOT_EXIST
    }

    protected final License.OperationMode operationMode;
    protected ModelRegistry globalModelRegistry;
    private TestThreadPool threadPool;

    AbstractSemanticMapperTestCase(License.OperationMode operationMode) {
        this.operationMode = operationMode;
    }

    @Before
    private void initializeTestEnvironment() {
        threadPool = createThreadPool();
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        var modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool), Utils.noopInferenceIndexMappingManager());
        globalModelRegistry = spy(modelRegistry);
        globalModelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
        registerDefaultEndpoints();
    }

    @After
    private void stopThreadPool() {
        threadPool.close();
    }

    /**
     * Called during test setup to register any default inference endpoints needed by the test subclass.
     */
    protected abstract void registerDefaultEndpoints();

    /** The concrete mapper class produced for this field type */
    protected abstract Class<T> expectedMapperClass();

    /** The concrete field type class produced for this field type */
    protected abstract Class<U> expectedFieldTypeClass();

    protected abstract String contentType();

    protected abstract Set<TaskType> supportedTaskTypes();

    protected abstract IndexVersion getRandomCompatibleIndexVersion();

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY) {
            @Override
            protected Supplier<ModelRegistry> getModelRegistry() {
                return () -> globalModelRegistry;
            }
        }, new XPackClientPlugin(), switch (operationMode) {
            case ENTERPRISE -> VariableLicenseDiskBBQPlugin.ENTERPRISE;
            case BASIC -> VariableLicenseDiskBBQPlugin.BASIC;
            default -> throw new AssertionError("unknown operation mode: " + operationMode);
        });
    }

    @Override
    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    @Override
    protected Object getSampleValueForDocument() {
        return null;
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // These parameters have complex interdependencies (inference endpoints, model types, dense vs sparse)
        // that cannot be expressed through the simple ParameterChecker mechanism. They are covered by
        // dedicated update tests.
        checker.registerIgnoredParameter("inference_id");
        checker.registerIgnoredParameter("search_inference_id");
        checker.registerIgnoredParameter("model_settings");
        checker.registerIgnoredParameter("chunking_settings");
        checker.registerIgnoredParameter("index_options");
    }

    @Override
    public void testEmbeddingsFieldAndFormat() throws IOException {
        MappedFieldType.EmbeddingsField expected = new MappedFieldType.EmbeddingsField(
            new FieldAndFormat("field", SemanticFieldMapper.EMBEDDINGS_FORMAT),
            MappedFieldType.EmbeddingsFieldSource.FIELDS
        );

        // Without model_settings, the field has never seen inference results and skips type validation — every requested vector type
        // is accepted.
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = mapperService.fieldType("field");
        assertEquals(expected, fieldType.embeddingsFieldAndFormat(null));
        for (VectorType vectorType : VectorType.values()) {
            assertEquals(expected, fieldType.embeddingsFieldAndFormat(vectorType));
        }

        // With model_settings, only the matching vector type is accepted.
        for (TaskType taskType : supportedTaskTypes()) {
            String inferenceId = randomAlphaOfLength(8);
            EndpointClusterState modelSettings = createRandomModelSettings(taskType);
            givenModelSettings(inferenceId, modelSettings);
            MapperService msWithSettings = createMapperService(semanticMapping("field", inferenceId, modelSettings));
            MappedFieldType ftWithSettings = msWithSettings.fieldType("field");

            VectorType producedType = VectorType.fromTaskType(taskType);
            assertEquals(expected, ftWithSettings.embeddingsFieldAndFormat(null));
            for (VectorType vectorType : VectorType.values()) {
                if (vectorType != producedType) {
                    assertNull(ftWithSettings.embeddingsFieldAndFormat(vectorType));
                } else {
                    assertEquals(expected, ftWithSettings.embeddingsFieldAndFormat(vectorType));
                }
            }
        }
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("doc_values are not supported in semantic fields", true);
        return null;
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }

    @Override
    protected boolean supportsDocValuesSkippers() {
        return false;
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        // Until a doc is indexed, the query is rewritten as match no docs
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    /** Whether the field type under test uses the legacy {@code semantic_text} storage format. */
    protected boolean useLegacyFormat() {
        return false;
    }

    public void testUpdateInferenceId_GivenNoModelSettings() throws IOException {
        for (int randomizedRun = 0; randomizedRun < 10; randomizedRun++) {
            String fieldName = randomAlphaOfLengthBetween(5, 15);
            String oldInferenceId = randomAlphaOfLengthBetween(5, 15);

            EndpointClusterState oldModelSettings = null;
            if (randomBoolean()) {
                oldModelSettings = createRandomModelSettings();
                givenModelSettings(oldInferenceId, oldModelSettings);
            }

            var mapperService = createSemanticMapperService(semanticMapping(fieldName, oldInferenceId));

            assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
            assertSemanticField(mapperService, fieldName, false, oldModelSettings, null, null);

            String newInferenceId = randomValueOtherThan(oldInferenceId, () -> randomAlphaOfLengthBetween(5, 15));
            EndpointClusterState newModelSettings = null;
            if (randomBoolean()) {
                newModelSettings = createRandomModelSettings();
                givenModelSettings(newInferenceId, newModelSettings);
            }

            merge(mapperService, semanticMapping(fieldName, newInferenceId));

            assertInferenceEndpoints(mapperService, fieldName, newInferenceId, newInferenceId);
            assertSemanticField(mapperService, fieldName, false, newModelSettings, null, null);
        }
    }

    public void testUpdateInferenceId_GivenModelSettings() throws IOException {
        for (int randomizedRun = 0; randomizedRun < 20; randomizedRun++) {
            final String fieldName = randomAlphaOfLengthBetween(5, 15);
            final String oldInferenceId = randomAlphaOfLengthBetween(5, 15);
            final String newInferenceId = randomValueOtherThan(oldInferenceId, () -> randomAlphaOfLengthBetween(5, 15));

            final EndpointClusterState previousModelSettings = createRandomModelSettings();
            givenModelSettings(oldInferenceId, previousModelSettings);

            final MapperService mapperService = createSemanticMapperService(
                semanticMapping(fieldName, oldInferenceId, previousModelSettings)
            );
            assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
            assertSemanticField(mapperService, fieldName, true, previousModelSettings, null, null);

            final CheckedRunnable<IOException> mergeRunner = () -> merge(mapperService, semanticMapping(fieldName, newInferenceId));

            if (randomBoolean()) {
                // Compatible: new endpoint has identical task type / dimensions / similarity / element type
                EndpointClusterState newModelSettings = createCompatibleModelSettings(previousModelSettings);
                givenModelSettings(newInferenceId, newModelSettings);

                mergeRunner.run();
                assertInferenceEndpoints(mapperService, fieldName, newInferenceId, newInferenceId);
                assertSemanticField(mapperService, fieldName, true, newModelSettings, null, null);
            } else {
                final EndpointClusterState incompatibleModelSettings = createIncompatibleModelSettings(previousModelSettings);
                final String expectedErrorMessage;
                if (incompatibleModelSettings == null) {
                    // Incompatible: new endpoint does not exist
                    expectedErrorMessage = "Cannot update ["
                        + contentType()
                        + "] field ["
                        + fieldName
                        + "] because inference endpoint ["
                        + newInferenceId
                        + "] does not exist.";
                } else {
                    // Incompatible: new endpoint exists but its model settings are incompatible
                    givenModelSettings(newInferenceId, incompatibleModelSettings);

                    expectedErrorMessage = "Cannot update ["
                        + contentType()
                        + "] field ["
                        + fieldName
                        + "] because inference endpoint ["
                        + oldInferenceId
                        + "] with model settings ["
                        + previousModelSettings
                        + "] is not compatible with new inference endpoint ["
                        + newInferenceId
                        + "] with model settings ["
                        + incompatibleModelSettings
                        + "]";
                }

                IllegalArgumentException exc = assertThrows(IllegalArgumentException.class, mergeRunner::run);
                assertThat(exc.getMessage(), containsString(expectedErrorMessage));
            }
        }
    }

    protected XContentBuilder semanticMapping(String fieldName, @Nullable String inferenceId) throws IOException {
        return semanticMapping(fieldName, inferenceId, null, null, null, null);
    }

    protected XContentBuilder semanticMapping(String fieldName, @Nullable String inferenceId, @Nullable String searchInferenceId)
        throws IOException {
        return semanticMapping(fieldName, inferenceId, searchInferenceId, null, null, null);
    }

    protected XContentBuilder semanticMapping(String fieldName, @Nullable String inferenceId, @Nullable EndpointClusterState modelSettings)
        throws IOException {
        return semanticMapping(fieldName, inferenceId, null, modelSettings, null, null);
    }

    protected XContentBuilder semanticMapping(
        String fieldName,
        @Nullable String inferenceId,
        @Nullable String searchInferenceId,
        @Nullable EndpointClusterState modelSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable SemanticIndexOptions indexOptions
    ) throws IOException {
        return semanticMapping(fieldName, inferenceId, searchInferenceId, modelSettings, chunkingSettings, indexOptions, null);
    }

    protected XContentBuilder semanticMapping(
        String fieldName,
        @Nullable String inferenceId,
        @Nullable String searchInferenceId,
        @Nullable EndpointClusterState modelSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable SemanticIndexOptions indexOptions,
        @Nullable CheckedConsumer<XContentBuilder, IOException> additionalFields
    ) throws IOException {
        return mapping(
            b -> addSemanticMapping(
                b,
                fieldName,
                inferenceId,
                searchInferenceId,
                modelSettings,
                chunkingSettings,
                indexOptions,
                additionalFields
            )
        );
    }

    protected void addSemanticMapping(
        XContentBuilder mappingBuilder,
        String fieldName,
        @Nullable String inferenceId,
        @Nullable String searchInferenceId,
        @Nullable EndpointClusterState modelSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable SemanticIndexOptions indexOptions
    ) throws IOException {
        addSemanticMapping(mappingBuilder, fieldName, inferenceId, searchInferenceId, modelSettings, chunkingSettings, indexOptions, null);
    }

    protected void addSemanticMapping(
        XContentBuilder mappingBuilder,
        String fieldName,
        @Nullable String inferenceId,
        @Nullable String searchInferenceId,
        @Nullable EndpointClusterState modelSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable SemanticIndexOptions indexOptions,
        @Nullable CheckedConsumer<XContentBuilder, IOException> additionalFields
    ) throws IOException {
        mappingBuilder.startObject(fieldName);
        mappingBuilder.field("type", contentType());
        if (inferenceId != null) {
            mappingBuilder.field(INFERENCE_ID_FIELD, inferenceId);
        }
        if (searchInferenceId != null) {
            mappingBuilder.field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId);
        }
        if (modelSettings != null) {
            mappingBuilder.field(MODEL_SETTINGS_FIELD, modelSettings, EndpointClusterState.withoutEndpointMetadata());
        }
        if (chunkingSettings != null) {
            mappingBuilder.field(CHUNKING_SETTINGS_FIELD, chunkingSettings);
        }
        if (indexOptions != null) {
            mappingBuilder.field(INDEX_OPTIONS_FIELD, indexOptions);
        }
        if (additionalFields != null) {
            additionalFields.accept(mappingBuilder);
        }
        mappingBuilder.endObject();
    }

    protected MapperService createSemanticMapperService(XContentBuilder mappings) throws IOException {
        IndexVersion indexVersion = getRandomCompatibleIndexVersion();
        return createSemanticMapperService(mappings, indexVersion, indexVersion);
    }

    protected MapperService createSemanticMapperService(XContentBuilder mappings, IndexVersion minIndexVersion) throws IOException {
        return createSemanticMapperService(mappings, minIndexVersion, IndexVersion.current());
    }

    protected MapperService createSemanticMapperService(
        XContentBuilder mappings,
        IndexVersion minIndexVersion,
        IndexVersion maxIndexVersion
    ) throws IOException {
        IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(minIndexVersion, maxIndexVersion);
        return createSemanticMapperServiceWithIndexVersion(mappings, indexVersion);
    }

    protected MapperService createSemanticMapperServiceWithIndexVersion(XContentBuilder mappings, IndexVersion indexVersion)
        throws IOException {
        var settings = Settings.builder().put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion).build();
        return createMapperService(indexVersion, settings, mappings);
    }

    /**
     * Creates a (non-legacy) mapper service at the given index version and source mode. With {@code synthetic} or
     * {@code columnar_stored}, the field stores its original input value(s) in the internal binary doc values field; with
     * {@code stored}, the original value is kept in {@code _source}.
     */
    protected MapperService createSemanticMapperServiceWithSourceMode(
        XContentBuilder mappings,
        IndexVersion indexVersion,
        SourceFieldMapper.Mode mode
    ) throws IOException {
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), false)
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), mode)
            .build();
        return createMapperService(indexVersion, settings, mappings);
    }

    protected MapperService createSemanticMapperServiceWithIndexMode(
        XContentBuilder mappings,
        IndexVersion indexVersion,
        IndexMode indexMode
    ) throws IOException {
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), false)
            .put(IndexSettings.MODE.getKey(), indexMode.getName())
            .build();
        return createMapperService(indexVersion, settings, mappings);
    }

    protected TestModel createRandomSupportedModel() {
        return TestModel.createRandomInstance(randomFrom(supportedTaskTypes()));
    }

    protected EndpointClusterState createRandomModelSettings() {
        return new EndpointClusterState(createRandomSupportedModel());
    }

    protected EndpointClusterState createRandomModelSettings(TaskType taskType) {
        return new EndpointClusterState(TestModel.createRandomInstance(taskType));
    }

    protected T getSemanticFieldMapper(MapperService mapperService, String fieldName) {
        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertThat(mapper, instanceOf(expectedMapperClass()));
        return expectedMapperClass().cast(mapper);
    }

    /**
     * Performs a dynamic mapping update on an existing semantic field by parsing a synthetic
     * {@link SemanticTextField} document and applying the resulting {@code dynamicMappingsUpdate}.
     * The mapper service must already have a semantic(_text) mapping for {@code fieldName}.
     */
    protected void performDynamicUpdate(
        MapperService mapperService,
        String fieldName,
        String inferenceId,
        EndpointClusterState modelSettings
    ) throws IOException {
        SemanticTextField semanticTextField = new SemanticTextField(
            useLegacyFormat(),
            fieldName,
            List.of(),
            new SemanticTextField.InferenceResult(inferenceId, modelSettings, null, Map.of()),
            XContentType.JSON
        );
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        if (useLegacyFormat()) {
            builder.field(semanticTextField.fieldName());
            builder.value(semanticTextField);
        } else {
            builder.field(InferenceMetadataFieldsMapper.NAME, Map.of(semanticTextField.fieldName(), semanticTextField));
        }
        builder.endObject();

        SourceToParse sourceToParse = new SourceToParse("test", BytesReference.bytes(builder), XContentType.JSON);
        ParsedDocument parsedDocument = mapperService.documentMapper().parse(sourceToParse);
        mergeDynamicUpdate(mapperService, parsedDocument.dynamicMappingsUpdate());
    }

    /**
     * Asserts the structural invariants of a semantic field mapping: the concrete mapper and field type classes, the nested chunks
     * field, the chunks text/offsets sub-fields, the embeddings sub-mapper, model settings, chunking settings, and index options.
     *
     * @param mapperService The mapper service.
     * @param fieldName The name of the field to check.
     * @param modelSettingsSetOnFieldType Whether model settings should be set on the field type. If true, {@code modelSettings} is
     *                                    required.
     * @param modelSettings The model settings used to validate the embeddings sub-mapper. When null, verifies that the embeddings
     *                      sub-mapper is not configured.
     * @param expectedChunkingSettings The expected chunking settings.
     * @param expectedIndexOptions The expected index options. When null and {@code modelSettings} is provided, the expected default index
     *                             options are automatically determined and validated. When non-null, they must specify the complete index
     *                             options expected, including any element type override.
     */
    protected void assertSemanticField(
        MapperService mapperService,
        String fieldName,
        boolean modelSettingsSetOnFieldType,
        @Nullable EndpointClusterState modelSettings,
        @Nullable ChunkingSettings expectedChunkingSettings,
        @Nullable SemanticIndexOptions expectedIndexOptions
    ) {
        T semanticFieldMapper = getSemanticFieldMapper(mapperService, fieldName);

        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(expectedFieldTypeClass()));
        U semanticFieldType = expectedFieldTypeClass().cast(fieldType);
        assertSame(semanticFieldMapper.fieldType(), semanticFieldType);

        NestedObjectMapper chunksMapper = mapperService.mappingLookup()
            .nestedLookup()
            .getNestedMappers()
            .get(getChunksFieldName(fieldName));
        assertThat(chunksMapper, equalTo(semanticFieldMapper.fieldType().getChunksField()));
        assertThat(chunksMapper.fullPath(), equalTo(getChunksFieldName(fieldName)));
        assertChunksTextField(semanticFieldType, chunksMapper);

        Mapper embeddingsMapper = chunksMapper.getMapper(CHUNKED_EMBEDDINGS_FIELD);
        if (modelSettings != null) {
            SemanticIndexOptions resolvedExpectedIndexOptions = expectedIndexOptions;
            if (resolvedExpectedIndexOptions == null) {
                resolvedExpectedIndexOptions = getDefaultIndexOptions(modelSettings, mapperService);
            }

            assertNotNull(embeddingsMapper);
            assertThat(embeddingsMapper, instanceOf(FieldMapper.class));
            FieldMapper embeddingsFieldMapper = (FieldMapper) embeddingsMapper;
            assertSame(embeddingsFieldMapper.fieldType(), mapperService.mappingLookup().getFieldType(getEmbeddingsFieldName(fieldName)));
            assertThat(embeddingsMapper.fullPath(), equalTo(getEmbeddingsFieldName(fieldName)));
            assertEmbeddingsField(mapperService, embeddingsFieldMapper, modelSettings, resolvedExpectedIndexOptions);
        } else {
            assertNull(embeddingsMapper);
        }

        if (modelSettingsSetOnFieldType) {
            assertNotNull("modelSettings must be provided to check model settings on the field type", modelSettings);
            assertModelSettingsOnFieldType(semanticFieldType, modelSettings);
        } else {
            assertNull(semanticFieldType.getModelSettings());
        }

        if (expectedChunkingSettings != null) {
            assertNotNull(semanticFieldType.getChunkingSettings());
            assertEquals(expectedChunkingSettings, semanticFieldType.getChunkingSettings());
        } else {
            assertNull(semanticFieldType.getChunkingSettings());
        }
    }

    /**
     * Asserts the text/offsets sub-fields under the chunks field. The base implementation covers the (only) format used by the
     * {@code semantic} field: no text sub-field, offsets stored via {@link OffsetSourceFieldMapper}. {@code semantic_text} overrides
     * this to cover its legacy format.
     */
    protected void assertChunksTextField(U fieldType, NestedObjectMapper chunksMapper) {
        assertNull(chunksMapper.getMapper(TEXT_FIELD));
        var offsetMapper = fieldType.getOffsetsField();
        assertThat(offsetMapper, instanceOf(OffsetSourceFieldMapper.class));
    }

    /**
     * Asserts the embeddings sub-mapper against the referenced {@link EndpointClusterState} and expected index options. The base
     * implementation covers the dense task types ({@code text_embedding}, {@code embedding}); {@code semantic_text} overrides this to
     * also cover {@code sparse_embedding}.
     */
    protected void assertEmbeddingsField(
        MapperService mapperService,
        FieldMapper embeddingsMapper,
        EndpointClusterState modelSettings,
        @Nullable SemanticIndexOptions expectedIndexOptions
    ) {
        IndexVersion indexVersion = mapperService.getIndexSettings().getIndexVersionCreated();
        TaskType taskType = modelSettings.taskType();
        if (taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.EMBEDDING) {
            assertThat(embeddingsMapper, instanceOf(DenseVectorFieldMapper.class));
            DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) embeddingsMapper;

            IndexOptions expectedBaseIndexOptions = null;
            DenseVectorFieldMapper.ElementType expectedElementType = modelSettings.elementType();
            if (expectedIndexOptions != null) {
                IndexOptions expectedEmbeddingFieldIndexOptions = expectedIndexOptions.indexOptions();
                if (expectedEmbeddingFieldIndexOptions instanceof ExtendedDenseVectorIndexOptions edvio) {
                    expectedBaseIndexOptions = edvio.getBaseIndexOptions();
                    if (edvio.getElementType() != null) {
                        expectedElementType = edvio.getElementType();
                    }
                } else {
                    expectedBaseIndexOptions = expectedEmbeddingFieldIndexOptions;
                }
            }

            assertEquals(expectedBaseIndexOptions, denseVectorFieldMapper.fieldType().getIndexOptions());
            assertEquals(expectedElementType, denseVectorFieldMapper.fieldType().getElementType());
            assertEquals(modelSettings.dimensions().intValue(), denseVectorFieldMapper.fieldType().getVectorDimensions());
            if (modelSettings.similarity() != null && indexVersion.onOrAfter(NEW_SPARSE_VECTOR)) {
                // We don't set similarity on pre 8.11 indices
                assertEquals(modelSettings.similarity().vectorSimilarity(), denseVectorFieldMapper.fieldType().getSimilarity());
            }
        } else {
            throw new AssertionError("Invalid task type [" + taskType + "]");
        }
    }

    protected void assertModelSettingsOnFieldType(U fieldType, EndpointClusterState modelSettings) {
        EndpointClusterState actual = fieldType.getModelSettings();
        assertNotNull(actual);

        assertEquals(modelSettings.taskType(), actual.taskType());
        assertEquals(modelSettings.dimensions(), actual.dimensions());
        assertEquals(modelSettings.similarity(), actual.similarity());
        assertEquals(modelSettings.elementType(), actual.elementType());

        assertTrue(modelSettings.canMergeWith(actual));
    }

    protected static void assertInferenceEndpoints(
        MapperService mapperService,
        String fieldName,
        String expectedInferenceId,
        String expectedSearchInferenceId
    ) {
        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(SemanticFieldMapper.SemanticFieldType.class));
        SemanticFieldMapper.SemanticFieldType semanticFieldType = (SemanticFieldMapper.SemanticFieldType) fieldType;
        assertEquals(expectedInferenceId, semanticFieldType.getInferenceId());
        assertEquals(expectedSearchInferenceId, semanticFieldType.getSearchInferenceId());
    }

    protected SemanticIndexOptions getDefaultIndexOptions(EndpointClusterState modelSettings, MapperService mapperService) {
        IndexVersion indexVersion = mapperService.getIndexSettings().getIndexVersionCreated();
        boolean experimentalFeatures = DENSE_VECTOR_EXPERIMENTAL_FEATURES_SETTING.get(mapperService.getIndexSettings().getSettings());

        TaskType taskType = modelSettings.taskType();
        DenseVectorFieldMapper.ElementType elementType = modelSettings.elementType();
        Integer dimensions = modelSettings.dimensions();

        return switch (taskType) {
            case TEXT_EMBEDDING, EMBEDDING -> {
                var denseDefaults = getExplicitDenseVectorIndexOptions(modelSettings, indexVersion);
                if (denseDefaults == null) {
                    denseDefaults = defaultDenseVectorIndexOptions(
                        indexVersion,
                        operationMode == License.OperationMode.ENTERPRISE,
                        true,
                        dimensions,
                        elementType,
                        experimentalFeatures
                    );
                }

                DenseVectorFieldMapper.ElementType elementTypeOverride = indexVersion.onOrAfter(SEMANTIC_TEXT_DEFAULTS_TO_BFLOAT16)
                    && elementType == DenseVectorFieldMapper.ElementType.FLOAT ? DenseVectorFieldMapper.ElementType.BFLOAT16 : null;

                yield denseDefaults == null && elementTypeOverride == null
                    ? null
                    : new SemanticIndexOptions(
                        SemanticIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                        new ExtendedDenseVectorIndexOptions(denseDefaults, elementTypeOverride)
                    );
            }
            case SPARSE_EMBEDDING -> {
                var sparseDefaults = SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(indexVersion);
                yield sparseDefaults == null
                    ? null
                    : new SemanticIndexOptions(SemanticIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, sparseDefaults);
            }
            default -> throw new AssertionError("Unexpected task type [" + taskType + "]");
        };
    }

    protected DenseVectorFieldMapper.DenseVectorIndexOptions getExplicitDenseVectorIndexOptions(
        EndpointClusterState modelSettings,
        IndexVersion indexVersion
    ) {
        return null;
    }

    protected void givenModelSettings(String inferenceId, EndpointClusterState modelSettings) {
        when(globalModelRegistry.getEndpointClusterState(inferenceId)).thenReturn(modelSettings);
    }

    /**
     * Creates a {@link EndpointClusterState} that is compatible with {@code base} (same task type,
     * dimensions, similarity, and element type) but with a distinct service name. Compatible settings
     * can be substituted via an inference-ID update.
     */
    protected EndpointClusterState createCompatibleModelSettings(EndpointClusterState base) {
        return new EndpointClusterState(randomAlphaOfLength(4), base.taskType(), base.dimensions(), base.similarity(), base.elementType());
    }

    /**
     * Creates a {@link EndpointClusterState} that is NOT compatible with {@code base}, choosing uniformly among the
     * applicable kinds of incompatibility: task type, dimensions, similarity, element type, or the endpoint not
     * existing. Setting-based perturbations (dimensions/similarity/element type) only apply to dense base models;
     * a sparse base model has null dimensions/similarity/element type, so only task-type and does-not-exist apply.
     *
     * @return new incompatible settings, or {@code null} to indicate the caller should NOT register an endpoint (the does-not-exist case).
     */
    @Nullable
    protected EndpointClusterState createIncompatibleModelSettings(EndpointClusterState base) {
        final DenseVectorFieldMapper.ElementType baseElementType = base.elementType();

        List<IncompatibilityKind> applicable = new ArrayList<>();
        if (supportedTaskTypes().size() > 1) {
            applicable.add(IncompatibilityKind.TASK_TYPE);
        }
        applicable.add(IncompatibilityKind.DOES_NOT_EXIST);
        if (base.taskType() != TaskType.SPARSE_EMBEDDING) {
            applicable.add(IncompatibilityKind.DIMENSIONS);
            applicable.add(IncompatibilityKind.ELEMENT_TYPE);
            // SIMILARITY only when the element type supports more than one option to perturb to
            if (getSupportedSimilarities(baseElementType).size() > 1) {
                applicable.add(IncompatibilityKind.SIMILARITY);
            }
        }

        TaskType taskType = base.taskType();
        Integer dimensions = base.dimensions();
        SimilarityMeasure similarity = base.similarity();
        DenseVectorFieldMapper.ElementType elementType = baseElementType;

        switch (randomFrom(applicable)) {
            case DOES_NOT_EXIST -> {
                return null;
            }
            case TASK_TYPE -> {
                taskType = randomValueOtherThan(taskType, () -> randomFrom(supportedTaskTypes()));
                if (taskType != TaskType.SPARSE_EMBEDDING && base.taskType() == TaskType.SPARSE_EMBEDDING) {
                    // Sparse -> dense: populate required dense settings from a fresh random dense model
                    TestModel randomDenseModel = TestModel.createRandomInstance(taskType);
                    dimensions = randomDenseModel.getServiceSettings().dimensions();
                    similarity = randomDenseModel.getServiceSettings().similarity();
                    elementType = randomDenseModel.getServiceSettings().elementType();
                } else if (taskType == TaskType.SPARSE_EMBEDDING) {
                    // Dense -> sparse: clear dense-only settings
                    dimensions = null;
                    similarity = null;
                    elementType = null;
                }
            }
            case DIMENSIONS -> dimensions = randomValueOtherThan(
                dimensions,
                () -> randomCompatibleDimensions(baseElementType, BBQ_MIN_DIMS * 2)
            );
            case SIMILARITY -> similarity = randomValueOtherThan(similarity, () -> randomFrom(getSupportedSimilarities(baseElementType)));
            case ELEMENT_TYPE -> {
                elementType = randomValueOtherThan(
                    elementType,
                    () -> randomFrom(
                        DenseVectorFieldMapper.ElementType.FLOAT,
                        DenseVectorFieldMapper.ElementType.BYTE,
                        DenseVectorFieldMapper.ElementType.BIT
                    )
                );
                // Regenerate dimensions and similarity compatible with the new element type
                dimensions = randomCompatibleDimensions(elementType, BBQ_MIN_DIMS * 2);
                similarity = randomFrom(getSupportedSimilarities(elementType));
            }
        }

        return new EndpointClusterState(base.service(), taskType, dimensions, similarity, elementType);
    }

    protected static String randomFieldName(int numLevel) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numLevel; i++) {
            if (i > 0) {
                builder.append('.');
            }
            builder.append(randomAlphaOfLengthBetween(5, 15));
        }
        return builder.toString();
    }
}
