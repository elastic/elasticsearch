/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.NestedLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapperTests;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldTypeTests;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.SparseVectorQueryWrapper;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldTypeTests.randomIndexOptionsAll;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldTypeTests.randomSparseVectorIndexOptions;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_EIS_ELSER_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_FALLBACK_ELSER_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_RESCORE_OVERSAMPLE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.INDEX_OPTIONS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.UNSUPPORTED_INDEX_MESSAGE;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettingsOtherThan;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SemanticTextFieldMapperTests extends MapperTestCase {
    private final boolean useLegacyFormat;

    private TestThreadPool threadPool;

    public SemanticTextFieldMapperTests(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    ModelRegistry globalModelRegistry;

    @Before
    private void initializeTestEnvironment() {
        threadPool = createThreadPool();
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        var modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool));
        globalModelRegistry = spy(modelRegistry);
        globalModelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
        registerDefaultEisEndpoint();
    }

    @After
    private void stopThreadPool() {
        threadPool.close();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY) {
            @Override
            protected Supplier<ModelRegistry> getModelRegistry() {
                return () -> globalModelRegistry;
            }
        }, new XPackClientPlugin());
    }

    private void registerDefaultEisEndpoint() {
        globalModelRegistry.putDefaultIdIfAbsent(
            new InferenceService.DefaultConfigId(
                DEFAULT_EIS_ELSER_INFERENCE_ID,
                MinimalServiceSettings.sparseEmbedding(ElasticInferenceService.NAME),
                mock(InferenceService.class)
            )
        );
    }

    private MapperService createMapperService(XContentBuilder mappings, boolean useLegacyFormat) throws IOException {
        IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);
        return createMapperService(mappings, useLegacyFormat, indexVersion, indexVersion);
    }

    private MapperService createMapperService(XContentBuilder mappings, boolean useLegacyFormat, IndexVersion minIndexVersion)
        throws IOException {
        return createMapperService(mappings, useLegacyFormat, minIndexVersion, IndexVersion.current());
    }

    private MapperService createMapperService(
        XContentBuilder mappings,
        boolean useLegacyFormat,
        IndexVersion minIndexVersion,
        IndexVersion maxIndexVersion
    ) throws IOException {
        validateIndexVersion(minIndexVersion, useLegacyFormat);
        IndexVersion indexVersion = IndexVersionUtils.randomVersionBetween(random(), minIndexVersion, maxIndexVersion);
        return createMapperServiceWithIndexVersion(mappings, useLegacyFormat, indexVersion);
    }

    private MapperService createMapperServiceWithIndexVersion(XContentBuilder mappings, boolean useLegacyFormat, IndexVersion indexVersion)
        throws IOException {
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
        return createMapperService(indexVersion, settings, mappings);
    }

    private static void validateIndexVersion(IndexVersion indexVersion, boolean useLegacyFormat) {
        if (useLegacyFormat == false
            && indexVersion.before(IndexVersions.INFERENCE_METADATA_FIELDS)
            && indexVersion.between(IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT, IndexVersions.UPGRADE_TO_LUCENE_10_0_0) == false) {
            throw new IllegalArgumentException("Index version " + indexVersion + " does not support new semantic text format");
        }
    }

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(super.getIndexSettings())
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "semantic_text");
    }

    @Override
    protected void metaMapping(XContentBuilder b) throws IOException {
        super.metaMapping(b);
        b.field(INFERENCE_ID_FIELD, DEFAULT_EIS_ELSER_INFERENCE_ID);
    }

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
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("doc_values are not supported in semantic_text", true);
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
    public MappedFieldType getMappedFieldType() {
        return new SemanticTextFieldMapper.SemanticTextFieldType(
            "field",
            "fake-inference-id",
            null,
            null,
            null,
            null,
            null,
            false,
            Map.of()
        );
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        assertTrue(fieldType.isSearchable());
    }

    public void testDefaults() throws Exception {
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
        final XContentBuilder expectedMapping = fieldMapping(this::metaMapping);

        MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
        DocumentMapper mapper = mapperService.documentMapper();
        assertEquals(Strings.toString(expectedMapping), mapper.mappingSource().toString());
        assertSemanticTextField(mapperService, fieldName, false, null, null);
        assertInferenceEndpoints(mapperService, fieldName, DEFAULT_EIS_ELSER_INFERENCE_ID, DEFAULT_EIS_ELSER_INFERENCE_ID);

        ParsedDocument doc1 = mapper.parse(source(this::writeField));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        // No indexable fields
        assertTrue(fields.isEmpty());
    }

    public void testDefaultInferenceIdUsesEisWhenAvailable() throws Exception {
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);

        MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
        assertInferenceEndpoints(mapperService, fieldName, DEFAULT_EIS_ELSER_INFERENCE_ID, DEFAULT_EIS_ELSER_INFERENCE_ID);
    }

    public void testDefaultInferenceIdFallsBackWhenEisUnavailable() throws Exception {
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);

        removeDefaultEisEndpoint();

        MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
        assertInferenceEndpoints(mapperService, fieldName, DEFAULT_FALLBACK_ELSER_INFERENCE_ID, DEFAULT_FALLBACK_ELSER_INFERENCE_ID);
    }

    private void removeDefaultEisEndpoint() {
        PlainActionFuture<Boolean> removalFuture = new PlainActionFuture<>();
        globalModelRegistry.removeDefaultConfigs(Set.of(DEFAULT_EIS_ELSER_INFERENCE_ID), removalFuture);
        assertTrue("Failed to remove default EIS endpoint", removalFuture.actionGet(TEST_REQUEST_TIMEOUT));
    }

    @Override
    public void testFieldHasValue() {
        MappedFieldType fieldType = getMappedFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName(getEmbeddingsFieldName("field")) });
        assertTrue(fieldType.fieldHasValue(fieldInfos));
    }

    public void testSetInferenceEndpoints() throws IOException {
        final String fieldName = "field";
        final String inferenceId = "foo";
        final String searchInferenceId = "bar";

        CheckedBiConsumer<XContentBuilder, MapperService, IOException> assertSerialization = (expectedMapping, mapperService) -> {
            DocumentMapper mapper = mapperService.documentMapper();
            assertEquals(Strings.toString(expectedMapping), mapper.mappingSource().toString());
        };

        {
            final XContentBuilder fieldMapping = fieldMapping(b -> b.field("type", "semantic_text").field(INFERENCE_ID_FIELD, inferenceId));
            final MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
            assertSerialization.accept(fieldMapping, mapperService);
        }
        {
            final XContentBuilder fieldMapping = fieldMapping(
                b -> b.field("type", "semantic_text").field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId)
            );
            final XContentBuilder expectedMapping = fieldMapping(
                b -> b.field("type", "semantic_text")
                    .field(INFERENCE_ID_FIELD, DEFAULT_EIS_ELSER_INFERENCE_ID)
                    .field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId)
            );
            final MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, DEFAULT_EIS_ELSER_INFERENCE_ID, searchInferenceId);
            assertSerialization.accept(expectedMapping, mapperService);
        }
        {
            final XContentBuilder fieldMapping = fieldMapping(
                b -> b.field("type", "semantic_text")
                    .field(INFERENCE_ID_FIELD, inferenceId)
                    .field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId)
            );
            MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId);
            assertSerialization.accept(fieldMapping, mapperService);
        }
    }

    public void testInvalidInferenceEndpoints() {
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    fieldMapping(b -> b.field("type", "semantic_text").field(INFERENCE_ID_FIELD, (String) null)),
                    useLegacyFormat
                )
            );
            assertThat(
                e.getMessage(),
                containsString("[inference_id] on mapper [field] of type [semantic_text] must not have a [null] value")
            );
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    fieldMapping(b -> b.field("type", "semantic_text").field(INFERENCE_ID_FIELD, "")),
                    useLegacyFormat
                )
            );
            assertThat(e.getMessage(), containsString("[inference_id] on mapper [field] of type [semantic_text] must not be empty"));
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    fieldMapping(b -> b.field("type", "semantic_text").field(SEARCH_INFERENCE_ID_FIELD, "")),
                    useLegacyFormat
                )
            );
            assertThat(e.getMessage(), containsString("[search_inference_id] on mapper [field] of type [semantic_text] must not be empty"));
        }
    }

    private SemanticTextIndexOptions getDefaultSparseVectorIndexOptionsForMapper(MapperService mapperService) {
        var mapperIndexVersion = mapperService.getIndexSettings().getIndexVersionCreated();
        var defaultSparseVectorIndexOptions = SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(mapperIndexVersion);
        return defaultSparseVectorIndexOptions == null
            ? null
            : new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, defaultSparseVectorIndexOptions);
    }

    public void testInvalidTaskTypes() {
        for (var taskType : TaskType.values()) {
            if (taskType == TaskType.TEXT_EMBEDDING || taskType == TaskType.SPARSE_EMBEDDING) {
                continue;
            }
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(
                    fieldMapping(
                        b -> b.field("type", "semantic_text")
                            .field(INFERENCE_ID_FIELD, "test1")
                            .startObject("model_settings")
                            .field("task_type", taskType)
                            .endObject()
                    ),
                    useLegacyFormat
                )
            );
            assertThat(e.getMessage(), containsString("Wrong [task_type], expected text_embedding or sparse_embedding"));
        }
    }

    public void testMultiFieldsSupport() throws IOException {
        if (useLegacyFormat) {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("semantic");
                b.field("type", "semantic_text");
                b.field("inference_id", "my_inference_id");
                b.endObject();
                b.endObject();
            }), useLegacyFormat));
            assertThat(e.getMessage(), containsString("Field [semantic] of type [semantic_text] can't be used in multifields"));
        } else {
            IndexVersion indexVersion = SparseVectorFieldMapperTests.getIndexOptionsCompatibleIndexVersion();
            SparseVectorFieldMapper.SparseVectorIndexOptions expectedIndexOptions = SparseVectorFieldMapper.SparseVectorIndexOptions
                .getDefaultIndexOptions(indexVersion);
            SemanticTextIndexOptions semanticTextIndexOptions = expectedIndexOptions == null
                ? null
                : new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, expectedIndexOptions);
            var mapperService = createMapperServiceWithIndexVersion(fieldMapping(b -> {
                b.field("type", "text");
                b.startObject("fields");
                b.startObject("semantic");
                b.field("type", "semantic_text");
                b.field("inference_id", "my_inference_id");
                b.startObject("model_settings");
                b.field("task_type", "sparse_embedding");
                b.endObject();
                b.endObject();
                b.endObject();
            }), useLegacyFormat, indexVersion);
            assertSemanticTextField(mapperService, "field.semantic", true, null, semanticTextIndexOptions);

            mapperService = createMapperServiceWithIndexVersion(fieldMapping(b -> {
                b.field("type", "semantic_text");
                b.field("inference_id", "my_inference_id");
                b.startObject("model_settings");
                b.field("task_type", "sparse_embedding");
                b.endObject();
                b.startObject("fields");
                b.startObject("text");
                b.field("type", "text");
                b.endObject();
                b.endObject();
            }), useLegacyFormat, indexVersion);
            assertSemanticTextField(mapperService, "field", true, null, semanticTextIndexOptions);

            mapperService = createMapperServiceWithIndexVersion(fieldMapping(b -> {
                b.field("type", "semantic_text");
                b.field("inference_id", "my_inference_id");
                b.startObject("model_settings");
                b.field("task_type", "sparse_embedding");
                b.endObject();
                b.startObject("fields");
                b.startObject("semantic");
                b.field("type", "semantic_text");
                b.field("inference_id", "another_inference_id");
                b.startObject("model_settings");
                b.field("task_type", "sparse_embedding");
                b.endObject();
                b.endObject();
                b.endObject();
            }), useLegacyFormat, indexVersion);
            assertSemanticTextField(mapperService, "field", true, null, semanticTextIndexOptions);
            assertSemanticTextField(mapperService, "field.semantic", true, null, semanticTextIndexOptions);

            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                b.field("type", "semantic_text");
                b.field("inference_id", "my_inference_id");
                b.startObject("fields");
                b.startObject("inference");
                b.field("type", "text");
                b.endObject();
                b.endObject();
            }), useLegacyFormat));
            assertThat(e.getMessage(), containsString("is already used by another field"));
        }
    }

    public void testUpdateInferenceId_GivenPreviousAndNewDoNotExist() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject()),
            useLegacyFormat
        );
        assertInferenceEndpoints(mapperService, fieldName, "test_model", "test_model");
        assertSemanticTextField(mapperService, fieldName, false, null, null);

        merge(
            mapperService,
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "another_model").endObject())
        );

        assertInferenceEndpoints(mapperService, fieldName, "another_model", "another_model");
        assertSemanticTextField(mapperService, fieldName, false, null, null);
    }

    public void testUpdateInferenceId_GivenCurrentHasNoSetModelSettingsAndNewDoesNotExist() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.sparseEmbedding("previous_service");
        givenModelSettings(oldInferenceId, previousModelSettings);
        var mapperService = createMapperService(
            mapping(
                b -> b.startObject(fieldName)
                    .field("type", SemanticTextFieldMapper.CONTENT_TYPE)
                    .field(INFERENCE_ID_FIELD, oldInferenceId)
                    .endObject()
            ),
            useLegacyFormat
        );

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, false, null, null);

        String newInferenceId = "new_inference_id";
        merge(
            mapperService,
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
        );

        assertInferenceEndpoints(mapperService, fieldName, newInferenceId, newInferenceId);
        assertSemanticTextField(mapperService, fieldName, false, null, null);
    }

    public void testUpdateInferenceId_GivenCurrentHasModelSettingsAndNewDoesNotExist() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.sparseEmbedding("previous_service");
        givenModelSettings(oldInferenceId, previousModelSettings);
        var mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        String newInferenceId = "new_inference_id";

        Exception exc = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
            )
        );

        assertThat(
            exc.getMessage(),
            containsString(
                "Cannot update [semantic_text] field ["
                    + fieldName
                    + "] because inference endpoint ["
                    + newInferenceId
                    + "] does not exist."
            )
        );
    }

    public void testUpdateInferenceId_GivenCurrentHasNoModelSettingsAndNewIsIncompatibleTaskType_ShouldSucceed() throws IOException {
        for (int randomizedRun = 0; randomizedRun < 10; randomizedRun++) {
            String fieldName = randomAlphaOfLengthBetween(5, 15);
            String oldInferenceId = "old_inference_id";
            TestModel oldModel = TestModel.createRandomInstance();
            givenModelSettings(oldInferenceId, new MinimalServiceSettings(oldModel));
            var mapperService = createMapperService(
                mapping(
                    b -> b.startObject(fieldName)
                        .field("type", SemanticTextFieldMapper.CONTENT_TYPE)
                        .field(INFERENCE_ID_FIELD, oldInferenceId)
                        .endObject()
                ),
                useLegacyFormat
            );

            assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertEmbeddingsFieldMapperMatchesModel(mapperService, fieldName, oldModel);

            String newInferenceId = "new_inference_id";
            TestModel newModel = randomValueOtherThan(oldModel, TestModel::createRandomInstance);
            givenModelSettings(newInferenceId, new MinimalServiceSettings(newModel));
            merge(
                mapperService,
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
            );

            assertInferenceEndpoints(mapperService, fieldName, newInferenceId, newInferenceId);
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertEmbeddingsFieldMapperMatchesModel(mapperService, fieldName, newModel);
        }
    }

    public void testUpdateInferenceId_GivenCurrentHasSparseModelSettingsAndNewIsCompatible() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.sparseEmbedding("previous_service");
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        String newInferenceId = "new_inference_id";
        MinimalServiceSettings newModelSettings = MinimalServiceSettings.sparseEmbedding("new_service");
        givenModelSettings(newInferenceId, newModelSettings);
        merge(
            mapperService,
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
        );

        assertInferenceEndpoints(mapperService, fieldName, newInferenceId, newInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);
        SemanticTextFieldMapper semanticFieldMapper = getSemanticFieldMapper(mapperService, fieldName);
        assertThat(semanticFieldMapper.fieldType().getModelSettings(), equalTo(newModelSettings));
    }

    public void testUpdateInferenceId_GivenCurrentHasSparseModelSettingsAndNewSetsDefault() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.sparseEmbedding("previous_service");
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        MinimalServiceSettings newModelSettings = MinimalServiceSettings.sparseEmbedding("new_service");
        givenModelSettings(DEFAULT_EIS_ELSER_INFERENCE_ID, newModelSettings);
        merge(mapperService, mapping(b -> b.startObject(fieldName).field("type", "semantic_text").endObject()));

        assertInferenceEndpoints(mapperService, fieldName, DEFAULT_EIS_ELSER_INFERENCE_ID, DEFAULT_EIS_ELSER_INFERENCE_ID);
        assertSemanticTextField(mapperService, fieldName, true, null, null);
        SemanticTextFieldMapper semanticFieldMapper = getSemanticFieldMapper(mapperService, fieldName);
        assertThat(semanticFieldMapper.fieldType().getModelSettings(), equalTo(newModelSettings));
    }

    public void testUpdateInferenceId_GivenCurrentHasSparseModelSettingsAndNewIsIncompatibleTaskType() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.sparseEmbedding("previous_service");
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        String newInferenceId = "new_inference_id";
        MinimalServiceSettings newModelSettings = MinimalServiceSettings.textEmbedding(
            "new_service",
            48,
            SimilarityMeasure.L2_NORM,
            DenseVectorFieldMapper.ElementType.BIT
        );
        givenModelSettings(newInferenceId, newModelSettings);

        Exception exc = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
            )
        );

        assertThat(
            exc.getMessage(),
            containsString(
                "Cannot update [semantic_text] field ["
                    + fieldName
                    + "] because inference endpoint ["
                    + oldInferenceId
                    + "] with model settings ["
                    + previousModelSettings
                    + "] is not compatible with new inference endpoint ["
                    + newInferenceId
                    + "] with model settings ["
                    + newModelSettings
                    + "]"
            )
        );
    }

    public void testUpdateInferenceId_GivenCurrentHasDenseModelSettingsAndNewIsCompatible() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.textEmbedding(
            "previous_service",
            48,
            SimilarityMeasure.L2_NORM,
            DenseVectorFieldMapper.ElementType.BIT
        );
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        String newInferenceId = "new_inference_id";
        MinimalServiceSettings newModelSettings = MinimalServiceSettings.textEmbedding(
            "new_service",
            48,
            SimilarityMeasure.L2_NORM,
            DenseVectorFieldMapper.ElementType.BIT
        );
        givenModelSettings(newInferenceId, newModelSettings);
        merge(
            mapperService,
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
        );

        assertInferenceEndpoints(mapperService, fieldName, newInferenceId, newInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);
        SemanticTextFieldMapper semanticFieldMapper = getSemanticFieldMapper(mapperService, fieldName);
        assertThat(semanticFieldMapper.fieldType().getModelSettings(), equalTo(newModelSettings));
    }

    public void testUpdateInferenceId_GivenCurrentHasDenseModelSettingsAndNewIsIncompatibleTaskType() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.textEmbedding(
            "previous_service",
            48,
            SimilarityMeasure.L2_NORM,
            DenseVectorFieldMapper.ElementType.BIT
        );
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        String newInferenceId = "new_inference_id";
        MinimalServiceSettings newModelSettings = MinimalServiceSettings.sparseEmbedding("new_service");
        givenModelSettings(newInferenceId, newModelSettings);

        Exception exc = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
            )
        );

        assertThat(
            exc.getMessage(),
            containsString(
                "Cannot update [semantic_text] field ["
                    + fieldName
                    + "] because inference endpoint ["
                    + oldInferenceId
                    + "] with model settings ["
                    + previousModelSettings
                    + "] is not compatible with new inference endpoint ["
                    + newInferenceId
                    + "] with model settings ["
                    + newModelSettings
                    + "]"
            )
        );
    }

    public void testUpdateInferenceId_GivenCurrentHasDenseModelSettingsAndNewHasIncompatibleDimensions() throws IOException {
        testUpdateInferenceId_GivenDenseModelsWithDifferentSettings(
            MinimalServiceSettings.textEmbedding("previous_service", 48, SimilarityMeasure.L2_NORM, DenseVectorFieldMapper.ElementType.BIT),
            MinimalServiceSettings.textEmbedding("new_service", 40, SimilarityMeasure.L2_NORM, DenseVectorFieldMapper.ElementType.BIT)
        );
    }

    public void testUpdateInferenceId_GivenCurrentHasDenseModelSettingsAndNewHasIncompatibleSimilarityMeasure() throws IOException {
        testUpdateInferenceId_GivenDenseModelsWithDifferentSettings(
            MinimalServiceSettings.textEmbedding(
                "previous_service",
                48,
                SimilarityMeasure.L2_NORM,
                DenseVectorFieldMapper.ElementType.BYTE
            ),
            MinimalServiceSettings.textEmbedding("new_service", 48, SimilarityMeasure.COSINE, DenseVectorFieldMapper.ElementType.BYTE)
        );
    }

    public void testUpdateInferenceId_GivenCurrentHasDenseModelSettingsAndNewHasIncompatibleElementType() throws IOException {
        testUpdateInferenceId_GivenDenseModelsWithDifferentSettings(
            MinimalServiceSettings.textEmbedding(
                "previous_service",
                48,
                SimilarityMeasure.L2_NORM,
                DenseVectorFieldMapper.ElementType.BYTE
            ),
            MinimalServiceSettings.textEmbedding("new_service", 48, SimilarityMeasure.L2_NORM, DenseVectorFieldMapper.ElementType.BIT)
        );
    }

    public void testUpdateInferenceId_CurrentHasDenseModelSettingsAndNewSetsDefault_ShouldFailAsDefaultIsSparse() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        MinimalServiceSettings previousModelSettings = MinimalServiceSettings.textEmbedding(
            "previous_service",
            48,
            SimilarityMeasure.L2_NORM,
            DenseVectorFieldMapper.ElementType.BIT
        );
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        MinimalServiceSettings newModelSettings = MinimalServiceSettings.sparseEmbedding("new_service");
        givenModelSettings(DEFAULT_EIS_ELSER_INFERENCE_ID, newModelSettings);

        Exception exc = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, mapping(b -> b.startObject(fieldName).field("type", "semantic_text").endObject()))
        );

        assertThat(
            exc.getMessage(),
            containsString(
                "Cannot update [semantic_text] field ["
                    + fieldName
                    + "] because inference endpoint ["
                    + oldInferenceId
                    + "] with model settings ["
                    + previousModelSettings
                    + "] is not compatible with new inference endpoint ["
                    + DEFAULT_EIS_ELSER_INFERENCE_ID
                    + "] with model settings ["
                    + newModelSettings
                    + "]"
            )
        );
    }

    private static void assertEmbeddingsFieldMapperMatchesModel(MapperService mapperService, String fieldName, Model model) {
        Mapper embeddingsFieldMapper = mapperService.mappingLookup().getMapper(getEmbeddingsFieldName(fieldName));
        switch (model.getTaskType()) {
            case SPARSE_EMBEDDING -> assertThat(embeddingsFieldMapper, is(instanceOf(SparseVectorFieldMapper.class)));
            case TEXT_EMBEDDING -> assertTextEmbeddingsFieldMapperMatchesModel(embeddingsFieldMapper, model);
            default -> throw new AssertionError("Unexpected task type [" + model.getTaskType() + "]");
        }
    }

    private static void assertTextEmbeddingsFieldMapperMatchesModel(Mapper embeddingsFieldMapper, Model model) {
        Function<SimilarityMeasure, DenseVectorFieldMapper.VectorSimilarity> convertToVectorSimilarity = s -> switch (s) {
            case COSINE -> DenseVectorFieldMapper.VectorSimilarity.COSINE;
            case DOT_PRODUCT -> DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT;
            case L2_NORM -> DenseVectorFieldMapper.VectorSimilarity.L2_NORM;
        };
        assertThat(embeddingsFieldMapper, is(instanceOf(DenseVectorFieldMapper.class)));
        DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) embeddingsFieldMapper;
        ServiceSettings modelServiceSettings = model.getConfigurations().getServiceSettings();
        assertThat(denseVectorFieldMapper.fieldType().getVectorDimensions(), equalTo(modelServiceSettings.dimensions()));
        assertThat(denseVectorFieldMapper.fieldType().getElementType(), equalTo(modelServiceSettings.elementType()));
        assertThat(
            denseVectorFieldMapper.fieldType().getSimilarity(),
            equalTo(convertToVectorSimilarity.apply(modelServiceSettings.similarity()))
        );
    }

    private void testUpdateInferenceId_GivenDenseModelsWithDifferentSettings(
        MinimalServiceSettings previousModelSettings,
        MinimalServiceSettings newModelSettings
    ) throws IOException {
        assertThat(previousModelSettings.taskType(), equalTo(TaskType.TEXT_EMBEDDING));
        assertThat(newModelSettings.taskType(), equalTo(TaskType.TEXT_EMBEDDING));
        assertThat(newModelSettings, not(equalTo(previousModelSettings)));

        String fieldName = randomAlphaOfLengthBetween(5, 15);
        String oldInferenceId = "old_inference_id";
        givenModelSettings(oldInferenceId, previousModelSettings);
        MapperService mapperService = mapperServiceForFieldWithModelSettings(fieldName, oldInferenceId, previousModelSettings);

        assertInferenceEndpoints(mapperService, fieldName, oldInferenceId, oldInferenceId);
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        String newInferenceId = "new_inference_id";
        givenModelSettings(newInferenceId, newModelSettings);

        Exception exc = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", newInferenceId).endObject())
            )
        );

        assertThat(
            exc.getMessage(),
            containsString(
                "Cannot update [semantic_text] field ["
                    + fieldName
                    + "] because inference endpoint ["
                    + oldInferenceId
                    + "] with model settings ["
                    + previousModelSettings
                    + "] is not compatible with new inference endpoint ["
                    + newInferenceId
                    + "] with model settings ["
                    + newModelSettings
                    + "]"
            )
        );
    }

    public void testDynamicUpdate() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";
        final String searchInferenceId = "search_test_service";

        {
            MapperService mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                new MinimalServiceSettings("service", TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            var expectedIndexOptions = getDefaultSparseVectorIndexOptionsForMapper(mapperService);
            assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }

        {
            MapperService mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                searchInferenceId,
                new MinimalServiceSettings("service", TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            var expectedIndexOptions = getDefaultSparseVectorIndexOptionsForMapper(mapperService);
            assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId);
        }
    }

    public void testUpdateModelSettings() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String fieldName = randomFieldName(depth);
            MapperService mapperService = createMapperService(
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject()),
                useLegacyFormat
            );
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            {
                Exception exc = expectThrows(
                    MapperParsingException.class,
                    () -> merge(
                        mapperService,
                        mapping(
                            b -> b.startObject(fieldName)
                                .field("type", "semantic_text")
                                .field("inference_id", "test_model")
                                .startObject("model_settings")
                                .field("inference_id", "test_model")
                                .endObject()
                                .endObject()
                        )
                    )
                );
                assertThat(exc.getMessage(), containsString("Required [task_type]"));
            }
            {
                merge(
                    mapperService,
                    mapping(
                        b -> b.startObject(fieldName)
                            .field("type", "semantic_text")
                            .field("inference_id", "test_model")
                            .startObject("model_settings")
                            .field("task_type", "sparse_embedding")
                            .endObject()
                            .endObject()
                    )
                );
                var expectedIndexOptions = getDefaultSparseVectorIndexOptionsForMapper(mapperService);
                assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            }
            {
                merge(
                    mapperService,
                    mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject())
                );
                var expectedIndexOptions = getDefaultSparseVectorIndexOptionsForMapper(mapperService);
                assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            }
            {
                Exception exc = expectThrows(
                    IllegalArgumentException.class,
                    () -> merge(
                        mapperService,
                        mapping(
                            b -> b.startObject(fieldName)
                                .field("type", "semantic_text")
                                .field("inference_id", "test_model")
                                .startObject("model_settings")
                                .field("task_type", "text_embedding")
                                .field("dimensions", 10)
                                .field("similarity", "cosine")
                                .field("element_type", "float")
                                .endObject()
                                .endObject()
                        )
                    )
                );
                assertThat(exc.getMessage(), containsString("cannot be changed from type [sparse_vector] to [dense_vector]"));
            }
        }
    }

    public void testDenseVectorIndexOptionValidation() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String inferenceId = "test_model";
            String fieldName = randomFieldName(depth);

            DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions = DenseVectorFieldTypeTests.randomIndexOptionsAll();
            Exception exc = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
                b.startObject(fieldName);
                b.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
                b.field(INFERENCE_ID_FIELD, inferenceId);
                b.startObject(INDEX_OPTIONS_FIELD);
                b.startObject("dense_vector");
                b.field("type", indexOptions.getType().name().toLowerCase(Locale.ROOT));
                b.field("unsupported_param", "any_value");
                b.endObject();
                b.endObject();
                b.endObject();
            }), useLegacyFormat));
            assertTrue(exc.getMessage().contains("unsupported parameters"));
        }
    }

    private void addSparseVectorModelSettingsToBuilder(XContentBuilder b) throws IOException {
        b.startObject("model_settings");
        b.field("task_type", TaskType.SPARSE_EMBEDDING);
        b.endObject();
    }

    public void testSparseVectorIndexOptionsValidationAndMapping() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String inferenceId = "test_model";
            String fieldName = randomFieldName(depth);
            IndexVersion indexVersion = SparseVectorFieldMapperTests.getIndexOptionsCompatibleIndexVersion();
            var sparseVectorIndexOptions = SparseVectorFieldTypeTests.randomSparseVectorIndexOptions();
            var expectedIndexOptions = sparseVectorIndexOptions == null
                ? null
                : new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, sparseVectorIndexOptions);

            // should not throw an exception
            MapperService mapper = createMapperServiceWithIndexVersion(mapping(b -> {
                b.startObject(fieldName);
                {
                    b.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
                    b.field(INFERENCE_ID_FIELD, inferenceId);
                    addSparseVectorModelSettingsToBuilder(b);
                    if (sparseVectorIndexOptions != null) {
                        b.startObject(INDEX_OPTIONS_FIELD);
                        {
                            b.field(SparseVectorFieldMapper.CONTENT_TYPE);
                            sparseVectorIndexOptions.toXContent(b, null);
                        }
                        b.endObject();
                    }
                }
                b.endObject();
            }), useLegacyFormat, indexVersion);

            assertSemanticTextField(mapper, fieldName, true, null, expectedIndexOptions);
        }
    }

    public void testSparseVectorMappingUpdate() throws IOException {
        for (int i = 0; i < 5; i++) {
            Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
            when(globalModelRegistry.getMinimalServiceSettings(anyString())).thenAnswer(
                invocation -> { return new MinimalServiceSettings(model); }
            );

            final ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
            IndexVersion indexVersion = SparseVectorFieldMapperTests.getIndexOptionsCompatibleIndexVersion();
            final SemanticTextIndexOptions indexOptions = randomSemanticTextIndexOptions(TaskType.SPARSE_EMBEDDING);
            String fieldName = "field";

            MapperService mapperService = createMapperServiceWithIndexVersion(
                mapping(b -> addSemanticTextMapping(b, fieldName, model.getInferenceEntityId(), null, chunkingSettings, indexOptions)),
                useLegacyFormat,
                indexVersion
            );
            var expectedIndexOptions = (indexOptions == null)
                ? new SemanticTextIndexOptions(
                    SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR,
                    SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(indexVersion)
                )
                : indexOptions;
            assertSemanticTextField(mapperService, fieldName, false, chunkingSettings, expectedIndexOptions);

            final SemanticTextIndexOptions newIndexOptions = randomSemanticTextIndexOptions(TaskType.SPARSE_EMBEDDING);
            expectedIndexOptions = (newIndexOptions == null)
                ? new SemanticTextIndexOptions(
                    SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR,
                    SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(indexVersion)
                )
                : newIndexOptions;

            ChunkingSettings newChunkingSettings = generateRandomChunkingSettingsOtherThan(chunkingSettings);
            merge(
                mapperService,
                mapping(b -> addSemanticTextMapping(b, fieldName, model.getInferenceEntityId(), null, newChunkingSettings, newIndexOptions))
            );
            assertSemanticTextField(mapperService, fieldName, false, newChunkingSettings, expectedIndexOptions);
        }
    }

    public void testUpdateSearchInferenceId() throws IOException {
        final String inferenceId = "test_inference_id";
        final String searchInferenceId1 = "test_search_inference_id_1";
        final String searchInferenceId2 = "test_search_inference_id_2";

        CheckedBiFunction<String, String, XContentBuilder, IOException> buildMapping = (f, sid) -> mapping(b -> {
            b.startObject(f).field("type", "semantic_text").field("inference_id", inferenceId);
            if (sid != null) {
                b.field("search_inference_id", sid);
            }
            b.endObject();
        });

        for (int depth = 1; depth < 5; depth++) {
            String fieldName = randomFieldName(depth);
            MapperService mapperService = createMapperService(buildMapping.apply(fieldName, null), useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId1));
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId1);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId2));
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId2);

            merge(mapperService, buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, false, null, null);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                new MinimalServiceSettings("my-service", TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            var expectedIndexOptions = getDefaultSparseVectorIndexOptionsForMapper(mapperService);
            assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId1));
            assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId1);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId2));
            assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId2);

            merge(mapperService, buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, true, null, expectedIndexOptions);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }
    }

    private static void assertSemanticTextField(
        MapperService mapperService,
        String fieldName,
        boolean expectedModelSettings,
        ChunkingSettings expectedChunkingSettings,
        SemanticTextIndexOptions expectedIndexOptions
    ) {
        SemanticTextFieldMapper semanticFieldMapper = getSemanticFieldMapper(mapperService, fieldName);

        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType = (SemanticTextFieldMapper.SemanticTextFieldType) fieldType;
        assertSame(semanticFieldMapper.fieldType(), semanticTextFieldType);

        NestedObjectMapper chunksMapper = mapperService.mappingLookup()
            .nestedLookup()
            .getNestedMappers()
            .get(getChunksFieldName(fieldName));
        assertThat(chunksMapper, equalTo(semanticFieldMapper.fieldType().getChunksField()));
        assertThat(chunksMapper.fullPath(), equalTo(getChunksFieldName(fieldName)));

        Mapper textMapper = chunksMapper.getMapper(TEXT_FIELD);
        if (semanticTextFieldType.useLegacyFormat()) {
            assertNotNull(textMapper);
            assertThat(textMapper, instanceOf(KeywordFieldMapper.class));
            KeywordFieldMapper textFieldMapper = (KeywordFieldMapper) textMapper;
            assertThat(textFieldMapper.fieldType().indexType(), equalTo(IndexType.NONE));
        } else {
            assertNull(textMapper);
            var offsetMapper = semanticTextFieldType.getOffsetsField();
            assertThat(offsetMapper, instanceOf(OffsetSourceFieldMapper.class));
        }

        if (expectedModelSettings) {
            assertNotNull(semanticFieldMapper.fieldType().getModelSettings());
            Mapper embeddingsMapper = chunksMapper.getMapper(CHUNKED_EMBEDDINGS_FIELD);
            assertNotNull(embeddingsMapper);
            assertThat(embeddingsMapper, instanceOf(FieldMapper.class));
            FieldMapper embeddingsFieldMapper = (FieldMapper) embeddingsMapper;
            assertSame(embeddingsFieldMapper.fieldType(), mapperService.mappingLookup().getFieldType(getEmbeddingsFieldName(fieldName)));
            assertThat(embeddingsMapper.fullPath(), equalTo(getEmbeddingsFieldName(fieldName)));
            switch (semanticFieldMapper.fieldType().getModelSettings().taskType()) {
                case SPARSE_EMBEDDING -> {
                    assertThat(embeddingsMapper, instanceOf(SparseVectorFieldMapper.class));
                    SparseVectorFieldMapper sparseVectorFieldMapper = (SparseVectorFieldMapper) embeddingsMapper;
                    assertEquals(sparseVectorFieldMapper.fieldType().isStored(), semanticTextFieldType.useLegacyFormat() == false);

                    SparseVectorFieldMapper.SparseVectorIndexOptions applied = sparseVectorFieldMapper.fieldType().getIndexOptions();
                    SparseVectorFieldMapper.SparseVectorIndexOptions expected = expectedIndexOptions == null
                        ? null
                        : (SparseVectorFieldMapper.SparseVectorIndexOptions) expectedIndexOptions.indexOptions();
                    if (expected == null && applied != null) {
                        var indexVersionCreated = mapperService.getIndexSettings().getIndexVersionCreated();
                        if (SparseVectorFieldMapper.SparseVectorIndexOptions.isDefaultOptions(applied, indexVersionCreated)) {
                            expected = SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(indexVersionCreated);
                        }
                    }
                    assertEquals(expected, applied);
                }
                case TEXT_EMBEDDING -> {
                    assertThat(embeddingsMapper, instanceOf(DenseVectorFieldMapper.class));
                    DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) embeddingsMapper;
                    if (expectedIndexOptions != null) {
                        assertEquals(expectedIndexOptions.indexOptions(), denseVectorFieldMapper.fieldType().getIndexOptions());
                    } else {
                        assertNull(denseVectorFieldMapper.fieldType().getIndexOptions());
                    }
                }
                default -> throw new AssertionError("Invalid task type");
            }
        } else {
            assertNull(semanticFieldMapper.fieldType().getModelSettings());
        }

        if (expectedChunkingSettings != null) {
            assertNotNull(semanticFieldMapper.fieldType().getChunkingSettings());
            assertEquals(expectedChunkingSettings, semanticFieldMapper.fieldType().getChunkingSettings());
        } else {
            assertNull(semanticFieldMapper.fieldType().getChunkingSettings());
        }
    }

    private static SemanticTextFieldMapper getSemanticFieldMapper(MapperService mapperService, String fieldName) {
        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertThat(mapper, instanceOf(SemanticTextFieldMapper.class));
        return (SemanticTextFieldMapper) mapper;
    }

    private static void assertInferenceEndpoints(
        MapperService mapperService,
        String fieldName,
        String expectedInferenceId,
        String expectedSearchInferenceId
    ) {
        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType = (SemanticTextFieldMapper.SemanticTextFieldType) fieldType;
        assertEquals(expectedInferenceId, semanticTextFieldType.getInferenceId());
        assertEquals(expectedSearchInferenceId, semanticTextFieldType.getSearchInferenceId());
    }

    public void testSuccessfulParse() throws IOException {
        for (int depth = 1; depth < 4; depth++) {
            final IndexVersion indexVersion = SemanticInferenceMetadataFieldsMapperTests.getRandomCompatibleIndexVersion(useLegacyFormat);

            final String fieldName1 = randomFieldName(depth);
            final String fieldName2 = randomFieldName(depth + 1);
            final String searchInferenceId = randomAlphaOfLength(8);
            final boolean setSearchInferenceId = randomBoolean();

            TaskType taskType = TaskType.SPARSE_EMBEDDING;
            Model model1 = TestModel.createRandomInstance(taskType);
            Model model2 = TestModel.createRandomInstance(taskType);

            when(globalModelRegistry.getMinimalServiceSettings(anyString())).thenAnswer(invocation -> {
                var modelId = (String) invocation.getArguments()[0];
                if (modelId.equals(model1.getInferenceEntityId())) {
                    return new MinimalServiceSettings(model1);
                }
                if (modelId.equals(model2.getInferenceEntityId())) {
                    return new MinimalServiceSettings(model2);
                }
                return null;
            });

            ChunkingSettings chunkingSettings = null; // Some chunking settings configs can produce different Lucene docs counts
            SemanticTextIndexOptions indexOptions = randomSemanticTextIndexOptions(taskType);
            XContentBuilder mapping = mapping(b -> {
                addSemanticTextMapping(
                    b,
                    fieldName1,
                    model1.getInferenceEntityId(),
                    setSearchInferenceId ? searchInferenceId : null,
                    chunkingSettings,
                    indexOptions
                );
                addSemanticTextMapping(
                    b,
                    fieldName2,
                    model2.getInferenceEntityId(),
                    setSearchInferenceId ? searchInferenceId : null,
                    chunkingSettings,
                    indexOptions
                );
            });

            var expectedIndexOptions = (indexOptions == null)
                ? new SemanticTextIndexOptions(
                    SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR,
                    SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(indexVersion)
                )
                : indexOptions;

            MapperService mapperService = createMapperServiceWithIndexVersion(mapping, useLegacyFormat, indexVersion);
            assertSemanticTextField(mapperService, fieldName1, false, null, expectedIndexOptions);
            assertInferenceEndpoints(
                mapperService,
                fieldName1,
                model1.getInferenceEntityId(),
                setSearchInferenceId ? searchInferenceId : model1.getInferenceEntityId()
            );
            assertSemanticTextField(mapperService, fieldName2, false, null, expectedIndexOptions);
            assertInferenceEndpoints(
                mapperService,
                fieldName2,
                model2.getInferenceEntityId(),
                setSearchInferenceId ? searchInferenceId : model2.getInferenceEntityId()
            );

            DocumentMapper documentMapper = mapperService.documentMapper();
            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        useLegacyFormat,
                        b,
                        List.of(
                            randomSemanticText(
                                useLegacyFormat,
                                fieldName1,
                                model1,
                                chunkingSettings,
                                List.of("a b", "c"),
                                XContentType.JSON
                            ),
                            randomSemanticText(useLegacyFormat, fieldName2, model2, chunkingSettings, List.of("d e f"), XContentType.JSON)
                        )
                    )
                )
            );

            List<LuceneDocument> luceneDocs = doc.docs();
            assertEquals(4, luceneDocs.size());
            for (int i = 0; i < 3; i++) {
                assertEquals(doc.rootDoc(), luceneDocs.get(i).getParent());
            }
            // nested docs are in reversed order
            assertSparseFeatures(luceneDocs.get(0), getEmbeddingsFieldName(fieldName1), 2);
            assertSparseFeatures(luceneDocs.get(1), getEmbeddingsFieldName(fieldName1), 1);
            assertSparseFeatures(luceneDocs.get(2), getEmbeddingsFieldName(fieldName2), 3);
            assertEquals(doc.rootDoc(), luceneDocs.get(3));
            assertNull(luceneDocs.get(3).getParent());

            withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), reader -> {
                NestedDocuments nested = new NestedDocuments(
                    mapperService.mappingLookup(),
                    QueryBitSetProducer::new,
                    IndexVersion.current()
                );
                LeafNestedDocuments leaf = nested.getLeafNestedDocuments(reader.leaves().get(0));

                Set<SearchHit.NestedIdentity> visitedNestedIdentities = new HashSet<>();
                Set<SearchHit.NestedIdentity> expectedVisitedNestedIdentities = Set.of(
                    new SearchHit.NestedIdentity(getChunksFieldName(fieldName1), 0, null),
                    new SearchHit.NestedIdentity(getChunksFieldName(fieldName1), 1, null),
                    new SearchHit.NestedIdentity(getChunksFieldName(fieldName2), 0, null)
                );

                assertChildLeafNestedDocument(leaf, 0, 3, visitedNestedIdentities);
                assertChildLeafNestedDocument(leaf, 1, 3, visitedNestedIdentities);
                assertChildLeafNestedDocument(leaf, 2, 3, visitedNestedIdentities);
                assertEquals(expectedVisitedNestedIdentities, visitedNestedIdentities);

                assertNull(leaf.advance(3));
                assertEquals(3, leaf.doc());
                assertEquals(3, leaf.rootDoc());
                assertNull(leaf.nestedIdentity());

                IndexSearcher searcher = newSearcher(reader);
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value());
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a", "b")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value());
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName2, List.of("d")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value());
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName2, List.of("z")),
                        10
                    );
                    assertEquals(0, topDocs.totalHits.value());
                }
            });
        }
    }

    public void testMissingInferenceId() throws IOException {
        final MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, "field", "my_id", null, null, null)),
            useLegacyFormat
        );

        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> mapperService.documentMapper()
                .parse(
                    semanticTextInferenceSource(
                        useLegacyFormat,
                        b -> b.startObject("field")
                            .startObject(INFERENCE_FIELD)
                            .field(
                                MODEL_SETTINGS_FIELD,
                                new MinimalServiceSettings("my-service", TaskType.SPARSE_EMBEDDING, null, null, null)
                            )
                            .field(CHUNKS_FIELD, useLegacyFormat ? List.of() : Map.of())
                            .endObject()
                            .endObject()
                    )
                )
        );
        assertThat(ex.getCause().getMessage(), containsString("Required [inference_id]"));
    }

    public void testMissingModelSettingsAndChunks() throws IOException {
        MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, "field", "my_id", null, null, null)),
            useLegacyFormat
        );
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> mapperService.documentMapper()
                .parse(
                    semanticTextInferenceSource(
                        useLegacyFormat,
                        b -> b.startObject("field").startObject(INFERENCE_FIELD).field(INFERENCE_ID_FIELD, "my_id").endObject().endObject()
                    )
                )
        );
        // Model settings may be null here so we only error on chunks
        assertThat(ex.getCause().getMessage(), containsString("Required [chunks]"));
    }

    public void testMissingTaskType() throws IOException {
        MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, "field", "my_id", null, null, null)),
            useLegacyFormat
        );
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> mapperService.documentMapper()
                .parse(
                    semanticTextInferenceSource(
                        useLegacyFormat,
                        b -> b.startObject("field")
                            .startObject(INFERENCE_FIELD)
                            .field(INFERENCE_ID_FIELD, "my_id")
                            .startObject(MODEL_SETTINGS_FIELD)
                            .endObject()
                            .endObject()
                            .endObject()
                    )
                )
        );
        assertThat(ex.getCause().getMessage(), containsString("failed to parse field [model_settings]"));
    }

    public void testDenseVectorElementType() throws IOException {
        final String fieldName = "field";
        final String inferenceId = "test_service";

        BiConsumer<MapperService, DenseVectorFieldMapper.ElementType> assertMapperService = (m, e) -> {
            SemanticTextFieldMapper semanticTextFieldMapper = getSemanticFieldMapper(m, fieldName);
            assertThat(semanticTextFieldMapper.fieldType().getModelSettings().elementType(), equalTo(e));
        };

        MapperService floatMapperService = mapperServiceForFieldWithModelSettings(
            fieldName,
            inferenceId,
            new MinimalServiceSettings(
                "my-service",
                TaskType.TEXT_EMBEDDING,
                1024,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT
            )
        );
        assertMapperService.accept(floatMapperService, DenseVectorFieldMapper.ElementType.FLOAT);

        MapperService byteMapperService = mapperServiceForFieldWithModelSettings(
            fieldName,
            inferenceId,
            new MinimalServiceSettings(
                "my-service",
                TaskType.TEXT_EMBEDDING,
                1024,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.BYTE
            )
        );
        assertMapperService.accept(byteMapperService, DenseVectorFieldMapper.ElementType.BYTE);
    }

    public void testSettingAndUpdatingChunkingSettings() throws IOException {
        Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        when(globalModelRegistry.getMinimalServiceSettings(anyString())).thenAnswer(
            invocation -> { return new MinimalServiceSettings(model); }
        );

        final ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
        final SemanticTextIndexOptions indexOptions = randomSemanticTextIndexOptions(TaskType.SPARSE_EMBEDDING);
        String fieldName = "field";

        MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, fieldName, model.getInferenceEntityId(), null, chunkingSettings, indexOptions)),
            useLegacyFormat
        );
        assertSemanticTextField(mapperService, fieldName, false, chunkingSettings, indexOptions);

        ChunkingSettings newChunkingSettings = generateRandomChunkingSettingsOtherThan(chunkingSettings);
        merge(
            mapperService,
            mapping(b -> addSemanticTextMapping(b, fieldName, model.getInferenceEntityId(), null, newChunkingSettings, indexOptions))
        );
        assertSemanticTextField(mapperService, fieldName, false, newChunkingSettings, indexOptions);
    }

    public void testModelSettingsRequiredWithChunks() throws IOException {
        // Create inference results where model settings are set to null and chunks are provided
        TaskType taskType = TaskType.SPARSE_EMBEDDING;
        Model model = TestModel.createRandomInstance(taskType);

        when(globalModelRegistry.getMinimalServiceSettings(anyString())).thenAnswer(
            invocation -> { return new MinimalServiceSettings(model); }
        );

        ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
        SemanticTextIndexOptions indexOptions = randomSemanticTextIndexOptions(taskType);
        SemanticTextField randomSemanticText = randomSemanticText(
            useLegacyFormat,
            "field",
            model,
            chunkingSettings,
            List.of("a"),
            XContentType.JSON
        );
        SemanticTextField inferenceResults = new SemanticTextField(
            randomSemanticText.useLegacyFormat(),
            randomSemanticText.fieldName(),
            randomSemanticText.originalValues(),
            new SemanticTextField.InferenceResult(
                randomSemanticText.inference().inferenceId(),
                null,
                randomSemanticText.inference().chunkingSettings(),
                randomSemanticText.inference().chunks()
            ),
            randomSemanticText.contentType()
        );

        MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, "field", model.getInferenceEntityId(), null, chunkingSettings, indexOptions)),
            useLegacyFormat
        );
        SourceToParse source = source(b -> addSemanticTextInferenceResults(useLegacyFormat, b, List.of(inferenceResults)));
        DocumentParsingException ex = expectThrows(
            DocumentParsingException.class,
            DocumentParsingException.class,
            () -> mapperService.documentMapper().parse(source)
        );
        assertThat(ex.getMessage(), containsString("[model_settings] must be set for field [field] when chunks are provided"));
    }

    public void testPre811IndexSemanticTextDenseVectorRaisesError() throws IOException {
        Model model = TestModel.createRandomInstance(TaskType.TEXT_EMBEDDING);
        String fieldName = randomAlphaOfLength(8);

        MapperService mapperService = createMapperService(
            mapping(
                b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", model.getInferenceEntityId()).endObject()
            ),
            true,
            IndexVersions.V_8_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.NEW_SPARSE_VECTOR)
        );
        assertSemanticTextField(mapperService, fieldName, false, null, null);

        merge(
            mapperService,
            mapping(
                b -> b.startObject(fieldName)
                    .field("type", "semantic_text")
                    .field("inference_id", model.getInferenceEntityId())
                    .startObject("model_settings")
                    .field("task_type", TaskType.TEXT_EMBEDDING.toString())
                    .field("dimensions", model.getServiceSettings().dimensions())
                    .field("similarity", model.getServiceSettings().similarity())
                    .field("element_type", model.getServiceSettings().elementType())
                    .endObject()
                    .endObject()
            )
        );
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        DocumentMapper documentMapper = mapperService.documentMapper();
        DocumentParsingException e = assertThrows(
            DocumentParsingException.class,
            () -> documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        true,
                        b,
                        List.of(randomSemanticText(true, fieldName, model, null, List.of("foo", "bar"), XContentType.JSON))
                    )
                )
            )
        );
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
        assertThat(e.getCause().getMessage(), equalTo(UNSUPPORTED_INDEX_MESSAGE));
    }

    public void testPre811IndexSemanticTextSparseVectorRaisesError() throws IOException {
        Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        String fieldName = randomAlphaOfLength(8);

        MapperService mapperService = createMapperService(
            mapping(
                b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", model.getInferenceEntityId()).endObject()
            ),
            true,
            IndexVersions.V_8_0_0,
            IndexVersionUtils.getPreviousVersion(IndexVersions.NEW_SPARSE_VECTOR)
        );
        assertSemanticTextField(mapperService, fieldName, false, null, null);

        merge(
            mapperService,
            mapping(
                b -> b.startObject(fieldName)
                    .field("type", "semantic_text")
                    .field("inference_id", model.getInferenceEntityId())
                    .startObject("model_settings")
                    .field("task_type", TaskType.SPARSE_EMBEDDING.toString())
                    .endObject()
                    .endObject()
            )
        );
        assertSemanticTextField(mapperService, fieldName, true, null, null);

        DocumentMapper documentMapper = mapperService.documentMapper();
        DocumentParsingException e = assertThrows(
            DocumentParsingException.class,
            () -> documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        true,
                        b,
                        List.of(randomSemanticText(true, fieldName, model, null, List.of("foo", "bar"), XContentType.JSON))
                    )
                )
            )
        );
        assertThat(e.getCause(), instanceOf(UnsupportedOperationException.class));
        assertThat(e.getCause().getMessage(), equalTo(UNSUPPORTED_INDEX_MESSAGE));
    }

    private MapperService mapperServiceForFieldWithModelSettings(String fieldName, String inferenceId, MinimalServiceSettings modelSettings)
        throws IOException {
        return mapperServiceForFieldWithModelSettings(fieldName, inferenceId, null, modelSettings);
    }

    private MapperService mapperServiceForFieldWithModelSettings(
        String fieldName,
        String inferenceId,
        String searchInferenceId,
        MinimalServiceSettings modelSettings
    ) throws IOException {
        String mappingParams = "type=semantic_text,inference_id=" + inferenceId;
        if (searchInferenceId != null) {
            mappingParams += ",search_inference_id=" + searchInferenceId;
        }

        MapperService mapperService = createMapperService(mapping(b -> {}), useLegacyFormat);
        mapperService.merge(
            "_doc",
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(fieldName, mappingParams))),
            MapperService.MergeReason.MAPPING_UPDATE
        );

        SemanticTextField semanticTextField = new SemanticTextField(
            useLegacyFormat,
            fieldName,
            List.of(),
            new SemanticTextField.InferenceResult(inferenceId, modelSettings, generateRandomChunkingSettings(), Map.of()),
            XContentType.JSON
        );
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        if (useLegacyFormat) {
            builder.field(semanticTextField.fieldName());
            builder.value(semanticTextField);
        } else {
            builder.field(InferenceMetadataFieldsMapper.NAME, Map.of(semanticTextField.fieldName(), semanticTextField));
        }
        builder.endObject();

        SourceToParse sourceToParse = new SourceToParse("test", BytesReference.bytes(builder), XContentType.JSON);
        ParsedDocument parsedDocument = mapperService.documentMapper().parse(sourceToParse);
        mapperService.merge(
            "_doc",
            parsedDocument.dynamicMappingsUpdate().toCompressedXContent(),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        return mapperService;
    }

    public void testExistsQuerySparseVector() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";

        MapperService mapperService = mapperServiceForFieldWithModelSettings(
            fieldName,
            inferenceId,
            new MinimalServiceSettings("my-service", TaskType.SPARSE_EMBEDDING, null, null, null)
        );

        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertNotNull(mapper);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
        Query existsQuery = ((SemanticTextFieldMapper) mapper).fieldType().existsQuery(searchExecutionContext);
        assertThat(existsQuery, instanceOf(ESToParentBlockJoinQuery.class));
    }

    public void testExistsQueryDenseVector() throws IOException {
        final String fieldName = "semantic";
        final String inferenceId = "test_service";

        MapperService mapperService = mapperServiceForFieldWithModelSettings(
            fieldName,
            inferenceId,
            new MinimalServiceSettings(
                "my-service",
                TaskType.TEXT_EMBEDDING,
                1024,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.FLOAT
            )
        );

        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertNotNull(mapper);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
        Query existsQuery = ((SemanticTextFieldMapper) mapper).fieldType().existsQuery(searchExecutionContext);
        assertThat(existsQuery, instanceOf(ESToParentBlockJoinQuery.class));
    }

    private static DenseVectorFieldMapper.DenseVectorIndexOptions defaultDenseVectorIndexOptions() {
        // These are the default index options for dense_vector fields, and used for semantic_text fields incompatible with BBQ.
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        return new DenseVectorFieldMapper.Int8HnswIndexOptions(m, efConstruction, null, false, null);
    }

    private static SemanticTextIndexOptions defaultDenseVectorSemanticIndexOptions() {
        return new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR, defaultDenseVectorIndexOptions());
    }

    private static DenseVectorFieldMapper.DenseVectorIndexOptions defaultBbqHnswDenseVectorIndexOptions() {
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        DenseVectorFieldMapper.RescoreVector rescoreVector = new DenseVectorFieldMapper.RescoreVector(DEFAULT_RESCORE_OVERSAMPLE);
        return new DenseVectorFieldMapper.BBQHnswIndexOptions(m, efConstruction, false, rescoreVector);
    }

    private static SemanticTextIndexOptions defaultBbqHnswSemanticTextIndexOptions() {
        return new SemanticTextIndexOptions(
            SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
            defaultBbqHnswDenseVectorIndexOptions()
        );
    }

    private static SemanticTextIndexOptions defaultSparseVectorIndexOptions(IndexVersion indexVersion) {
        return new SemanticTextIndexOptions(
            SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR,
            SparseVectorFieldMapper.SparseVectorIndexOptions.getDefaultIndexOptions(indexVersion)
        );
    }

    public void testDefaultIndexOptions() throws IOException {

        // We default to BBQ for eligible dense vectors
        var mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ);
        assertSemanticTextField(mapperService, "field", true, null, defaultBbqHnswSemanticTextIndexOptions());

        // Element types that are incompatible with BBQ will continue to use dense_vector defaults
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "byte");
            b.endObject();
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ);
        assertSemanticTextField(mapperService, "field", true, null, null);

        // A dim count of 10 is too small to support BBQ, so we continue to use dense_vector defaults
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 10);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ);
        assertSemanticTextField(
            mapperService,
            "field",
            true,
            null,
            new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR, defaultDenseVectorIndexOptions())
        );

        // If we explicitly set index options, we respect those over the defaults
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.startObject("index_options");
            b.startObject("dense_vector");
            b.field("type", "int4_hnsw");
            b.field("m", 25);
            b.field("ef_construction", 100);
            b.endObject();
            b.endObject();
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ);
        assertSemanticTextField(
            mapperService,
            "field",
            true,
            null,
            new SemanticTextIndexOptions(
                SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                new DenseVectorFieldMapper.Int4HnswIndexOptions(25, 100, null, false, null)
            )
        );

        // Previous index versions do not set BBQ index options
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
        }),
            useLegacyFormat,
            IndexVersions.INFERENCE_METADATA_FIELDS,
            IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ)
        );
        assertSemanticTextField(mapperService, "field", true, null, defaultDenseVectorSemanticIndexOptions());

        // 8.x index versions that use backported default BBQ set default BBQ index options as expected
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
        }),
            useLegacyFormat,
            IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X,
            IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0)
        );
        assertSemanticTextField(mapperService, "field", true, null, defaultBbqHnswSemanticTextIndexOptions());

        // Previous 8.x index versions do not set BBQ index options
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
        }),
            useLegacyFormat,
            IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT,
            IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X)
        );
        assertSemanticTextField(mapperService, "field", true, null, defaultDenseVectorSemanticIndexOptions());

        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "sparse_embedding");
            b.endObject();
        }),
            useLegacyFormat,
            IndexVersionUtils.getPreviousVersion(IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT),
            IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT
        );

        assertSemanticTextField(
            mapperService,
            "field",
            true,
            null,
            defaultSparseVectorIndexOptions(mapperService.getIndexSettings().getIndexVersionCreated())
        );
    }

    public void testSparseVectorIndexOptionsDefaultsBeforeSupport() throws IOException {
        var mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "sparse_embedding");
            b.endObject();
        }),
            useLegacyFormat,
            IndexVersions.INFERENCE_METADATA_FIELDS,
            IndexVersionUtils.getPreviousVersion(IndexVersions.SPARSE_VECTOR_PRUNING_INDEX_OPTIONS_SUPPORT)
        );

        assertSemanticTextField(mapperService, "field", true, null, null);
    }

    public void testSpecifiedDenseVectorIndexOptions() throws IOException {

        // Specifying index options will override default index option settings
        var mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.startObject("index_options");
            b.startObject("dense_vector");
            b.field("type", "int4_hnsw");
            b.field("m", 20);
            b.field("ef_construction", 90);
            b.field("confidence_interval", 0.4);
            b.endObject();
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT);
        assertSemanticTextField(
            mapperService,
            "field",
            true,
            null,
            new SemanticTextIndexOptions(
                SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                new DenseVectorFieldMapper.Int4HnswIndexOptions(20, 90, 0.4f, false, null)
            )
        );

        // Specifying partial index options will in the remainder index options with defaults
        mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.startObject("index_options");
            b.startObject("dense_vector");
            b.field("type", "int4_hnsw");
            b.endObject();
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT);
        assertSemanticTextField(
            mapperService,
            "field",
            true,
            null,
            new SemanticTextIndexOptions(
                SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR,
                new DenseVectorFieldMapper.Int4HnswIndexOptions(16, 100, 0f, false, null)
            )
        );

        // Incompatible index options will fail
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "sparse_embedding");
            b.endObject();
            b.startObject("index_options");
            b.startObject("dense_vector");
            b.field("type", "int8_hnsw");
            b.endObject();
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT));
        assertThat(e.getMessage(), containsString("Invalid task type"));

        e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.startObject("index_options");
            b.startObject("dense_vector");
            b.field("type", "bbq_flat");
            b.field("ef_construction", 100);
            b.endObject();
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT));
        assertThat(e.getMessage(), containsString("unsupported parameters:  [ef_construction : 100]"));

        e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "semantic_text");
            b.field("inference_id", "another_inference_id");
            b.startObject("model_settings");
            b.field("task_type", "text_embedding");
            b.field("dimensions", 100);
            b.field("similarity", "cosine");
            b.field("element_type", "float");
            b.endObject();
            b.startObject("index_options");
            b.startObject("dense_vector");
            b.field("type", "invalid");
            b.endObject();
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT));
        assertThat(e.getMessage(), containsString("Unsupported index options type invalid"));
    }

    public void testSpecificSparseVectorIndexOptions() throws IOException {
        for (int i = 0; i < 10; i++) {
            SparseVectorFieldMapper.SparseVectorIndexOptions testIndexOptions = randomSparseVectorIndexOptions(false);
            var mapperService = createMapperService(fieldMapping(b -> {
                b.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
                b.field(INFERENCE_ID_FIELD, "test_inference_id");
                addSparseVectorModelSettingsToBuilder(b);
                b.startObject(INDEX_OPTIONS_FIELD);
                {
                    b.field(SparseVectorFieldMapper.CONTENT_TYPE);
                    testIndexOptions.toXContent(b, null);
                }
                b.endObject();
            }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT);

            assertSemanticTextField(
                mapperService,
                "field",
                true,
                null,
                new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR, testIndexOptions)
            );
        }
    }

    public void testSparseVectorIndexOptionsValidations() throws IOException {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
            b.field(INFERENCE_ID_FIELD, "test_inference_id");
            b.startObject(INDEX_OPTIONS_FIELD);
            {
                b.startObject(SparseVectorFieldMapper.CONTENT_TYPE);
                {
                    b.field("prune", false);
                    b.startObject("pruning_config");
                    {
                        b.field(TokenPruningConfig.TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName(), 5.0f);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT));
        assertThat(e.getMessage(), containsString("failed to parse field [pruning_config]"));

        e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
            b.field(INFERENCE_ID_FIELD, "test_inference_id");
            b.startObject(INDEX_OPTIONS_FIELD);
            {
                b.startObject(SparseVectorFieldMapper.CONTENT_TYPE);
                {
                    b.field("prune", true);
                    b.startObject("pruning_config");
                    {
                        b.field(TokenPruningConfig.TOKENS_FREQ_RATIO_THRESHOLD.getPreferredName(), 1000.0f);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }), useLegacyFormat, IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT));
        var innerClause = e.getCause().getCause().getCause().getCause();
        assertThat(innerClause.getMessage(), containsString("[tokens_freq_ratio_threshold] must be between [1] and [100], got 1000.0"));
    }

    public static SemanticTextIndexOptions randomSemanticTextIndexOptions() {
        TaskType taskType = randomFrom(TaskType.SPARSE_EMBEDDING, TaskType.TEXT_EMBEDDING);
        return randomSemanticTextIndexOptions(taskType);
    }

    public static SemanticTextIndexOptions randomSemanticTextIndexOptions(TaskType taskType) {
        if (taskType == TaskType.TEXT_EMBEDDING) {
            return randomBoolean()
                ? null
                : new SemanticTextIndexOptions(SemanticTextIndexOptions.SupportedIndexOptions.DENSE_VECTOR, randomIndexOptionsAll());
        }

        if (taskType == TaskType.SPARSE_EMBEDDING) {
            return randomBoolean()
                ? null
                : new SemanticTextIndexOptions(
                    SemanticTextIndexOptions.SupportedIndexOptions.SPARSE_VECTOR,
                    randomSparseVectorIndexOptions(false)
                );
        }

        return null;
    }

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        // Until a doc is indexed, the query is rewritten as match no docs
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    private static void addSemanticTextMapping(
        XContentBuilder mappingBuilder,
        String fieldName,
        String inferenceId,
        String searchInferenceId,
        ChunkingSettings chunkingSettings,
        SemanticTextIndexOptions indexOptions
    ) throws IOException {
        mappingBuilder.startObject(fieldName);
        mappingBuilder.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mappingBuilder.field("inference_id", inferenceId);
        if (searchInferenceId != null) {
            mappingBuilder.field("search_inference_id", searchInferenceId);
        }
        if (chunkingSettings != null) {
            mappingBuilder.startObject("chunking_settings");
            mappingBuilder.mapContents(chunkingSettings.asMap());
            mappingBuilder.endObject();
        }
        if (indexOptions != null) {
            mappingBuilder.field(INDEX_OPTIONS_FIELD);
            indexOptions.toXContent(mappingBuilder, null);
        }
        mappingBuilder.endObject();
    }

    public static void addSemanticTextInferenceResults(
        boolean useLegacyFormat,
        XContentBuilder sourceBuilder,
        List<SemanticTextField> semanticTextInferenceResults
    ) throws IOException {
        if (useLegacyFormat) {
            for (var field : semanticTextInferenceResults) {
                sourceBuilder.field(field.fieldName());
                sourceBuilder.value(field);
            }
        } else {
            // Use a linked hash map to maintain insertion-order iteration over the inference fields
            Map<String, Object> inferenceMetadataFields = new LinkedHashMap<>();
            for (var field : semanticTextInferenceResults) {
                inferenceMetadataFields.put(field.fieldName(), field);
            }
            sourceBuilder.field(InferenceMetadataFieldsMapper.NAME, inferenceMetadataFields);
        }
    }

    static String randomFieldName(int numLevel) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numLevel; i++) {
            if (i > 0) {
                builder.append('.');
            }
            builder.append(randomAlphaOfLengthBetween(5, 15));
        }
        return builder.toString();
    }

    private static Query generateNestedTermSparseVectorQuery(NestedLookup nestedLookup, String fieldName, List<String> tokens) {
        NestedObjectMapper mapper = nestedLookup.getNestedMappers().get(getChunksFieldName(fieldName));
        assertNotNull(mapper);

        BitSetProducer parentFilter = new QueryBitSetProducer(Queries.newNonNestedFilter(IndexVersion.current()));
        BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
        for (String token : tokens) {
            queryBuilder.add(
                new BooleanClause(new TermQuery(new Term(getEmbeddingsFieldName(fieldName), token)), BooleanClause.Occur.MUST)
            );
        }
        queryBuilder.add(new BooleanClause(mapper.nestedTypeFilter(), BooleanClause.Occur.FILTER));

        return new ESToParentBlockJoinQuery(
            new SparseVectorQueryWrapper(fieldName, queryBuilder.build()),
            parentFilter,
            ScoreMode.Total,
            null
        );
    }

    private static SourceToParse semanticTextInferenceSource(boolean useLegacyFormat, CheckedConsumer<XContentBuilder, IOException> build)
        throws IOException {
        return source(b -> {
            if (useLegacyFormat == false) {
                b.startObject(InferenceMetadataFieldsMapper.NAME);
            }
            build.accept(b);
            if (useLegacyFormat == false) {
                b.endObject();
            }
        });
    }

    private static void assertChildLeafNestedDocument(
        LeafNestedDocuments leaf,
        int advanceToDoc,
        int expectedRootDoc,
        Set<SearchHit.NestedIdentity> visitedNestedIdentities
    ) throws IOException {

        assertNotNull(leaf.advance(advanceToDoc));
        assertEquals(advanceToDoc, leaf.doc());
        assertEquals(expectedRootDoc, leaf.rootDoc());
        assertNotNull(leaf.nestedIdentity());
        visitedNestedIdentities.add(leaf.nestedIdentity());
    }

    private static void assertSparseFeatures(LuceneDocument doc, String fieldName, int expectedCount) {
        int count = 0;
        for (IndexableField field : doc.getFields()) {
            if (field instanceof FeatureField featureField) {
                assertThat(featureField.name(), equalTo(fieldName));
                ++count;
            }
        }
        assertThat(count, equalTo(expectedCount));
    }

    private void givenModelSettings(String inferenceId, MinimalServiceSettings modelSettings) {
        when(globalModelRegistry.getMinimalServiceSettings(inferenceId)).thenReturn(modelSettings);
    }

    @Override
    protected List<SortShortcutSupport> getSortShortcutSupport() {
        return List.of();
    }
}
