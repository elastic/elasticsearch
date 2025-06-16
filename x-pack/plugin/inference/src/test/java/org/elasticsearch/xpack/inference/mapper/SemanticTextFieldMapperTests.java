/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
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
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.XFeatureField;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
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
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_ELSER_2_INFERENCE_ID;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettingsOtherThan;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SemanticTextFieldMapperTests extends MapperTestCase {
    private final boolean useLegacyFormat;

    private TestThreadPool threadPool;

    public SemanticTextFieldMapperTests(boolean useLegacyFormat) {
        this.useLegacyFormat = useLegacyFormat;
    }

    @Before
    private void startThreadPool() {
        threadPool = createThreadPool();
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
        var clusterService = ClusterServiceUtils.createClusterService(threadPool);
        var modelRegistry = new ModelRegistry(clusterService, new NoOpClient(threadPool));
        modelRegistry.clusterChanged(new ClusterChangedEvent("init", clusterService.state(), clusterService.state()) {
            @Override
            public boolean localNodeMaster() {
                return false;
            }
        });
        return List.of(new InferencePlugin(Settings.EMPTY) {
            @Override
            protected Supplier<ModelRegistry> getModelRegistry() {
                return () -> modelRegistry;
            }
        }, new XPackClientPlugin());
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
        var settings = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), indexVersion)
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
        return createMapperService(indexVersion, settings, mappings);
    }

    private static void validateIndexVersion(IndexVersion indexVersion, boolean useLegacyFormat) {
        if (useLegacyFormat == false && indexVersion.before(IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT)) {
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
        b.field(INFERENCE_ID_FIELD, DEFAULT_ELSER_2_INFERENCE_ID);
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
        return new SemanticTextFieldMapper.SemanticTextFieldType("field", "fake-inference-id", null, null, null, null, false, Map.of());
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        assertTrue(fieldType.isIndexed());
        assertTrue(fieldType.isSearchable());
    }

    public void testDefaults() throws Exception {
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
        final XContentBuilder expectedMapping = fieldMapping(this::metaMapping);

        MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
        DocumentMapper mapper = mapperService.documentMapper();
        assertEquals(Strings.toString(expectedMapping), mapper.mappingSource().toString());
        assertSemanticTextField(mapperService, fieldName, false);
        assertInferenceEndpoints(mapperService, fieldName, DEFAULT_ELSER_2_INFERENCE_ID, DEFAULT_ELSER_2_INFERENCE_ID);

        ParsedDocument doc1 = mapper.parse(source(this::writeField));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        // No indexable fields
        assertTrue(fields.isEmpty());
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
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
            assertSerialization.accept(fieldMapping, mapperService);
        }
        {
            final XContentBuilder fieldMapping = fieldMapping(
                b -> b.field("type", "semantic_text").field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId)
            );
            final XContentBuilder expectedMapping = fieldMapping(
                b -> b.field("type", "semantic_text")
                    .field(INFERENCE_ID_FIELD, DEFAULT_ELSER_2_INFERENCE_ID)
                    .field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId)
            );
            final MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, DEFAULT_ELSER_2_INFERENCE_ID, searchInferenceId);
            assertSerialization.accept(expectedMapping, mapperService);
        }
        {
            final XContentBuilder fieldMapping = fieldMapping(
                b -> b.field("type", "semantic_text")
                    .field(INFERENCE_ID_FIELD, inferenceId)
                    .field(SEARCH_INFERENCE_ID_FIELD, searchInferenceId)
            );
            MapperService mapperService = createMapperService(fieldMapping, useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName, false);
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
            assertThat(e.getMessage(), containsString("Failed to parse mapping: Wrong [task_type]"));
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
            var mapperService = createMapperService(fieldMapping(b -> {
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
            }), useLegacyFormat);
            assertSemanticTextField(mapperService, "field.semantic", true);

            mapperService = createMapperService(fieldMapping(b -> {
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
            }), useLegacyFormat);
            assertSemanticTextField(mapperService, "field", true);

            mapperService = createMapperService(fieldMapping(b -> {
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
            }), useLegacyFormat);
            assertSemanticTextField(mapperService, "field", true);
            assertSemanticTextField(mapperService, "field.semantic", true);

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

    public void testUpdatesToInferenceIdNotSupported() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject()),
            useLegacyFormat
        );
        assertSemanticTextField(mapperService, fieldName, false);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(
                mapperService,
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "another_model").endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Cannot update parameter [inference_id] from [test_model] to [another_model]"));
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
            assertSemanticTextField(mapperService, fieldName, true);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }

        {
            MapperService mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                searchInferenceId,
                new MinimalServiceSettings("service", TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            assertSemanticTextField(mapperService, fieldName, true);
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
            assertSemanticTextField(mapperService, fieldName, false);
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
                assertSemanticTextField(mapperService, fieldName, true);
            }
            {
                merge(
                    mapperService,
                    mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject())
                );
                assertSemanticTextField(mapperService, fieldName, true);
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
                assertThat(
                    exc.getMessage(),
                    containsString(
                        "Cannot update parameter [model_settings] "
                            + "from [service=null, task_type=sparse_embedding] "
                            + "to [service=null, task_type=text_embedding, dimensions=10, similarity=cosine, element_type=float]"
                    )
                );
            }
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
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId1));
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId1);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId2));
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId2);

            merge(mapperService, buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                new MinimalServiceSettings("my-service", TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            assertSemanticTextField(mapperService, fieldName, true);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId1));
            assertSemanticTextField(mapperService, fieldName, true);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId1);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId2));
            assertSemanticTextField(mapperService, fieldName, true);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId2);

            merge(mapperService, buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, true);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }
    }

    private static void assertSemanticTextField(MapperService mapperService, String fieldName, boolean expectedModelSettings) {
        assertSemanticTextField(mapperService, fieldName, expectedModelSettings, null, null);
    }

    private static void assertSemanticTextField(
        MapperService mapperService,
        String fieldName,
        boolean expectedModelSettings,
        ChunkingSettings expectedChunkingSettings,
        DenseVectorFieldMapper.IndexOptions expectedIndexOptions
    ) {
        Mapper mapper = mapperService.mappingLookup().getMapper(fieldName);
        assertNotNull(mapper);
        assertThat(mapper, instanceOf(SemanticTextFieldMapper.class));
        SemanticTextFieldMapper semanticFieldMapper = (SemanticTextFieldMapper) mapper;

        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType = (SemanticTextFieldMapper.SemanticTextFieldType) fieldType;
        assertTrue(semanticFieldMapper.fieldType() == semanticTextFieldType);

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
            assertFalse(textFieldMapper.fieldType().isIndexed());
            assertFalse(textFieldMapper.fieldType().hasDocValues());
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
            assertTrue(embeddingsFieldMapper.fieldType() == mapperService.mappingLookup().getFieldType(getEmbeddingsFieldName(fieldName)));
            assertThat(embeddingsMapper.fullPath(), equalTo(getEmbeddingsFieldName(fieldName)));
            switch (semanticFieldMapper.fieldType().getModelSettings().taskType()) {
                case SPARSE_EMBEDDING -> {
                    assertThat(embeddingsMapper, instanceOf(SparseVectorFieldMapper.class));
                    SparseVectorFieldMapper sparseMapper = (SparseVectorFieldMapper) embeddingsMapper;
                    assertEquals(sparseMapper.fieldType().isStored(), semanticTextFieldType.useLegacyFormat() == false);
                    assertNull(expectedIndexOptions);
                }
                case TEXT_EMBEDDING -> {
                    assertThat(embeddingsMapper, instanceOf(DenseVectorFieldMapper.class));
                    DenseVectorFieldMapper denseVectorFieldMapper = (DenseVectorFieldMapper) embeddingsMapper;
                    if (expectedIndexOptions != null) {
                        assertEquals(expectedIndexOptions, denseVectorFieldMapper.fieldType().getIndexOptions());
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
            final String fieldName1 = randomFieldName(depth);
            final String fieldName2 = randomFieldName(depth + 1);
            final String searchInferenceId = randomAlphaOfLength(8);
            final boolean setSearchInferenceId = randomBoolean();

            Model model1 = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
            Model model2 = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
            ChunkingSettings chunkingSettings = null; // Some chunking settings configs can produce different Lucene docs counts
            XContentBuilder mapping = mapping(b -> {
                addSemanticTextMapping(
                    b,
                    fieldName1,
                    model1.getInferenceEntityId(),
                    setSearchInferenceId ? searchInferenceId : null,
                    chunkingSettings
                );
                addSemanticTextMapping(
                    b,
                    fieldName2,
                    model2.getInferenceEntityId(),
                    setSearchInferenceId ? searchInferenceId : null,
                    chunkingSettings
                );
            });

            MapperService mapperService = createMapperService(mapping, useLegacyFormat);
            assertSemanticTextField(mapperService, fieldName1, false);
            assertInferenceEndpoints(
                mapperService,
                fieldName1,
                model1.getInferenceEntityId(),
                setSearchInferenceId ? searchInferenceId : model1.getInferenceEntityId()
            );
            assertSemanticTextField(mapperService, fieldName2, false);
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
                    assertEquals(1, topDocs.totalHits.value);
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName1, List.of("a", "b")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value);
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName2, List.of("d")),
                        10
                    );
                    assertEquals(1, topDocs.totalHits.value);
                    assertEquals(3, topDocs.scoreDocs[0].doc);
                }
                {
                    TopDocs topDocs = searcher.search(
                        generateNestedTermSparseVectorQuery(mapperService.mappingLookup().nestedLookup(), fieldName2, List.of("z")),
                        10
                    );
                    assertEquals(0, topDocs.totalHits.value);
                }
            });
        }
    }

    public void testMissingInferenceId() throws IOException {
        final MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, "field", "my_id", null, null)),
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
            mapping(b -> addSemanticTextMapping(b, "field", "my_id", null, null)),
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
            mapping(b -> addSemanticTextMapping(b, "field", "my_id", null, null)),
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
            Mapper mapper = m.mappingLookup().getMapper(fieldName);
            assertThat(mapper, instanceOf(SemanticTextFieldMapper.class));
            SemanticTextFieldMapper semanticTextFieldMapper = (SemanticTextFieldMapper) mapper;
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
        final ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
        String fieldName = "field";

        SemanticTextField randomSemanticText = randomSemanticText(
            useLegacyFormat,
            fieldName,
            model,
            chunkingSettings,
            List.of("a"),
            XContentType.JSON
        );

        MapperService mapperService = createMapperService(
            mapping(b -> addSemanticTextMapping(b, fieldName, model.getInferenceEntityId(), null, chunkingSettings)),
            useLegacyFormat
        );
        assertSemanticTextField(mapperService, fieldName, false, chunkingSettings, null);

        ChunkingSettings newChunkingSettings = generateRandomChunkingSettingsOtherThan(chunkingSettings);
        merge(mapperService, mapping(b -> addSemanticTextMapping(b, fieldName, model.getInferenceEntityId(), null, newChunkingSettings)));
        assertSemanticTextField(mapperService, fieldName, false, newChunkingSettings, null);
    }

    public void testModelSettingsRequiredWithChunks() throws IOException {
        // Create inference results where model settings are set to null and chunks are provided
        Model model = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        ChunkingSettings chunkingSettings = generateRandomChunkingSettings(false);
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
            mapping(b -> addSemanticTextMapping(b, "field", model.getInferenceEntityId(), null, chunkingSettings)),
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

    private static DenseVectorFieldMapper.IndexOptions defaultDenseVectorIndexOptions() {
        // These are the default index options for dense_vector fields, and used for semantic_text fields incompatible with BBQ.
        int m = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
        int efConstruction = Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
        return new DenseVectorFieldMapper.Int8HnswIndexOptions(m, efConstruction, null, null);
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
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X);
        assertSemanticTextField(mapperService, "field", true, null, SemanticTextFieldMapper.defaultSemanticDenseIndexOptions());

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
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X);
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
        }), useLegacyFormat, IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X);
        assertSemanticTextField(mapperService, "field", true, null, defaultDenseVectorIndexOptions());

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
            IndexVersions.INFERENCE_METADATA_FIELDS_BACKPORT,
            IndexVersionUtils.getPreviousVersion(IndexVersions.SEMANTIC_TEXT_DEFAULTS_TO_BBQ_BACKPORT_8_X)
        );
        assertSemanticTextField(mapperService, "field", true, null, defaultDenseVectorIndexOptions());

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
        ChunkingSettings chunkingSettings
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
            if (field instanceof XFeatureField featureField) {
                assertThat(featureField.name(), equalTo(fieldName));
                ++count;
            }
        }
        assertThat(count, equalTo(expectedCount));
    }
}
