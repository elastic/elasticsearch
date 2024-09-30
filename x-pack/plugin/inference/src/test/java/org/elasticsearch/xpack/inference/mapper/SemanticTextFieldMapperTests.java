/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

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
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.FieldMapper;
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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.LeafNestedDocuments;
import org.elasticsearch.search.NestedDocuments;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SemanticTextFieldMapperTests extends MapperTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "semantic_text").field("inference_id", "test_model");
    }

    @Override
    protected String minimalIsInvalidRoutingPathErrorMessage(Mapper mapper) {
        return "cannot have nested fields when index is in [index.mode=time_series]";
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
            IndexVersion.current(),
            Map.of()
        );
    }

    @Override
    protected void assertSearchable(MappedFieldType fieldType) {
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        assertTrue(fieldType.isIndexed());
        assertTrue(fieldType.isSearchable());
    }

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

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

    public void testInferenceIdNotPresent() {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "semantic_text")))
        );
        assertThat(e.getMessage(), containsString("field [inference_id] must be specified"));
    }

    public void testCannotBeUsedInMultiFields() {
        Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("semantic");
            b.field("type", "semantic_text");
            b.field("inference_id", "my_inference_id");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("Field [semantic] of type [semantic_text] can't be used in multifields"));
    }

    public void testUpdatesToInferenceIdNotSupported() throws IOException {
        String fieldName = randomAlphaOfLengthBetween(5, 15);
        MapperService mapperService = createMapperService(
            mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject())
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
                new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            assertSemanticTextField(mapperService, fieldName, true);
            assertSearchInferenceId(mapperService, fieldName, inferenceId);
        }

        {
            MapperService mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                searchInferenceId,
                new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            assertSemanticTextField(mapperService, fieldName, true);
            assertSearchInferenceId(mapperService, fieldName, searchInferenceId);
        }
    }

    public void testUpdateModelSettings() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String fieldName = randomFieldName(depth);
            MapperService mapperService = createMapperService(
                mapping(b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject())
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
                            + "from [task_type=sparse_embedding] "
                            + "to [task_type=text_embedding, dimensions=10, similarity=cosine, element_type=float]"
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
            MapperService mapperService = createMapperService(buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, false);
            assertSearchInferenceId(mapperService, fieldName, inferenceId);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId1));
            assertSemanticTextField(mapperService, fieldName, false);
            assertSearchInferenceId(mapperService, fieldName, searchInferenceId1);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId2));
            assertSemanticTextField(mapperService, fieldName, false);
            assertSearchInferenceId(mapperService, fieldName, searchInferenceId2);

            merge(mapperService, buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, false);
            assertSearchInferenceId(mapperService, fieldName, inferenceId);

            mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            assertSemanticTextField(mapperService, fieldName, true);
            assertSearchInferenceId(mapperService, fieldName, inferenceId);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId1));
            assertSemanticTextField(mapperService, fieldName, true);
            assertSearchInferenceId(mapperService, fieldName, searchInferenceId1);

            merge(mapperService, buildMapping.apply(fieldName, searchInferenceId2));
            assertSemanticTextField(mapperService, fieldName, true);
            assertSearchInferenceId(mapperService, fieldName, searchInferenceId2);

            merge(mapperService, buildMapping.apply(fieldName, null));
            assertSemanticTextField(mapperService, fieldName, true);
            assertSearchInferenceId(mapperService, fieldName, inferenceId);
        }
    }

    private static void assertSemanticTextField(MapperService mapperService, String fieldName, boolean expectedModelSettings) {
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
        Mapper textMapper = chunksMapper.getMapper(CHUNKED_TEXT_FIELD);
        assertNotNull(textMapper);
        assertThat(textMapper, instanceOf(KeywordFieldMapper.class));
        KeywordFieldMapper textFieldMapper = (KeywordFieldMapper) textMapper;
        assertFalse(textFieldMapper.fieldType().isIndexed());
        assertFalse(textFieldMapper.fieldType().hasDocValues());
        if (expectedModelSettings) {
            assertNotNull(semanticFieldMapper.fieldType().getModelSettings());
            Mapper embeddingsMapper = chunksMapper.getMapper(CHUNKED_EMBEDDINGS_FIELD);
            assertNotNull(embeddingsMapper);
            assertThat(embeddingsMapper, instanceOf(FieldMapper.class));
            FieldMapper embeddingsFieldMapper = (FieldMapper) embeddingsMapper;
            assertTrue(embeddingsFieldMapper.fieldType() == mapperService.mappingLookup().getFieldType(getEmbeddingsFieldName(fieldName)));
            assertThat(embeddingsMapper.fullPath(), equalTo(getEmbeddingsFieldName(fieldName)));
            switch (semanticFieldMapper.fieldType().getModelSettings().taskType()) {
                case SPARSE_EMBEDDING -> assertThat(embeddingsMapper, instanceOf(SparseVectorFieldMapper.class));
                case TEXT_EMBEDDING -> assertThat(embeddingsMapper, instanceOf(DenseVectorFieldMapper.class));
                default -> throw new AssertionError("Invalid task type");
            }
        } else {
            assertNull(semanticFieldMapper.fieldType().getModelSettings());
        }
    }

    private static void assertSearchInferenceId(MapperService mapperService, String fieldName, String expectedSearchInferenceId) {
        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType = (SemanticTextFieldMapper.SemanticTextFieldType) fieldType;
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
            XContentBuilder mapping = mapping(b -> {
                addSemanticTextMapping(b, fieldName1, model1.getInferenceEntityId(), setSearchInferenceId ? searchInferenceId : null);
                addSemanticTextMapping(b, fieldName2, model2.getInferenceEntityId(), setSearchInferenceId ? searchInferenceId : null);
            });

            MapperService mapperService = createMapperService(mapping);
            assertSemanticTextField(mapperService, fieldName1, false);
            assertSearchInferenceId(mapperService, fieldName1, setSearchInferenceId ? searchInferenceId : model1.getInferenceEntityId());
            assertSemanticTextField(mapperService, fieldName2, false);
            assertSearchInferenceId(mapperService, fieldName2, setSearchInferenceId ? searchInferenceId : model2.getInferenceEntityId());

            DocumentMapper documentMapper = mapperService.documentMapper();
            ParsedDocument doc = documentMapper.parse(
                source(
                    b -> addSemanticTextInferenceResults(
                        b,
                        List.of(
                            randomSemanticText(fieldName1, model1, List.of("a b", "c"), XContentType.JSON),
                            randomSemanticText(fieldName2, model2, List.of("d e f"), XContentType.JSON)
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
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> addSemanticTextMapping(b, "field", "my_id", null)));
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> documentMapper.parse(
                source(
                    b -> b.startObject("field")
                        .startObject(INFERENCE_FIELD)
                        .field(MODEL_SETTINGS_FIELD, new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null))
                        .field(CHUNKS_FIELD, List.of())
                        .endObject()
                        .endObject()
                )
            )
        );
        assertThat(ex.getCause().getMessage(), containsString("Required [inference_id]"));
    }

    public void testMissingModelSettings() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> addSemanticTextMapping(b, "field", "my_id", null)));
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> documentMapper.parse(
                source(b -> b.startObject("field").startObject(INFERENCE_FIELD).field(INFERENCE_ID_FIELD, "my_id").endObject().endObject())
            )
        );
        assertThat(ex.getCause().getMessage(), containsString("Required [model_settings, chunks]"));
    }

    public void testMissingTaskType() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(mapping(b -> addSemanticTextMapping(b, "field", "my_id", null)));
        IllegalArgumentException ex = expectThrows(
            DocumentParsingException.class,
            IllegalArgumentException.class,
            () -> documentMapper.parse(
                source(
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
            new SemanticTextField.ModelSettings(
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
            new SemanticTextField.ModelSettings(
                TaskType.TEXT_EMBEDDING,
                1024,
                SimilarityMeasure.COSINE,
                DenseVectorFieldMapper.ElementType.BYTE
            )
        );
        assertMapperService.accept(byteMapperService, DenseVectorFieldMapper.ElementType.BYTE);
    }

    private MapperService mapperServiceForFieldWithModelSettings(
        String fieldName,
        String inferenceId,
        SemanticTextField.ModelSettings modelSettings
    ) throws IOException {
        return mapperServiceForFieldWithModelSettings(fieldName, inferenceId, null, modelSettings);
    }

    private MapperService mapperServiceForFieldWithModelSettings(
        String fieldName,
        String inferenceId,
        String searchInferenceId,
        SemanticTextField.ModelSettings modelSettings
    ) throws IOException {
        String mappingParams = "type=semantic_text,inference_id=" + inferenceId;
        if (searchInferenceId != null) {
            mappingParams += ",search_inference_id=" + searchInferenceId;
        }

        MapperService mapperService = createMapperService(mapping(b -> {}));
        mapperService.merge(
            "_doc",
            new CompressedXContent(Strings.toString(PutMappingRequest.simpleMapping(fieldName, mappingParams))),
            MapperService.MergeReason.MAPPING_UPDATE
        );

        SemanticTextField semanticTextField = new SemanticTextField(
            fieldName,
            List.of(),
            new SemanticTextField.InferenceResult(inferenceId, modelSettings, List.of()),
            XContentType.JSON
        );
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        builder.field(semanticTextField.fieldName());
        builder.value(semanticTextField);
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
            new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null)
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
            new SemanticTextField.ModelSettings(
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

    @Override
    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        // Until a doc is indexed, the query is rewritten as match no docs
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    private static void addSemanticTextMapping(
        XContentBuilder mappingBuilder,
        String fieldName,
        String inferenceId,
        String searchInferenceId
    ) throws IOException {
        mappingBuilder.startObject(fieldName);
        mappingBuilder.field("type", SemanticTextFieldMapper.CONTENT_TYPE);
        mappingBuilder.field("inference_id", inferenceId);
        if (searchInferenceId != null) {
            mappingBuilder.field("search_inference_id", searchInferenceId);
        }
        mappingBuilder.endObject();
    }

    private static void addSemanticTextInferenceResults(XContentBuilder sourceBuilder, List<SemanticTextField> semanticTextInferenceResults)
        throws IOException {
        for (var field : semanticTextInferenceResults) {
            sourceBuilder.field(field.fieldName());
            sourceBuilder.value(field);
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

        return new ESToParentBlockJoinQuery(queryBuilder.build(), parentFilter, ScoreMode.Total, null);
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
}
