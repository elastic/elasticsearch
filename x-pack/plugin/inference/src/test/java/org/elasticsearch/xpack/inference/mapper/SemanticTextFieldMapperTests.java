/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

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
import org.elasticsearch.common.CheckedBiConsumer;
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
import org.elasticsearch.index.mapper.vectors.XFeatureField;
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
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_TEXT_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.MODEL_SETTINGS_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.SEARCH_INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getChunksFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.getEmbeddingsFieldName;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.DEFAULT_ELSER_2_INFERENCE_ID;
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
        b.field("type", "semantic_text");
    }

    @Override
    protected String minimalIsInvalidRoutingPathErrorMessage(Mapper mapper) {
        return "cannot have nested fields when index is in [index.mode=time_series]";
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
        final String fieldName = "field";
        final XContentBuilder fieldMapping = fieldMapping(this::minimalMapping);
        final XContentBuilder expectedMapping = fieldMapping(this::metaMapping);

        MapperService mapperService = createMapperService(fieldMapping);
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
            final MapperService mapperService = createMapperService(fieldMapping);
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
            final MapperService mapperService = createMapperService(fieldMapping);
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
            MapperService mapperService = createMapperService(fieldMapping);
            assertSemanticTextField(mapperService, fieldName, false);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId);
            assertSerialization.accept(fieldMapping, mapperService);
        }
    }

    public void testInvalidInferenceEndpoints() {
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "semantic_text").field(INFERENCE_ID_FIELD, (String) null)))
            );
            assertThat(
                e.getMessage(),
                containsString("[inference_id] on mapper [field] of type [semantic_text] must not have a [null] value")
            );
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "semantic_text").field(INFERENCE_ID_FIELD, "")))
            );
            assertThat(e.getMessage(), containsString("[inference_id] on mapper [field] of type [semantic_text] must not be empty"));
        }
        {
            Exception e = expectThrows(
                MapperParsingException.class,
                () -> createMapperService(fieldMapping(b -> b.field("type", "semantic_text").field(SEARCH_INFERENCE_ID_FIELD, "")))
            );
            assertThat(e.getMessage(), containsString("[search_inference_id] on mapper [field] of type [semantic_text] must not be empty"));
        }
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
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, inferenceId);
        }

        {
            MapperService mapperService = mapperServiceForFieldWithModelSettings(
                fieldName,
                inferenceId,
                searchInferenceId,
                new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null)
            );
            assertSemanticTextField(mapperService, fieldName, true);
            assertInferenceEndpoints(mapperService, fieldName, inferenceId, searchInferenceId);
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
                new SemanticTextField.ModelSettings(TaskType.SPARSE_EMBEDDING, null, null, null)
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
            XContentBuilder mapping = mapping(b -> {
                addSemanticTextMapping(b, fieldName1, model1.getInferenceEntityId(), setSearchInferenceId ? searchInferenceId : null);
                addSemanticTextMapping(b, fieldName2, model2.getInferenceEntityId(), setSearchInferenceId ? searchInferenceId : null);
            });

            MapperService mapperService = createMapperService(mapping);
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

    public void testInsertValueMapTraversal() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("test", "value").endObject();

            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            SemanticTextFieldMapper.insertValue("test", map, "value2");
            assertThat(getMapValue(map, "test"), equalTo("value2"));
            SemanticTextFieldMapper.insertValue("something.else", map, "something_else_value");
            assertThat(getMapValue(map, "something\\.else"), equalTo("something_else_value"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            builder.startObject("path1").startObject("path2").field("test", "value").endObject().endObject();
            builder.endObject();

            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            SemanticTextFieldMapper.insertValue("path1.path2.test", map, "value2");
            assertThat(getMapValue(map, "path1.path2.test"), equalTo("value2"));
            SemanticTextFieldMapper.insertValue("path1.path2.test_me", map, "test_me_value");
            assertThat(getMapValue(map, "path1.path2.test_me"), equalTo("test_me_value"));
            SemanticTextFieldMapper.insertValue("path1.non_path2.test", map, "test_value");
            assertThat(getMapValue(map, "path1.non_path2\\.test"), equalTo("test_value"));

            SemanticTextFieldMapper.insertValue("path1.path2", map, Map.of("path3", "bar"));
            assertThat(getMapValue(map, "path1.path2"), equalTo(Map.of("path3", "bar")));

            SemanticTextFieldMapper.insertValue("path1", map, "baz");
            assertThat(getMapValue(map, "path1"), equalTo("baz"));

            SemanticTextFieldMapper.insertValue("path3.path4", map, Map.of("test", "foo"));
            assertThat(getMapValue(map, "path3\\.path4"), equalTo(Map.of("test", "foo")));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            builder.startObject("path1").array("test", "value1", "value2").endObject();
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextFieldMapper.insertValue("path1.test", map, List.of("value3", "value4", "value5"));
            assertThat(getMapValue(map, "path1.test"), equalTo(List.of("value3", "value4", "value5")));

            SemanticTextFieldMapper.insertValue("path2.test", map, List.of("value6", "value7", "value8"));
            assertThat(getMapValue(map, "path2\\.test"), equalTo(List.of("value6", "value7", "value8")));
        }
    }

    public void testInsertValueListTraversal() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1");
                {
                    builder.startArray("path2");
                    builder.startObject().field("test", "value1").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            {
                builder.startObject("path3");
                {
                    builder.startArray("path4");
                    builder.startObject().field("test", "value1").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextFieldMapper.insertValue("path1.path2.test", map, "value2");
            assertThat(getMapValue(map, "path1.path2.test"), equalTo("value2"));
            SemanticTextFieldMapper.insertValue("path1.path2.test2", map, "value3");
            assertThat(getMapValue(map, "path1.path2.test2"), equalTo("value3"));
            assertThat(getMapValue(map, "path1.path2"), equalTo(List.of(Map.of("test", "value2", "test2", "value3"))));

            SemanticTextFieldMapper.insertValue("path3.path4.test", map, "value4");
            assertThat(getMapValue(map, "path3.path4.test"), equalTo("value4"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1");
                {
                    builder.startArray("path2");
                    builder.startArray();
                    builder.startObject().field("test", "value1").endObject();
                    builder.endArray();
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextFieldMapper.insertValue("path1.path2.test", map, "value2");
            assertThat(getMapValue(map, "path1.path2.test"), equalTo("value2"));
            SemanticTextFieldMapper.insertValue("path1.path2.test2", map, "value3");
            assertThat(getMapValue(map, "path1.path2.test2"), equalTo("value3"));
            assertThat(getMapValue(map, "path1.path2"), equalTo(List.of(List.of(Map.of("test", "value2", "test2", "value3")))));
        }
    }

    public void testInsertValueFieldsWithDots() throws IOException {
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("xxx.yyy", "value1").endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextFieldMapper.insertValue("xxx.yyy", map, "value2");
            assertThat(getMapValue(map, "xxx\\.yyy"), equalTo("value2"));

            SemanticTextFieldMapper.insertValue("xxx", map, "value3");
            assertThat(getMapValue(map, "xxx"), equalTo("value3"));
        }
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1.path2");
                {
                    builder.startObject("path3.path4");
                    builder.field("test", "value1");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));

            SemanticTextFieldMapper.insertValue("path1.path2.path3.path4.test", map, "value2");
            assertThat(getMapValue(map, "path1\\.path2.path3\\.path4.test"), equalTo("value2"));

            SemanticTextFieldMapper.insertValue("path1.path2.path3.path4.test2", map, "value3");
            assertThat(getMapValue(map, "path1\\.path2.path3\\.path4.test2"), equalTo("value3"));
            assertThat(getMapValue(map, "path1\\.path2.path3\\.path4"), equalTo(Map.of("test", "value2", "test2", "value3")));
        }
    }

    public void testInsertValueAmbiguousPath() throws IOException {
        // Mixed dotted object notation
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1.path2");
                {
                    builder.startObject("path3");
                    builder.field("test1", "value1");
                    builder.endObject();
                }
                builder.endObject();
            }
            {
                builder.startObject("path1");
                {
                    builder.startObject("path2.path3");
                    builder.field("test2", "value2");
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            final Map<String, Object> originalMap = Collections.unmodifiableMap(toSourceMap(Strings.toString(builder)));

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextFieldMapper.insertValue("path1.path2.path3.test1", map, "value3")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test1] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextFieldMapper.insertValue("path1.path2.path3.test3", map, "value4")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test3] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            assertThat(map, equalTo(originalMap));
        }

        // traversal through lists
        {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            {
                builder.startObject("path1.path2");
                {
                    builder.startArray("path3");
                    builder.startObject().field("test1", "value1").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            {
                builder.startObject("path1");
                {
                    builder.startArray("path2.path3");
                    builder.startObject().field("test2", "value2").endObject();
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endObject();
            Map<String, Object> map = toSourceMap(Strings.toString(builder));
            final Map<String, Object> originalMap = Collections.unmodifiableMap(toSourceMap(Strings.toString(builder)));

            IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextFieldMapper.insertValue("path1.path2.path3.test1", map, "value3")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test1] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            ex = assertThrows(
                IllegalArgumentException.class,
                () -> SemanticTextFieldMapper.insertValue("path1.path2.path3.test3", map, "value4")
            );
            assertThat(
                ex.getMessage(),
                equalTo("Path [path1.path2.path3.test3] could be inserted in 2 distinct ways, it is ambiguous which one to use")
            );

            assertThat(map, equalTo(originalMap));
        }
    }

    public void testInsertValueCannotTraversePath() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        {
            builder.startObject("path1");
            {
                builder.startArray("path2");
                builder.startArray();
                builder.startObject().field("test", "value1").endObject();
                builder.endArray();
                builder.endArray();
            }
            builder.endObject();
        }
        builder.endObject();
        Map<String, Object> map = toSourceMap(Strings.toString(builder));
        final Map<String, Object> originalMap = Collections.unmodifiableMap(toSourceMap(Strings.toString(builder)));

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> SemanticTextFieldMapper.insertValue("path1.path2.test.test2", map, "value2")
        );
        assertThat(
            ex.getMessage(),
            equalTo("Path [path1.path2.test] has value [value1] of type [String], which cannot be traversed into further")
        );

        assertThat(map, equalTo(originalMap));
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
            if (field instanceof XFeatureField featureField) {
                assertThat(featureField.name(), equalTo(fieldName));
                ++count;
            }
        }
        assertThat(count, equalTo(expectedCount));
    }

    private Map<String, Object> toSourceMap(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            return parser.map();
        }
    }

    private static Object getMapValue(Map<String, Object> map, String key) {
        // Split the path on unescaped "." chars and then unescape the escaped "." chars
        final String[] pathElements = Arrays.stream(key.split("(?<!\\\\)\\.")).map(k -> k.replace("\\.", ".")).toArray(String[]::new);

        Object value = null;
        Object nextLayer = map;
        for (int i = 0; i < pathElements.length; i++) {
            if (nextLayer instanceof Map<?, ?> nextMap) {
                value = nextMap.get(pathElements[i]);
            } else if (nextLayer instanceof List<?> nextList) {
                final String pathElement = pathElements[i];
                List<?> values = nextList.stream().flatMap(v -> {
                    Stream.Builder<Object> streamBuilder = Stream.builder();
                    if (v instanceof List<?> innerList) {
                        traverseList(innerList, streamBuilder);
                    } else {
                        streamBuilder.add(v);
                    }
                    return streamBuilder.build();
                }).filter(v -> v instanceof Map<?, ?>).map(v -> ((Map<?, ?>) v).get(pathElement)).filter(Objects::nonNull).toList();

                if (values.isEmpty()) {
                    return null;
                } else if (values.size() > 1) {
                    throw new AssertionError("List " + nextList + " contains multiple values for [" + pathElement + "]");
                } else {
                    value = values.getFirst();
                }
            } else if (nextLayer == null) {
                break;
            } else {
                throw new AssertionError(
                    "Path ["
                        + String.join(".", Arrays.copyOfRange(pathElements, 0, i))
                        + "] has value ["
                        + value
                        + "] of type ["
                        + value.getClass().getSimpleName()
                        + "], which cannot be traversed into further"
                );
            }

            nextLayer = value;
        }

        return value;
    }

    private static void traverseList(List<?> list, Stream.Builder<Object> streamBuilder) {
        for (Object value : list) {
            if (value instanceof List<?> innerList) {
                traverseList(innerList, streamBuilder);
            } else {
                streamBuilder.add(value);
            }
        }
    }
}
