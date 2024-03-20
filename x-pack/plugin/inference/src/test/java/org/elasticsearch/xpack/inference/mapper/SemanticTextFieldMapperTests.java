/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.inference.mapper.InferenceMetadataFieldMapper.findMapper;
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
        return "value";
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

    public void testDefaults() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapper.mappingSource().toString());

        ParsedDocument doc1 = mapper.parse(source(this::writeField));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        // No indexable fields
        assertTrue(fields.isEmpty());
    }

    public void testInferenceIdNotPresent() throws IOException {
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

    public void testUpdateModelSettings() throws IOException {
        for (int depth = 1; depth < 5; depth++) {
            String fieldName = InferenceMetadataFieldMapperTests.randomFieldName(depth);
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
                assertThat(exc.getMessage(), containsString("Failed to parse [model_settings], required [task_type] is missing"));
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
                Exception exc = expectThrows(
                    IllegalArgumentException.class,
                    () -> merge(
                        mapperService,
                        mapping(
                            b -> b.startObject(fieldName).field("type", "semantic_text").field("inference_id", "test_model").endObject()
                        )
                    )
                );
                assertThat(
                    exc.getMessage(),
                    containsString("Cannot update parameter [model_settings] " + "from [{\"task_type\":\"sparse_embedding\"}] to [null]")
                );
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
                                .endObject()
                                .endObject()
                        )
                    )
                );
                assertThat(
                    exc.getMessage(),
                    containsString(
                        "Cannot update parameter [model_settings] "
                            + "from [{\"task_type\":\"sparse_embedding\"}] "
                            + "to [{\"task_type\":\"text_embedding\",\"dimensions\":10,\"similarity\":\"cosine\"}]"
                    )
                );
            }
        }
    }

    static void assertSemanticTextField(MapperService mapperService, String fieldName, boolean expectedModelSettings) {
        var res = findMapper(mapperService.mappingLookup().getMapping().getRoot(), fieldName);
        Mapper mapper = res.mapper();
        assertNotNull(mapper);
        assertThat(mapper, instanceOf(SemanticTextFieldMapper.class));
        SemanticTextFieldMapper semanticFieldMapper = (SemanticTextFieldMapper) mapper;

        var fieldType = mapperService.fieldType(fieldName);
        assertNotNull(fieldType);
        assertThat(fieldType, instanceOf(SemanticTextFieldMapper.SemanticTextFieldType.class));
        SemanticTextFieldMapper.SemanticTextFieldType semanticTextFieldType = (SemanticTextFieldMapper.SemanticTextFieldType) fieldType;
        assertTrue(semanticFieldMapper.fieldType() == semanticTextFieldType);
        assertTrue(semanticFieldMapper.getSubMappers() == semanticTextFieldType.getSubMappers());
        assertTrue(semanticFieldMapper.getModelSettings() == semanticTextFieldType.getModelSettings());

        NestedObjectMapper nestedObjectMapper = mapperService.mappingLookup()
            .nestedLookup()
            .getNestedMappers()
            .get(fieldName + "." + InferenceMetadataFieldMapper.RESULTS);
        assertThat(nestedObjectMapper, equalTo(semanticFieldMapper.getSubMappers()));
        Mapper textMapper = nestedObjectMapper.getMapper(InferenceMetadataFieldMapper.INFERENCE_CHUNKS_TEXT);
        assertNotNull(textMapper);
        assertThat(textMapper, instanceOf(TextFieldMapper.class));
        TextFieldMapper textFieldMapper = (TextFieldMapper) textMapper;
        assertFalse(textFieldMapper.fieldType().isIndexed());
        assertFalse(textFieldMapper.fieldType().hasDocValues());
        if (expectedModelSettings) {
            assertNotNull(semanticFieldMapper.getModelSettings());
            Mapper inferenceMapper = nestedObjectMapper.getMapper(InferenceMetadataFieldMapper.INFERENCE_CHUNKS_RESULTS);
            assertNotNull(inferenceMapper);
            switch (semanticFieldMapper.getModelSettings().taskType()) {
                case SPARSE_EMBEDDING -> assertThat(inferenceMapper, instanceOf(SparseVectorFieldMapper.class));
                case TEXT_EMBEDDING -> assertThat(inferenceMapper, instanceOf(DenseVectorFieldMapper.class));
                default -> throw new AssertionError("Invalid task type");
            }
        } else {
            assertNull(semanticFieldMapper.getModelSettings());
        }
    }
}
