/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SemanticTextFieldMapperTests extends MapperTestCase {

    public void testDefaults() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertEquals(Strings.toString(fieldMapping(this::minimalMapping)), mapperService.documentMapper().mappingSource().toString());

        ParsedDocument doc1 = mapperService.documentMapper().parse(source(this::writeField));
        List<IndexableField> fields = doc1.rootDoc().getFields("field");

        // No indexable fields
        assertTrue(fields.isEmpty());
        assertThat(mapperService.mappingLookup().getFieldsForModels(), equalTo(Map.of("test_model", Map.of("field", List.of("field")))));
    }

    public void testModelIdNotPresent() throws IOException {
        Exception e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(fieldMapping(b -> b.field("type", "semantic_text")))
        );
        assertThat(e.getMessage(), containsString("field [model_id] must be specified"));
    }

    public void testAsMultiFieldTarget() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("semantic");
            b.field("type", "semantic_text");
            b.field("model_id", "test_model");
            b.endObject();
            b.endObject();
        }));

        assertThat(
            mapperService.mappingLookup().getFieldsForModels(),
            equalTo(Map.of("test_model", Map.of("field.semantic", List.of("field"))))
        );
    }

    public void testAsCopyToTarget() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1");
            b.field("type", "text");
            b.field("copy_to", "field3");
            b.endObject();
            b.startObject("field2");
            b.field("type", "text");
            b.field("copy_to", "field3");
            b.endObject();
            b.startObject("field3");
            b.field("type", "semantic_text");
            b.field("model_id", "test_model");
            b.endObject();
        }));

        assertThat(
            mapperService.mappingLookup().getFieldsForModels(),
            equalTo(Map.of("test_model", Map.of("field3", List.of("field1", "field3", "field2"))))
        );
    }

    public void testAsCopyToTargetInMultiField() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {
            b.startObject("field1");
            b.field("type", "text");
            b.field("copy_to", "field3");
            b.endObject();
            b.startObject("field2");
            b.field("type", "text");
            b.field("copy_to", "field3");
            b.endObject();
            b.startObject("field3");
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("semantic");
            b.field("type", "semantic_text");
            b.field("model_id", "test_model");
            b.endObject();
            b.endObject();
            b.endObject();
        }));

        assertThat(
            mapperService.mappingLookup().getFieldsForModels(),
            equalTo(Map.of("test_model", Map.of("field3.semantic", List.of("field1", "field3", "field2"))))
        );
    }

    public void testUpdatesToModelIdNotSupported() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "semantic_text").field("model_id", "test_model"))
        );
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "semantic_text").field("model_id", "another_model")))
        );
        assertThat(e.getMessage(), containsString("Cannot update parameter [model_id] from [test_model] to [another_model]"));
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MachineLearning(Settings.EMPTY));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "semantic_text").field("model_id", "test_model");
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
}
