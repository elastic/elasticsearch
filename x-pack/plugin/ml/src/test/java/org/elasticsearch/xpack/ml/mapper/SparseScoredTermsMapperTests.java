/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
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

import static org.hamcrest.Matchers.notNullValue;

public class SparseScoredTermsMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("terms", List.of("1", "123"), "scores", List.of(1, 3));
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new MachineLearning(Settings.EMPTY));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", SparseScoredTermsMapper.CONTENT_TYPE);
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> assertTrue(m.ignoreMalformed()));
    }

    @Override
    protected boolean supportsSearchLookup() {
        return true;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    public void testParseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", Map.of("terms", List.of("1", "2"), "scores", List.of(54, 100)))));
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    @Override
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        return List.of(
            exampleMalformedValue(b -> b.startObject().startArray("scores").value(2).value(2).endArray().endObject()).errorMatches(
                "failed to parse field [field] of type [sparse_scored_terms]"
            ),
            exampleMalformedValue(b -> b.startObject().field("terms", List.of("foo", "bar")).endObject()).errorMatches(
                "failed to parse field [field] of type [sparse_scored_terms]"
            ),
            exampleMalformedValue(b -> b.startObject().field("scores", List.of(-1, 1)).field("terms", List.of("foo", "bar")).endObject())
                .errorMatches("[scores] must be non-negative"),
            exampleMalformedValue(b -> b.startObject().field("scores", List.of(1)).field("terms", List.of("foo", "bar")).endObject())
                .errorMatches("[terms] and [scores] must have equal length"),
            exampleMalformedValue(b -> b.startObject().field("scores", List.of(1, 2)).field("terms", List.of("bar")).endObject())
                .errorMatches("[terms] and [scores] must have equal length"),
            exampleMalformedValue(b -> b.startObject().field("scores", List.of(1, 2)).field("terms", List.of("bar", "bar")).endObject())
                .errorMatches("requires unique items in [terms]"),
            exampleMalformedValue(
                b -> b.startObject()
                    .field("unknown", "foo")
                    .field("scores", List.of(1, 2))
                    .field("terms", List.of("foo", "bar"))
                    .endObject()
            ).errorMatches("failed to parse field [field] of type [sparse_scored_terms]")
        );
    }

    @Override
    protected boolean supportsIgnoreMalformed() {
        return true;
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("We don't have a way to assert things here", true);
        return null;
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }
}
