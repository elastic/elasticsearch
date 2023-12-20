/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class ExponentialHistogramFieldMapperTests extends MapperTestCase {

    @Override
    protected Object getSampleValueForDocument() {
        return Map.of("scale", 1,
                      "positive", Map.of("counts", new int[] { 1, 0, 0, 2 }));
    }

    @Override
    protected Object getSampleObjectForDocument() {
        return getSampleValueForDocument();
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new ExponentialHistogramMapperPlugin());
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "exponential_histogram");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {}

    @Override
    protected boolean supportsSearchLookup() {
        return false;
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    @Override
    protected boolean supportsIgnoreMalformed() { return false; }

    @Override
    protected boolean supportsEmptyInputArray() { return false; }

    @Override
    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed) {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected IngestScriptSupport ingestScriptSupport() {
        throw new AssumptionViolatedException("not supported");
    }

    @Override
    protected Object generateRandomInputValue(MappedFieldType ft) {
        assumeFalse("Test implemented in a follow up", true);
        return null;
    }

    public void testParseImpliedValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field")
                .field("scale", 1)
                .startObject("positive").field("counts", new long[]{1, 0, 0, 2}).endObject()
                .startObject("negative").field("counts", new long[]{1, 0, 0, 2}).endObject()
                .endObject())
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testParseExplicitValues() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(b -> b.startObject("field")
                .field("scale", 1)
                .startObject("negative")
                    .field("counts", new long[]{1, 2, 3})
                    .field("values", new double[]{-10, -100, -1000})
                    .endObject()
                .endObject())
        );
        assertThat(doc.rootDoc().getField("field"), notNullValue());
    }

    public void testMissingScale() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field").endObject()))
        );
        assertThat(e.getCause().getMessage(), containsString("expected field called [scale]"));
    }

    public void testMissingCounts() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Exception e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field")
                .field("scale", 0)
                    .startObject("negative")
                    .field("counts", new long[]{}).endObject()
                .endObject()))
        );
        assertThat(e.getCause().getMessage(),
            containsString("expected at least one the fields called [positive] or [negative] to be specified and non-empty"));
    }

    public void testUnknownField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field")
                .field("unknown", new double[] { 2, 2 })
                .endObject()
        );

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("with unknown parameter [unknown]"));
    }

    public void testUnknownBucketsField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field")
                .startObject("positive")
                    .field("unknown", new double[] { 2, 2 })
                    .endObject()
                .endObject()
        );

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("with unknown parameter [unknown]"));
    }

    /*
    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("pre_aggregated")));
        assertThat(doc.rootDoc().getField("pre_aggregated"), nullValue());
    }

    public void testNegativeCount() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        SourceToParse source = source(
            b -> b.startObject("field").field("counts", new int[] { 2, 2, -3 }).field("values", new double[] { 2, 2, 3 }).endObject()
        );
        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source));
        assertThat(e.getCause().getMessage(), containsString("[counts] elements must be >= 0 but got -3"));
    }
    */
}
