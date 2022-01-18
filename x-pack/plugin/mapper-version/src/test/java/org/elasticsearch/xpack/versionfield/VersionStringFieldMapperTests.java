/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class VersionStringFieldMapperTests extends MapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new VersionFieldPlugin(getIndexSettings()));
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "version");
    }

    @Override
    protected Object getSampleValueForDocument() {
        return "1.2.3";
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        // no configurable parameters
    }

    @Override
    protected boolean supportsStoredFields() {
        return false;
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "1",
                BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "1.2.3").endObject()),
                XContentType.JSON
            )
        );

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);

        assertEquals("1.2.3", VersionEncoder.decodeVersion(fields[0].binaryValue()));
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.omitNorms(), equalTo(true));
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertThat(fieldType.storeTermVectors(), equalTo(false));
        assertThat(fieldType.storeTermVectorOffsets(), equalTo(false));
        assertThat(fieldType.storeTermVectorPositions(), equalTo(false));
        assertThat(fieldType.storeTermVectorPayloads(), equalTo(false));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        assertEquals("1.2.3", VersionEncoder.decodeVersion(fields[1].binaryValue()));
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());

    }

    public void testParsesNestedEmptyObjectStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));

        BytesReference source = BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject().startObject("field").endObject().endObject()
        );
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("1", source, XContentType.JSON))
        );
        assertEquals(
            "failed to parse field [field] of type [version] in document with id '1'. " + "Preview of field's value: '{}'",
            ex.getMessage()
        );
    }

    public void testFailsParsingNestedList() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        BytesReference source = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .startArray("field")
                .startObject()
                .startArray("array_name")
                .value("inner_field_first")
                .value("inner_field_second")
                .endArray()
                .endObject()
                .endArray()
                .endObject()
        );
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> defaultMapper.parse(new SourceToParse("1", source, XContentType.JSON))
        );
        assertEquals(
            "failed to parse field [field] of type [version] in document with id '1'. "
                + "Preview of field's value: '{array_name=[inner_field_first, inner_field_second]}'",
            ex.getMessage()
        );
    }

    @Override
    protected String generateRandomInputValue(MappedFieldType ft) {
        return randomVersionNumber() + (randomBoolean() ? "" : randomPrerelease());
    }

    private String randomVersionNumber() {
        int numbers = between(1, 3);
        String v = Integer.toString(between(0, 100));
        for (int i = 1; i < numbers; i++) {
            v += "." + between(0, 100);
        }
        return v;
    }

    private String randomPrerelease() {
        if (rarely()) {
            return randomFrom("alpha", "beta", "prerelease", "whatever");
        }
        return randomFrom("alpha", "beta", "") + randomVersionNumber();
    }

    @Override
    protected boolean dedupAfterFetch() {
        return true;
    }
}
