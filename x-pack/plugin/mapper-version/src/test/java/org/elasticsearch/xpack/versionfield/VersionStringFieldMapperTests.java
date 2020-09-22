/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(Strings.toString(mapping), mapper.mappingSource().toString());

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
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
            () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON))
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
            () -> defaultMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON))
        );
        assertEquals(
            "failed to parse field [field] of type [version] in document with id '1'. "
                + "Preview of field's value: '{array_name=[inner_field_first, inner_field_second]}'",
            ex.getMessage()
        );
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        VersionStringFieldMapper mapper = new VersionStringFieldMapper.Builder("field").build(context);
        assertEquals(List.of("value"), fetchSourceValue(mapper, "value"));
        assertEquals(List.of("42"), fetchSourceValue(mapper, 42L));
        assertEquals(List.of("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] of type [version] doesn't support formats.", e.getMessage());
    }
}
