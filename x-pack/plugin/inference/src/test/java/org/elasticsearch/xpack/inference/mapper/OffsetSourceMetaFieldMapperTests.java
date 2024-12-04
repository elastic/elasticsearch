/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.hamcrest.CoreMatchers;

import java.util.Collection;
import java.util.Collections;

public class OffsetSourceMetaFieldMapperTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new InferencePlugin(Settings.EMPTY));
    }

    public void testBasics() throws Exception {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("field")
                .field("type", "offset_source")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );

        Mapping parsedMapping = createMapperService(mapping).parseMapping(
            "type",
            MapperService.MergeReason.MAPPING_UPDATE,
            new CompressedXContent(mapping)
        );
        assertEquals(mapping, parsedMapping.toCompressedXContent().toString());
        assertNotNull(parsedMapping.getMetadataMapperByClass(OffsetSourceMetaFieldMapper.class));
    }

    public void testDocumentParsingFailsOnMetaField() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        DocumentMapper mapper = createMapperService(mapping).merge(
            "_doc",
            new CompressedXContent(mapping),
            MapperService.MergeReason.MAPPING_UPDATE
        );
        String metaField = OffsetSourceMetaFieldMapper.CONTENT_TYPE;
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field(metaField, 0).endObject());
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(new SourceToParse("1", bytes, XContentType.JSON))
        );
        assertThat(
            e.getCause().getMessage(),
            CoreMatchers.containsString("Field [" + metaField + "] is a metadata field and cannot be added inside a document.")
        );
    }

    @Override
    public void testFieldHasValue() {
        assertTrue(
            getMappedFieldType().fieldHasValue(new FieldInfos(new FieldInfo[] { getFieldInfoWithName(OffsetSourceMetaFieldMapper.NAME) }))
        );
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assertFalse(getMappedFieldType().fieldHasValue(FieldInfos.EMPTY));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new OffsetSourceMetaFieldMapper.OffsetSourceMetaFieldType();
    }
}
