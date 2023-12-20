/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataMapperTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.nullValue;

public class SizeMappingTests extends MetadataMapperTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new MapperSizePlugin());
    }

    private static void enabled(XContentBuilder b) throws IOException {
        b.startObject(SizeFieldMapper.NAME);
        b.field("enabled", true);
        b.endObject();
    }

    private static void disabled(XContentBuilder b) throws IOException {
        b.startObject(SizeFieldMapper.NAME);
        b.field("enabled", false);
        b.endObject();
    }

    public void testSizeEnabled() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(SizeMappingTests::enabled));

        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));

        boolean stored = false;
        boolean points = false;
        for (IndexableField field : doc.rootDoc().getFields("_size")) {
            stored |= field.fieldType().stored();
            points |= field.fieldType().pointIndexDimensionCount() > 0;
        }
        assertTrue(stored);
        assertTrue(points);
    }

    public void testSizeDisabled() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(SizeMappingTests::disabled));
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testSizeNotSet() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> {}));
        ParsedDocument doc = docMapper.parse(source(b -> b.field("field", "value")));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testThatDisablingWorksWhenMerging() throws Exception {
        MapperService mapperService = createMapperService(topMapping(SizeMappingTests::enabled));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "value")));
        assertNotNull(doc.rootDoc().getField(SizeFieldMapper.NAME));

        merge(mapperService, topMapping(SizeMappingTests::disabled));
        doc = mapperService.documentMapper().parse(source(b -> b.field("field", "value")));
        assertNull(doc.rootDoc().getField(SizeFieldMapper.NAME));
    }

    @Override
    protected String fieldName() {
        return SizeFieldMapper.NAME;
    }

    @Override
    protected boolean isConfigurable() {
        return true;
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerUpdateCheck(
            topMapping(SizeMappingTests::disabled),
            topMapping(SizeMappingTests::enabled),
            dm -> assertTrue(dm.metadataMapper(SizeFieldMapper.class).enabled())
        );
        checker.registerUpdateCheck(
            topMapping(SizeMappingTests::enabled),
            topMapping(SizeMappingTests::disabled),
            dm -> assertFalse(dm.metadataMapper(SizeFieldMapper.class).enabled())
        );
    }
}
