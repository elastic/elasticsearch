/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.size;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugin.mapper.MapperSizePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SizeMappingTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(MapperSizePlugin.class, InternalSettingsPlugin.class);
    }

    public void testSizeEnabled() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));

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
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=false");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testSizeNotSet() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type");
        DocumentMapper docMapper = service.mapperService().documentMapper();

        BytesReference source = BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject());
        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", source, XContentType.JSON));

        assertThat(doc.rootDoc().getField("_size"), nullValue());
    }

    public void testThatDisablingWorksWhenMerging() throws Exception {
        IndexService service = createIndex("test", Settings.EMPTY, "type", "_size", "enabled=true");
        DocumentMapper docMapper = service.mapperService().documentMapper();
        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(true));

        String disabledMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_size").field("enabled", false).endObject()
            .endObject().endObject());
        docMapper = service.mapperService().merge("type", new CompressedXContent(disabledMapping),
            MapperService.MergeReason.MAPPING_UPDATE);

        assertThat(docMapper.metadataMapper(SizeFieldMapper.class).enabled(), is(false));
    }

}
