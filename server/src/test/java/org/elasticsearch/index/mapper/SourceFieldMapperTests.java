/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.index.MapperTestUtils.assertConflicts;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SourceFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testNoFormat() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").endObject()
                .endObject().endObject());

        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper documentMapper = mapperService.parse("type", new CompressedXContent(mapping), false);
        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject()),
                XContentType.JSON));

        assertThat(XContentFactory.xContentType(doc.source().toBytesRef().bytes), equalTo(XContentType.JSON));

        documentMapper = mapperService.parse("type", new CompressedXContent(mapping), false);
        doc = documentMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.smileBuilder().startObject()
                .field("field", "value")
                .endObject()),
                XContentType.SMILE));

        assertThat(XContentHelper.xContentType(doc.source()), equalTo(XContentType.SMILE));
    }

    public void testIncludes() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", new String[]{"path1*"}).endObject()
            .endObject().endObject());

        DocumentMapper documentMapper = createIndex("test").mapperService().parse("type", new CompressedXContent(mapping), false);

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("path1").field("field1", "value1").endObject()
            .startObject("path2").field("field2", "value2").endObject()
            .endObject()),
            XContentType.JSON));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));
    }

    public void testExcludes() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "path1*").endObject()
            .endObject().endObject());

        DocumentMapper documentMapper = createIndex("test").mapperService().parse("type", new CompressedXContent(mapping), false);

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "type", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("path1").field("field1", "value1").endObject()
            .startObject("path2").field("field2", "value2").endObject()
            .endObject()),
            XContentType.JSON));

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(true));
    }

    public void testEnabledNotUpdateable() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        // using default of true
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", false).endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping2, mapperService, "Cannot update parameter [enabled] from [true] to [false]");

        // not changing is ok
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", true).endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping3, mapperService);
    }

    public void testIncludesNotUpdateable() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*").endObject()
            .endObject().endObject());
        assertConflicts(defaultMapping, mapping1, mapperService, "Cannot update parameter [includes] from [[]] to [[foo.*]]");
        assertConflicts(mapping1, defaultMapping, mapperService, "Cannot update parameter [includes] from [[foo.*]] to [[]]");

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*", "bar.*").endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping2, mapperService, "Cannot update parameter [includes] from [[foo.*]] to [[foo.*, bar.*]]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, mapperService);
    }

    public void testExcludesNotUpdateable() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*").endObject()
            .endObject().endObject());
        assertConflicts(defaultMapping, mapping1, mapperService, "Cannot update parameter [excludes] from [[]] to [[foo.*]]");
        assertConflicts(mapping1, defaultMapping, mapperService, "Cannot update parameter [excludes] from [[foo.*]] to [[]]");

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*", "bar.*").endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping2, mapperService, "Cannot update parameter [excludes]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, mapperService);
    }

    public void testComplete() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        assertTrue(mapperService.parse("type", new CompressedXContent(mapping), false).sourceMapper().isComplete());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", false).endObject()
            .endObject().endObject());
        assertFalse(mapperService.parse("type", new CompressedXContent(mapping), false).sourceMapper().isComplete());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*").endObject()
            .endObject().endObject());
        assertFalse(mapperService.parse("type", new CompressedXContent(mapping), false).sourceMapper().isComplete());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*").endObject()
            .endObject().endObject());
        assertFalse(mapperService.parse("type", new CompressedXContent(mapping), false).sourceMapper().isComplete());
    }

    public void testSourceObjectContainsExtraTokens() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper documentMapper = createIndex("test").mapperService().parse("type", new CompressedXContent(mapping), false);

        MapperParsingException exception = expectThrows(MapperParsingException.class,
            // extra end object (invalid JSON))
            () -> documentMapper.parse(new SourceToParse("test", "type", "1", new BytesArray("{}}"), XContentType.JSON)));
        assertNotNull(exception.getRootCause());
        assertThat(exception.getRootCause().getMessage(), containsString("Unexpected close marker '}'"));
    }
}
