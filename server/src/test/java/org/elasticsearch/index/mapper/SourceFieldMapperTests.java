/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

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

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse("type", new CompressedXContent(mapping));
        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1",
            BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject()),
                XContentType.JSON));

        assertThat(XContentFactory.xContentType(doc.source().toBytesRef().bytes), equalTo(XContentType.JSON));

        documentMapper = parser.parse("type", new CompressedXContent(mapping));
        doc = documentMapper.parse(new SourceToParse("test", "1",
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

        DocumentMapper documentMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1",
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
            .startObject("_source").array("excludes", new String[]{"path1*"}).endObject()
            .endObject().endObject());

        DocumentMapper documentMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1",
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

    private void assertConflicts(String mapping1, String mapping2, DocumentMapperParser parser, String... conflicts) throws IOException {
        DocumentMapper docMapper = parser.parse("_doc", new CompressedXContent(mapping1));
        docMapper = parser.parse("_doc", docMapper.mappingSource());
        if (conflicts.length == 0) {
            docMapper.merge(parser.parse("_doc", new CompressedXContent(mapping2)).mapping());
        } else {
            try {
                docMapper.merge(parser.parse("_doc", new CompressedXContent(mapping2)).mapping());
                fail();
            } catch (IllegalArgumentException e) {
                for (String conflict : conflicts) {
                    assertThat(e.getMessage(), containsString(conflict));
                }
            }
        }
    }

    public void testEnabledNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        // using default of true
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("_source").field("enabled", false).endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping2, parser, "Cannot update enabled setting for [_source]");

        // not changing is ok
        String mapping3 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("_source").field("enabled", true).endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping3, parser);
    }

    public void testIncludesNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("_source").array("includes", "foo.*").endObject()
            .endObject().endObject());
        assertConflicts(defaultMapping, mapping1, parser, "Cannot update includes setting for [_source]");
        assertConflicts(mapping1, defaultMapping, parser, "Cannot update includes setting for [_source]");

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("_source").array("includes", "foo.*", "bar.*").endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping2, parser, "Cannot update includes setting for [_source]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, parser);
    }

    public void testExcludesNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc").endObject().endObject());
        String mapping1 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("_source").array("excludes", "foo.*").endObject()
            .endObject().endObject());
        assertConflicts(defaultMapping, mapping1, parser, "Cannot update excludes setting for [_source]");
        assertConflicts(mapping1, defaultMapping, parser, "Cannot update excludes setting for [_source]");

        String mapping2 = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("_source").array("excludes", "foo.*", "bar.*").endObject()
            .endObject().endObject());
        assertConflicts(mapping1, mapping2, parser, "Cannot update excludes setting for [_source]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, parser);
    }

    public void testComplete() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        assertTrue(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", false).endObject()
            .endObject().endObject());
        assertFalse(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*").endObject()
            .endObject().endObject());
        assertFalse(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*").endObject()
            .endObject().endObject());
        assertFalse(parser.parse("type", new CompressedXContent(mapping)).sourceMapper().isComplete());
    }

    public void testSourceObjectContainsExtraTokens() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper documentMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(mapping));

        try {
            documentMapper.parse(new SourceToParse("test", "1",
                new BytesArray("{}}"), XContentType.JSON)); // extra end object (invalid JSON)
            fail("Expected parse exception");
        } catch (MapperParsingException e) {
            assertNotNull(e.getRootCause());
            String message = e.getRootCause().getMessage();
            assertTrue(message, message.contains("Unexpected close marker '}'"));
        }
    }
}
