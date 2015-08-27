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

package org.elasticsearch.index.mapper.source;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DefaultSourceMappingTests extends ESSingleNodeTestCase {

    public void testNoFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").endObject()
                .endObject().endObject().string();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse(mapping);
        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.JSON));

        documentMapper = parser.parse(mapping);
        doc = documentMapper.parse("test", "type", "1", XContentFactory.smileBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.SMILE));
    }

    public void testJsonFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("format", "json").endObject()
                .endObject().endObject().string();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse(mapping);
        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.JSON));

        documentMapper = parser.parse(mapping);
        doc = documentMapper.parse("test", "type", "1", XContentFactory.smileBuilder().startObject()
            .field("field", "value")
            .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.JSON));
    }

    public void testJsonFormatCompressedBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("format", "json").field("compress", true).endObject()
                .endObject().endObject().string();

        Settings backcompatSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapperParser parser = createIndex("test", backcompatSettings).mapperService().documentMapperParser();
        DocumentMapper documentMapper = parser.parse(mapping);
        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(CompressorFactory.isCompressed(doc.source()), equalTo(true));
        byte[] uncompressed = CompressorFactory.uncompressIfNeeded(doc.source()).toBytes();
        assertThat(XContentFactory.xContentType(uncompressed), equalTo(XContentType.JSON));

        documentMapper = parser.parse(mapping);
        doc = documentMapper.parse("test", "type", "1", XContentFactory.smileBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(CompressorFactory.isCompressed(doc.source()), equalTo(true));
        uncompressed = CompressorFactory.uncompressIfNeeded(doc.source()).toBytes();
        assertThat(XContentFactory.xContentType(uncompressed), equalTo(XContentType.JSON));
    }

    public void testIncludes() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("includes", new String[]{"path1*"}).endObject()
            .endObject().endObject().string();

        DocumentMapper documentMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
            .startObject("path1").field("field1", "value1").endObject()
            .startObject("path2").field("field2", "value2").endObject()
            .endObject().bytes());

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));
    }

    public void testExcludes() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("excludes", new String[]{"path1*"}).endObject()
            .endObject().endObject().string();

        DocumentMapper documentMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
            .startObject("path1").field("field1", "value1").endObject()
            .startObject("path2").field("field2", "value2").endObject()
            .endObject().bytes());

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap;
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new BytesArray(sourceField.binaryValue()))) {
            sourceAsMap = parser.map();
        }
        assertThat(sourceAsMap.containsKey("path1"), equalTo(false));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(true));
    }

    public void testDefaultMappingAndNoMapping() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper mapper = parser.parse("my_type", null, defaultMapping);
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
        try {
            mapper = parser.parse(null, null, defaultMapping);
            assertThat(mapper.type(), equalTo("my_type"));
            assertThat(mapper.sourceMapper().enabled(), equalTo(false));
            fail();
        } catch (MapperParsingException e) {
            // all is well
        }
        try {
            mapper = parser.parse(null, "{}", defaultMapping);
            assertThat(mapper.type(), equalTo("my_type"));
            assertThat(mapper.sourceMapper().enabled(), equalTo(false));
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("malformed mapping no root object found"));
            // all is well
        }
    }

    public void testDefaultMappingAndWithMappingOverride() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("my_type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = createIndex("test").mapperService().documentMapperParser().parse("my_type", mapping, defaultMapping);
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }

    public void testDefaultMappingAndNoMappingWithMapperService() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedXContent(defaultMapping), true, false);

        DocumentMapper mapper = mapperService.documentMapperWithAutoCreate("my_type").getDocumentMapper();
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
    }

    public void testDefaultMappingAndWithMappingOverrideWithMapperService() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        MapperService mapperService = createIndex("test").mapperService();
        mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedXContent(defaultMapping), true, false);

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("my_type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject().string();
        mapperService.merge("my_type", new CompressedXContent(mapping), true, false);

        DocumentMapper mapper = mapperService.documentMapper("my_type");
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }

    void assertConflicts(String mapping1, String mapping2, DocumentMapperParser parser, String... conflicts) throws IOException {
        DocumentMapper docMapper = parser.parse(mapping1);
        docMapper = parser.parse(docMapper.mappingSource().string());
        MergeResult mergeResult = docMapper.merge(parser.parse(mapping2).mapping(), true, false);

        List<String> expectedConflicts = new ArrayList<>(Arrays.asList(conflicts));
        for (String conflict : mergeResult.buildConflicts()) {
            assertTrue("found unexpected conflict [" + conflict + "]", expectedConflicts.remove(conflict));
        }
        assertTrue("missing conflicts: " + Arrays.toString(expectedConflicts.toArray()), expectedConflicts.isEmpty());
    }

    public void testEnabledNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        // using default of true
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", false).endObject()
            .endObject().endObject().string();
        assertConflicts(mapping1, mapping2, parser, "Cannot update enabled setting for [_source]");

        // not changing is ok
        String mapping3 = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", true).endObject()
            .endObject().endObject().string();
        assertConflicts(mapping1, mapping3, parser);
    }

    public void testIncludesNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*").endObject()
            .endObject().endObject().string();
        assertConflicts(defaultMapping, mapping1, parser, "Cannot update includes setting for [_source]");
        assertConflicts(mapping1, defaultMapping, parser, "Cannot update includes setting for [_source]");

        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*", "bar.*").endObject()
            .endObject().endObject().string();
        assertConflicts(mapping1, mapping2, parser, "Cannot update includes setting for [_source]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, parser);
    }

    public void testExcludesNotUpdateable() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        String mapping1 = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*").endObject()
            .endObject().endObject().string();
        assertConflicts(defaultMapping, mapping1, parser, "Cannot update excludes setting for [_source]");
        assertConflicts(mapping1, defaultMapping, parser, "Cannot update excludes setting for [_source]");

        String mapping2 = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*", "bar.*").endObject()
            .endObject().endObject().string();
        assertConflicts(mapping1, mapping2, parser, "Cannot update excludes setting for [_source]");

        // not changing is ok
        assertConflicts(mapping1, mapping1, parser);
    }

    public void testComplete() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        assertTrue(parser.parse(mapping).sourceMapper().isComplete());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").field("enabled", false).endObject()
            .endObject().endObject().string();
        assertFalse(parser.parse(mapping).sourceMapper().isComplete());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("includes", "foo.*").endObject()
            .endObject().endObject().string();
        assertFalse(parser.parse(mapping).sourceMapper().isComplete());

        mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_source").array("excludes", "foo.*").endObject()
            .endObject().endObject().string();
        assertFalse(parser.parse(mapping).sourceMapper().isComplete());
    }

    public void testSourceObjectContainsExtraTokens() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper documentMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        try {
            documentMapper.parse("test", "type", "1", new BytesArray("{}}")); // extra end object (invalid JSON)
            fail("Expected parse exception");
        } catch (MapperParsingException e) {
            assertNotNull(e.getRootCause());
            String message = e.getRootCause().getMessage();
            assertTrue(message, message.contains("Unexpected close marker '}'"));
        }
    }
}
