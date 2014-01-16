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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class DefaultSourceMappingTests extends ElasticsearchTestCase {

    @Test
    public void testNoFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = MapperTestUtils.newParser().parse(mapping);
        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.JSON));

        documentMapper = MapperTestUtils.newParser().parse(mapping);
        doc = documentMapper.parse("type", "1", XContentFactory.smileBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.SMILE));
    }

    @Test
    public void testJsonFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("format", "json").endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = MapperTestUtils.newParser().parse(mapping);
        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.JSON));

        documentMapper = MapperTestUtils.newParser().parse(mapping);
        doc = documentMapper.parse("type", "1", XContentFactory.smileBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(XContentFactory.xContentType(doc.source()), equalTo(XContentType.JSON));
    }

    @Test
    public void testJsonFormatCompressed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("format", "json").field("compress", true).endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = MapperTestUtils.newParser().parse(mapping);
        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(CompressorFactory.isCompressed(doc.source()), equalTo(true));
        byte[] uncompressed = CompressorFactory.uncompressIfNeeded(doc.source()).toBytes();
        assertThat(XContentFactory.xContentType(uncompressed), equalTo(XContentType.JSON));

        documentMapper = MapperTestUtils.newParser().parse(mapping);
        doc = documentMapper.parse("type", "1", XContentFactory.smileBuilder().startObject()
                .field("field", "value")
                .endObject().bytes());

        assertThat(CompressorFactory.isCompressed(doc.source()), equalTo(true));
        uncompressed = CompressorFactory.uncompressIfNeeded(doc.source()).toBytes();
        assertThat(XContentFactory.xContentType(uncompressed), equalTo(XContentType.JSON));
    }

    @Test
    public void testIncludeExclude() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_source").field("includes", new String[]{"path1*"}).endObject()
                .endObject().endObject().string();

        DocumentMapper documentMapper = MapperTestUtils.newParser().parse(mapping);

        ParsedDocument doc = documentMapper.parse("type", "1", XContentFactory.jsonBuilder().startObject()
                .startObject("path1").field("field1", "value1").endObject()
                .startObject("path2").field("field2", "value2").endObject()
                .endObject().bytes());

        IndexableField sourceField = doc.rootDoc().getField("_source");
        Map<String, Object> sourceAsMap = XContentFactory.xContent(XContentType.JSON).createParser(new BytesArray(sourceField.binaryValue())).mapAndClose();
        assertThat(sourceAsMap.containsKey("path1"), equalTo(true));
        assertThat(sourceAsMap.containsKey("path2"), equalTo(false));
    }

    @Test
    public void testDefaultMappingAndNoMapping() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTestUtils.newParser().parse("my_type", null, defaultMapping);
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
        try {
            mapper = MapperTestUtils.newParser().parse(null, null, defaultMapping);
            assertThat(mapper.type(), equalTo("my_type"));
            assertThat(mapper.sourceMapper().enabled(), equalTo(false));
            fail();
        } catch (MapperParsingException e) {
            // all is well
        }
        try {
            mapper = MapperTestUtils.newParser().parse(null, "{}", defaultMapping);
            assertThat(mapper.type(), equalTo("my_type"));
            assertThat(mapper.sourceMapper().enabled(), equalTo(false));
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("malformed mapping no root object found"));
            // all is well
        }
    }

    @Test
    public void testDefaultMappingAndWithMappingOverride() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("my_type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTestUtils.newParser().parse("my_type", mapping, defaultMapping);
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }

    @Test
    public void testDefaultMappingAndNoMappingWithMapperService() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        MapperService mapperService = MapperTestUtils.newMapperService();
        mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedString(defaultMapping), true);

        DocumentMapper mapper = mapperService.documentMapperWithAutoCreate("my_type");
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(false));
    }

    @Test
    public void testDefaultMappingAndWithMappingOverrideWithMapperService() throws Exception {
        String defaultMapping = XContentFactory.jsonBuilder().startObject().startObject(MapperService.DEFAULT_MAPPING)
                .startObject("_source").field("enabled", false).endObject()
                .endObject().endObject().string();

        MapperService mapperService = MapperTestUtils.newMapperService();
        mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedString(defaultMapping), true);

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("my_type")
                .startObject("_source").field("enabled", true).endObject()
                .endObject().endObject().string();
        mapperService.merge("my_type", new CompressedString(mapping), true);

        DocumentMapper mapper = mapperService.documentMapper("my_type");
        assertThat(mapper.type(), equalTo("my_type"));
        assertThat(mapper.sourceMapper().enabled(), equalTo(true));
    }
}
