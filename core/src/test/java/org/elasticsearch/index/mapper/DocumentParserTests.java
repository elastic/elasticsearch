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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

// TODO: make this a real unit test
public class DocumentParserTests extends ESSingleNodeTestCase {

    public void testTypeDisabled() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("enabled", false).endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
            .field("field", "1234")
            .endObject().endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.rootDoc().getField(UidFieldMapper.NAME));
    }

    public void testFieldDisabled() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("enabled", false).endObject()
            .startObject("bar").field("type", "integer").endObject()
            .endObject().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "1234")
            .field("bar", 10)
            .endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNull(doc.rootDoc().getField("foo"));
        assertNotNull(doc.rootDoc().getField("bar"));
        assertNotNull(doc.rootDoc().getField(UidFieldMapper.NAME));
    }

    public void testDotsWithExistingMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").startObject("properties")
            .startObject("bar").startObject("properties")
            .startObject("baz").field("type", "integer")
            .endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject()
            .field("foo.bar.baz", 123)
            .startObject("foo")
            .field("bar.baz", 456)
            .endObject()
            .startObject("foo.bar")
            .field("baz", 789)
            .endObject()
            .endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNull(doc.dynamicMappingsUpdate()); // no update!
        String[] values = doc.rootDoc().getValues("foo.bar.baz");
        assertEquals(3, values.length);
        assertEquals("123", values[0]);
        assertEquals("456", values[1]);
        assertEquals("789", values[2]);
    }

    public void testPropagateDynamicWithExistingMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", false)
            .startObject("properties")
                .startObject("foo")
                    .field("type", "object")
                    .field("dynamic", true)
                    .startObject("properties")
            .endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
            .field("bar", "something")
            .endObject().endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar"));
    }

    public void testPropagateDynamicWithDynamicMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", false)
            .startObject("properties")
            .startObject("foo")
            .field("type", "object")
            .field("dynamic", true)
            .startObject("properties")
            .endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject().startObject("foo").startObject("bar")
                .field("baz", "something")
            .endObject().endObject().endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar.baz"));
    }

    public void testDynamicRootFallback() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", false)
            .startObject("properties")
            .startObject("foo")
            .field("type", "object")
            .startObject("properties")
            .endObject().endObject().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
            .field("bar", "something")
            .endObject().endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("foo.bar"));
    }

    DocumentMapper createDummyMapping(MapperService mapperService) throws Exception {
        String mapping = jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("y").field("type", "object").endObject()
            .startObject("x").startObject("properties")
            .startObject("subx").field("type", "object").startObject("properties")
            .startObject("subsubx").field("type", "object")
            .endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject().string();

        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type", new CompressedXContent(mapping));
        return defaultMapper;
    }

    // creates an object mapper, which is about 100x harder than it should be....
    ObjectMapper createObjectMapper(MapperService mapperService, String name) throws Exception {
        String[] nameParts = name.split("\\.");
        ContentPath path = new ContentPath();
        for (int i = 0; i < nameParts.length - 1; ++i) {
            path.add(nameParts[i]);
        }
        ParseContext context = new ParseContext.InternalParseContext(Settings.EMPTY,
            mapperService.documentMapperParser(), mapperService.documentMapper("type"), path);
        Mapper.Builder builder = new ObjectMapper.Builder(nameParts[nameParts.length - 1]).enabled(true);
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings(), context.path());
        return (ObjectMapper)builder.build(builderContext);
    }

    public void testEmptyMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping(createIndex("test").mapperService());
        assertNull(DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, Collections.emptyList()));
    }

    public void testSingleMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping(createIndex("test").mapperService());
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        assertNotNull(mapping.root().getMapper("foo"));
    }

    public void testSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping(createIndex("test").mapperService());
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("x.foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)xMapper).getMapper("foo"));
        assertNull(((ObjectMapper)xMapper).getMapper("subx"));
    }

    public void testMultipleSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping(createIndex("test").mapperService());
        List<Mapper> updates = new ArrayList<>();
        updates.add(new MockFieldMapper("x.foo"));
        updates.add(new MockFieldMapper("x.bar"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)xMapper).getMapper("foo"));
        assertNotNull(((ObjectMapper)xMapper).getMapper("bar"));
        assertNull(((ObjectMapper)xMapper).getMapper("subx"));
    }

    public void testDeepSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping(createIndex("test").mapperService());
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("x.subx.foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        Mapper subxMapper = ((ObjectMapper)xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)subxMapper).getMapper("foo"));
        assertNull(((ObjectMapper)subxMapper).getMapper("subsubx"));
    }

    public void testDeepSubfieldAfterSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping(createIndex("test").mapperService());
        List<Mapper> updates = new ArrayList<>();
        updates.add(new MockFieldMapper("x.a"));
        updates.add(new MockFieldMapper("x.subx.b"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        Mapper xMapper = mapping.root().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)xMapper).getMapper("a"));
        Mapper subxMapper = ((ObjectMapper)xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)subxMapper).getMapper("b"));
    }

    public void testObjectMappingUpdate() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper docMapper = createDummyMapping(mapperService);
        List<Mapper> updates = new ArrayList<>();
        updates.add(createObjectMapper(mapperService, "foo"));
        updates.add(createObjectMapper(mapperService, "foo.bar"));
        updates.add(new MockFieldMapper("foo.bar.baz"));
        updates.add(new MockFieldMapper("foo.field"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mapping(), docMapper, updates);
        Mapper fooMapper = mapping.root().getMapper("foo");
        assertNotNull(fooMapper);
        assertTrue(fooMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)fooMapper).getMapper("field"));
        Mapper barMapper = ((ObjectMapper)fooMapper).getMapper("bar");
        assertTrue(barMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)barMapper).getMapper("baz"));
    }

    public void testDynamicArrayWithTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "geo_point").endObject()
            .endObject().endObject().endArray().endObject().endObject().string();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .startArray().value(0).value(0).endArray()
                .startArray().value(1).value(1).endArray()
            .endArray().endObject().bytes();
        ParsedDocument doc = mapper.parse("test", "type", "1", bytes);
        assertEquals(2, doc.rootDoc().getFields("foo").length);
    }
}
