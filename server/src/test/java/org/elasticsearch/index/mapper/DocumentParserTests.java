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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

// TODO: make this a real unit test
public class DocumentParserTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testFieldDisabled() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("enabled", false).endObject()
            .startObject("bar").field("type", "integer").endObject()
            .endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo", "1234")
            .field("bar", 10)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertNull(doc.rootDoc().getField("foo"));
        assertNotNull(doc.rootDoc().getField("bar"));
        assertNotNull(doc.rootDoc().getField(IdFieldMapper.NAME));
    }

    public void testDotsWithFieldDisabled() throws IOException {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("enabled", false).endObject()
            .endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        {
            BytesReference bytes = BytesReference.bytes(jsonBuilder()
                .startObject()
                .field("foo.bar", 111)
                .endObject());
            ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
            assertNull(doc.rootDoc().getField("foo"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("foo.bar"));
        }
        {
            BytesReference bytes = BytesReference.bytes(jsonBuilder()
                .startObject()
                .field("foo.bar", new int[]{1, 2, 3})
                .endObject());
            ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
            assertNull(doc.rootDoc().getField("foo"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("foo.bar"));
        }
        {
            BytesReference bytes = BytesReference.bytes(jsonBuilder()
                .startObject()
                .field("foo.bar", Collections.singletonMap("key", "value"))
                .endObject());
            ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
            assertNull(doc.rootDoc().getField("foo"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("foo.bar"));
        }
        {
            BytesReference bytes = BytesReference.bytes(jsonBuilder()
                .startObject()
                .field("foo.bar", "string value")
                .field("blub", 222)
                .endObject());
            ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
            assertNull(doc.rootDoc().getField("foo"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("foo.bar"));
            assertNotNull(doc.rootDoc().getField("blub"));
        }
    }

    public void testDotsWithExistingMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").startObject("properties")
            .startObject("bar").startObject("properties")
            .startObject("baz").field("type", "integer")
            .endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo.bar.baz", 123)
            .startObject("foo")
            .field("bar.baz", 456)
            .endObject()
            .startObject("foo.bar")
            .field("baz", 789)
            .endObject()
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertNull(doc.dynamicMappingsUpdate()); // no update!
        String[] values = doc.rootDoc().getValues("foo.bar.baz");
        assertEquals(3, values.length);
        assertEquals("123", values[0]);
        assertEquals("456", values[1]);
        assertEquals("789", values[2]);
    }

    public void testDotsWithExistingNestedMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("type", "nested").startObject("properties")
            .startObject("bar").field("type", "integer")
            .endObject().endObject().endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo.bar", 123)
            .endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals(
                "Cannot add a value for field [foo.bar] since one of the intermediate objects is mapped as a nested object: [foo]",
                e.getMessage());
    }

    public void testUnexpectedFieldMappingType() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("foo").field("type", "long").endObject()
                .startObject("bar").field("type", "boolean").endObject()
                .startObject("geo").field("type", "geo_shape").endObject()
                .endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        {
            BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("foo", true)
                .endObject());
            MapperException exception = expectThrows(MapperException.class,
                    () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
            assertThat(exception.getMessage(), containsString("failed to parse field [foo] of type [long] in document with id '1'"));
        }
        {
            BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("bar", "bar")
                .endObject());
            MapperException exception = expectThrows(MapperException.class,
                    () -> mapper.parse(new SourceToParse("test", "2", bytes, XContentType.JSON)));
            assertThat(exception.getMessage(), containsString("failed to parse field [bar] of type [boolean] in document with id '2'"));
        }
        {
            BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("geo", 123)
                .endObject());
            MapperException exception = expectThrows(MapperException.class,
                    () -> mapper.parse(new SourceToParse("test", "2", bytes, XContentType.JSON)));
            assertThat(exception.getMessage(), containsString("failed to parse field [geo]"));
        }

    }

    public void testDotsWithDynamicNestedMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_templates")
                    .startObject()
                        .startObject("objects_as_nested")
                            .field("match_mapping_type", "object")
                            .startObject("mapping")
                                .field("type", "nested")
                            .endObject()
                        .endObject()
                    .endObject()
                .endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
            .field("foo.bar",42)
            .endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals(
                "It is forbidden to create dynamic nested objects ([foo]) through `copy_to` or dots in field names",
                e.getMessage());
    }

    public void testNestedHaveIdAndTypeFields() throws Exception {
        DocumentMapperParser mapperParser2 = createIndex("index2").mapperService().documentMapperParser();

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties");
        {
            mapping.startObject("foo");
            mapping.field("type", "nested");
            {
                mapping.startObject("properties");
                {

                    mapping.startObject("bar");
                    mapping.field("type", "keyword");
                    mapping.endObject();
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        {
            mapping.startObject("baz");
            mapping.field("type", "keyword");
            mapping.endObject();
        }
        mapping.endObject().endObject().endObject();
        DocumentMapper mapper = mapperParser2.parse("type", new CompressedXContent(Strings.toString(mapping)));

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
        {
            doc.startArray("foo");
            {
                doc.startObject();
                doc.field("bar", "value1");
                doc.endObject();
            }
            doc.endArray();
            doc.field("baz", "value2");
        }
        doc.endObject();

        // Verify in the case where only a single type is allowed that the _id field is added to nested documents:
        ParsedDocument result = mapper.parse(new SourceToParse("index2", "1",
            BytesReference.bytes(doc), XContentType.JSON));
        assertEquals(2, result.docs().size());
        // Nested document:
        assertNotNull(result.docs().get(0).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(0).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.NESTED_FIELD_TYPE, result.docs().get(0).getField(IdFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(0).getField(TypeFieldMapper.NAME));
        assertEquals("__foo", result.docs().get(0).getField(TypeFieldMapper.NAME).stringValue());
        assertEquals("value1", result.docs().get(0).getField("foo.bar").binaryValue().utf8ToString());
        // Root document:
        assertNotNull(result.docs().get(1).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(1).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.FIELD_TYPE, result.docs().get(1).getField(IdFieldMapper.NAME).fieldType());
        assertNull(result.docs().get(1).getField(TypeFieldMapper.NAME));
        assertEquals("value2", result.docs().get(1).getField("baz").binaryValue().utf8ToString());
    }

    public void testPropagateDynamicWithExistingMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", false)
            .startObject("properties")
                .startObject("foo")
                    .field("type", "object")
                    .field("dynamic", true)
                    .startObject("properties")
            .endObject().endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
            .field("bar", "something")
            .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar"));
    }

    public void testPropagateDynamicWithDynamicMapper() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", false)
            .startObject("properties")
            .startObject("foo")
            .field("type", "object")
            .field("dynamic", true)
            .startObject("properties")
            .endObject().endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startObject("foo").startObject("bar")
                .field("baz", "something")
            .endObject().endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar.baz"));
    }

    public void testDynamicRootFallback() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", false)
            .startObject("properties")
            .startObject("foo")
            .field("type", "object")
            .startObject("properties")
            .endObject().endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
            .field("bar", "something")
            .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("foo.bar"));
    }

    DocumentMapper createDummyMapping(MapperService mapperService) throws Exception {
        String mapping = Strings.toString(jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("y").field("type", "object").endObject()
            .startObject("x").startObject("properties")
            .startObject("subx").field("type", "object").startObject("properties")
            .startObject("subsubx").field("type", "object")
            .endObject().endObject().endObject().endObject().endObject().endObject().endObject().endObject());

        DocumentMapper defaultMapper = mapperService.documentMapperParser().parse("type", new CompressedXContent(mapping));
        return defaultMapper;
    }

    // creates an object mapper, which is about 100x harder than it should be....
    ObjectMapper createObjectMapper(MapperService mapperService, String name) throws Exception {
        IndexMetaData build = IndexMetaData.builder("")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0).build();
        IndexSettings settings = new IndexSettings(build, Settings.EMPTY);
        ParseContext context = new ParseContext.InternalParseContext(settings,
            mapperService.documentMapperParser(), mapperService.documentMapper(), null, null);
        String[] nameParts = name.split("\\.");
        for (int i = 0; i < nameParts.length - 1; ++i) {
            context.path().add(nameParts[i]);
        }
        Mapper.Builder builder = new ObjectMapper.Builder(nameParts[nameParts.length - 1]).enabled(true);
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(context.indexSettings().getSettings(), context.path());
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

    public void testDynamicGeoPointArrayWithTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "geo_point").field("doc_values", false).endObject()
            .endObject().endObject().endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .startArray().value(0).value(0).endArray()
                .startArray().value(1).value(1).endArray()
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicLongArrayWithTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "long").endObject()
            .endObject().endObject().endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicFalseLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicStrictLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .value(0)
                .value(1)
            .endArray().endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testMappedGeoPointArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "geo_point")
            .field("doc_values", false)
                .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .startArray().value(0).value(0).endArray()
                .startArray().value(1).value(1).endArray()
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo").length);
    }

    public void testMappedLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "long")
                .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicObjectWithTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "object")
                .startObject("properties").startObject("bar").field("type", "keyword").endObject().endObject().endObject()
            .endObject().endObject().endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("foo")
                    .field("bar", "baz")
                .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar").length);
    }

    public void testDynamicFalseObject() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startObject("foo")
                .field("bar", "baz")
            .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("foo.bar").length);
    }

    public void testDynamicStrictObject() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("foo")
                    .field("bar", "baz")
                .endObject().endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicFalseValue() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .field("bar", "baz")
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("bar").length);
    }

    public void testDynamicStrictValue() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                    .field("bar", "baz")
                .endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicFalseNull() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .field("bar", (String) null)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("bar").length);
    }

    public void testDynamicStrictNull() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("bar", (String) null)
                .endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed", exception.getMessage());
    }

    public void testMappedNullValue() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("foo").field("type", "long")
                .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().field("foo", (Long) null)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicBigInteger() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startArray("dynamic_templates").startObject()
                    .startObject("big-integer-to-keyword")
                        .field("match", "big-*")
                        .field("match_mapping_type", "long")
                        .startObject("mapping").field("type", "keyword").endObject()
                    .endObject()
                .endObject().endArray()
            .endObject()
        .endObject());

        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .field("big-integer", value)
        .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("big-integer");
        assertEquals(2, fields.length);
        assertTrue(fields[0].fieldType() instanceof KeywordFieldMapper.KeywordFieldType);
        assertEquals(new BytesRef(value.toString()), fields[0].binaryValue());
    }

    public void testDynamicBigDecimal() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startArray("dynamic_templates").startObject()
                    .startObject("big-decimal-to-scaled-float")
                        .field("match", "big-*")
                        .field("match_mapping_type", "double")
                        .startObject("mapping")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject().endArray()
            .endObject()
        .endObject());

        BigDecimal value = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(10.1));
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .field("big-decimal", value)
        .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));

        IndexableField[] fields = doc.rootDoc().getFields("big-decimal");
        assertEquals(2, fields.length);
        assertTrue(fields[0].fieldType() instanceof KeywordFieldMapper.KeywordFieldType);
        assertEquals(new BytesRef(value.toString()), fields[0].binaryValue());
    }

    public void testDynamicDottedFieldNameLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startArray("foo.bar.baz")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongArrayWithParentTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "object").endObject()
            .endObject().endObject().endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("foo.bar.baz")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongArrayWithExistingParent() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties") .startObject("foo")
            .field("type", "object")
            .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("foo.bar.baz")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongArrayWithExistingParentWrongType() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties") .startObject("foo")
            .field("type", "long")
            .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("foo.bar.baz")
                .value(0)
                .value(1)
            .endArray().endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("Could not dynamically add mapping for field [foo.bar.baz]. "
                + "Existing mapping for [foo] must be of type object but found [long].", exception.getMessage());
    }

    public void testDynamicFalseDottedFieldNameLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("foo.bar.baz")
                .value(0)
                .value(1)
            .endArray().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicStrictDottedFieldNameLongArray() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("foo.bar.baz")
                .value(0)
                .value(1)
            .endArray().endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicDottedFieldNameLong() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().field("foo.bar.baz", 0)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongWithParentTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "object").endObject()
            .endObject().endObject().endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("foo.bar.baz", 0)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongWithExistingParent() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties") .startObject("foo")
            .field("type", "object")
            .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("foo.bar.baz", 0)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameLongWithExistingParentWrongType() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties") .startObject("foo")
            .field("type", "long")
            .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("foo.bar.baz", 0)
            .endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("Could not dynamically add mapping for field [foo.bar.baz]. "
                + "Existing mapping for [foo] must be of type object but found [long].", exception.getMessage());
    }

    public void testDynamicFalseDottedFieldNameLong() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("foo.bar.baz", 0)
            .endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicStrictDottedFieldNameLong() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().field("foo.bar.baz", 0)
            .endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicDottedFieldNameObject() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject().startObject("foo.bar.baz")
                .field("a", 0)
            .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(ObjectMapper.class));
        Mapper aMapper = ((ObjectMapper) bazMapper).getMapper("a");
        assertNotNull(aMapper);
        assertThat(aMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameObjectWithParentTemplate() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startArray("dynamic_templates").startObject().startObject("georule")
                .field("match", "foo*")
                .startObject("mapping").field("type", "object").endObject()
            .endObject().endObject().endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("foo.bar.baz")
                .field("a", 0)
            .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(ObjectMapper.class));
        Mapper aMapper = ((ObjectMapper) bazMapper).getMapper("a");
        assertNotNull(aMapper);
        assertThat(aMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameObjectWithExistingParent() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo").field("type", "object").endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().startObject("foo.bar.baz")
            .field("a", 0).endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().root().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(ObjectMapper.class));
        Mapper aMapper = ((ObjectMapper) bazMapper).getMapper("a");
        assertNotNull(aMapper);
        assertThat(aMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testDynamicDottedFieldNameObjectWithExistingParentWrongType() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties") .startObject("foo")
            .field("type", "long")
            .endObject().endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().startObject("foo.bar.baz")
            .field("a", 0).endObject().endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));

        assertEquals("Could not dynamically add mapping for field [foo.bar.baz]. "
                + "Existing mapping for [foo] must be of type object but found [long].", exception.getMessage());
    }

    public void testDynamicFalseDottedFieldNameObject() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "false")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("foo.bar.baz")
                .field("a", 0)
            .endObject().endObject());
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz.a").length);
    }

    public void testDynamicStrictDottedFieldNameObject() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .field("dynamic", "strict")
            .endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startObject("foo.bar.baz")
                .field("a", 0)
            .endObject().endObject());
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDocumentContainsMetadataField() throws Exception {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("_ttl", 0).endObject());
        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));
        assertTrue(e.getMessage(), e.getMessage().contains("cannot be added inside a document"));

        BytesReference bytes2 = BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .field("foo._ttl", 0).endObject());
        mapper.parse(new SourceToParse("test", "1", bytes2, XContentType.JSON)); // parses without error
    }

    public void testSimpleMapper() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapper docMapper = new DocumentMapper.Builder(
                new RootObjectMapper.Builder("person")
                        .add(new ObjectMapper.Builder("name")
                            .add(new TextFieldMapper.Builder("first").store(true).index(false))),
            indexService.mapperService()).build(indexService.mapperService());

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
        doc = docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
    }

    public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse("person", new CompressedXContent(mapping));
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse("_doc", new CompressedXContent(builtMapping));
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = builtDocMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("person", new CompressedXContent(mapping));

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("person", new CompressedXContent(mapping));
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1-notype-noid.json"));
        Document doc = docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse("person", new CompressedXContent(mapping));

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        DocumentMapper builtDocMapper = parser.parse("_doc", new CompressedXContent(builtMapping));
        assertThat((String) builtDocMapper.meta().get("param1"), equalTo("value1"));
    }

    public void testNoDocumentSent() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapper docMapper = new DocumentMapper.Builder(
                new RootObjectMapper.Builder("person")
                        .add(new ObjectMapper.Builder("name")
                            .add(new TextFieldMapper.Builder("first").store(true).index(false))),
            indexService.mapperService()).build(indexService.mapperService());

        BytesReference json = new BytesArray("".getBytes(StandardCharsets.UTF_8));
        try {
            docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
            fail("this point is never reached");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("failed to parse, document is empty"));
        }
    }

    public void testNoLevel() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevel() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsValue() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("type", "value_type")
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsValue() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .field("type", "value_type")
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsObject() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("type").field("type_field", "type_value").endObject()
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject()),
                XContentType.JSON));

        // in this case, we analyze the type object as the actual document, and ignore the other same level fields
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
    }

    public void testTypeLevelWithFieldTypeAsObject() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .startObject("type").field("type_field", "type_value").endObject()
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsValueNotFirst() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .field("test1", "value1")
                        .field("test2", "value2")
                        .field("type", "value_type")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsValueNotFirst() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .field("test1", "value1")
                        .field("type", "value_type")
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("test1", "value1")
                        .startObject("type").field("type_field", "type_value").endObject()
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject()),
                XContentType.JSON));

        // when the type is not the first one, we don't confuse it...
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        String defaultMapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject());

        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("type", new CompressedXContent(defaultMapping));

        ParsedDocument doc = defaultMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .field("test1", "value1")
                        .startObject("type").field("type_field", "type_value").endObject()
                        .field("test2", "value2")
                        .startObject("inner").field("inner_field", "inner_value").endObject()
                        .endObject().endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testDynamicDateDetectionDisabledOnNumbers() throws IOException {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_date_formats")
                    .value("yyyy")
                .endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .field("foo", "2016")
            .endObject());

        // Even though we matched the dynamic format, we do not match on numbers,
        // which are too likely to be false positives
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update);
        Mapper dateMapper = update.root().getMapper("foo");
        assertNotNull(dateMapper);
        assertThat(dateMapper, not(instanceOf(DateFieldMapper.class)));
    }

    public void testDynamicDateDetectionEnabledWithNoSpecialCharacters() throws IOException {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startArray("dynamic_date_formats")
                    .value("yyyy MM")
                .endArray().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .field("foo", "2016 12")
            .endObject());

        // We should have generated a date field
        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update);
        Mapper dateMapper = update.root().getMapper("foo");
        assertNotNull(dateMapper);
        assertThat(dateMapper, instanceOf(DateFieldMapper.class));
    }

    public void testDynamicFieldsStartingAndEndingWithDot() throws Exception {
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().startArray("top.")
                .startObject().startArray("foo.")
                .startObject()
                .field("thing", "bah")
                .endObject().endArray()
                .endObject().endArray()
                .endObject());

        client().prepareIndex("idx").setSource(bytes, XContentType.JSON).get();

        bytes = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().startArray("top.")
                .startObject().startArray("foo.")
                .startObject()
                .startObject("bar.")
                .startObject("aoeu")
                .field("a", 1).field("b", 2)
                .endObject()
                .endObject()
                .endObject()
                .endArray().endObject().endArray()
                .endObject());

        try {
            client().prepareIndex("idx").setSource(bytes, XContentType.JSON).get();
            fail("should have failed to dynamically introduce a double-dot field");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(),
                containsString("object field starting or ending with a [.] makes object resolution ambiguous: [top..foo..bar]"));
        }
    }

    public void testDynamicFieldsEmptyName() throws Exception {
        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject().startArray("top.")
                .startObject()
                .startObject("aoeu")
                .field("a", 1).field(" ", 2)
                .endObject()
                .endObject().endArray()
                .endObject());

        IllegalArgumentException emptyFieldNameException = expectThrows(IllegalArgumentException.class,
                () -> client().prepareIndex("idx").setSource(bytes, XContentType.JSON).get());

        assertThat(emptyFieldNameException.getMessage(), containsString(
                "object field cannot contain only whitespace: ['top.aoeu. ']"));
    }

    public void testBlankFieldNames() throws Exception {
        final BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("", "foo")
                .endObject());

        MapperParsingException err = expectThrows(MapperParsingException.class, () ->
                client().prepareIndex("idx").setSource(bytes, XContentType.JSON).get());
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot be an empty string"));

        final BytesReference bytes2 = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("foo")
                .field("", "bar")
                .endObject()
                .endObject());

        err = expectThrows(MapperParsingException.class, () ->
                client().prepareIndex("idx").setSource(bytes2, XContentType.JSON).get());
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot be an empty string"));
    }

    public void testWriteToFieldAlias() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "concrete-field")
                        .endObject()
                        .startObject("concrete-field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .field("alias-field", "value")
            .endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));

        assertEquals("Cannot write to a field alias [alias-field].", exception.getCause().getMessage());
    }

     public void testCopyToFieldAlias() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "concrete-field")
                        .endObject()
                        .startObject("concrete-field")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("text-field")
                            .field("type", "text")
                            .field("copy_to", "alias-field")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .field("text-field", "value")
            .endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));

        assertEquals("Cannot copy to a field alias [alias-field].", exception.getCause().getMessage());
    }

    public void testDynamicDottedFieldNameWithFieldAlias() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("type")
                    .startObject("properties")
                        .startObject("alias-field")
                            .field("type", "alias")
                            .field("path", "concrete-field")
                        .endObject()
                        .startObject("concrete-field")
                            .field("type", "keyword")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject());

        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
            .startObject()
                .startObject("alias-field.dynamic-field")
                    .field("type", "keyword")
                .endObject()
            .endObject());
        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON)));

        assertEquals("Could not dynamically add mapping for field [alias-field.dynamic-field]. "
            + "Existing mapping for [alias-field] must be of type object but found [alias].", exception.getMessage());
    }

    public void testTypeless() throws IOException {
        DocumentMapperParser mapperParser = createIndex("test").mapperService().documentMapperParser();
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("foo").field("type", "keyword").endObject()
                .endObject().endObject().endObject());
        DocumentMapper mapper = mapperParser.parse("type", new CompressedXContent(mapping));

        BytesReference bytes = BytesReference.bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("foo", "1234")
                .endObject());

        ParsedDocument doc = mapper.parse(new SourceToParse("test", "1", bytes, XContentType.JSON));
        assertNull(doc.dynamicMappingsUpdate()); // no update since we reused the existing type
    }
}
