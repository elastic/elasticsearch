/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DocumentParserTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new DocumentParserTestsPlugin());
    }

    public void testParseWithRuntimeField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(runtimeFieldMapping(b -> b.field("type", "keyword")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "value")));
        //field defined as runtime field but not under properties: no dynamic updates, the field does not get indexed
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("field"));
    }

    public void testParseWithRuntimeFieldArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(runtimeFieldMapping(b -> b.field("type", "keyword")));
        ParsedDocument doc = mapper.parse(source(b -> b.array("field", "value1", "value2")));
        //field defined as runtime field but not under properties: no dynamic updates, the field does not get indexed
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("field"));
    }

    public void testParseWithShadowedField() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        builder.startObject("runtime");
        builder.startObject("field").field("type", "keyword").endObject();
        builder.endObject();
        builder.startObject("properties");
        builder.startObject("field").field("type", "keyword").endObject();
        builder.endObject().endObject().endObject();

        DocumentMapper mapper = createDocumentMapper(builder);
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "value")));
        //field defined as runtime field as well as under properties: no dynamic updates, the field gets indexed
        assertNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("field"));
    }

    public void testParseWithRuntimeFieldDottedNameDisabledObject() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        builder.startObject("runtime");
        builder.startObject("path1.path2.path3.field").field("type", "keyword").endObject();
        builder.endObject();
        builder.startObject("properties");
        builder.startObject("path1").field("type", "object").field("enabled", false).endObject();
        builder.endObject().endObject().endObject();
        MapperService mapperService = createMapperService(builder);
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("path1").startObject("path2").startObject("path3");
            b.field("field", "value");
            b.endObject().endObject().endObject();
        }));
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("path1.path2.path3.field"));
    }

    public void testParseWithShadowedSubField() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        builder.startObject("runtime");
        builder.startObject("field.keyword").field("type", "keyword").endObject();
        builder.endObject();
        builder.startObject("properties");
        builder.startObject("field").field("type", "text");
        builder.startObject("fields").startObject("keyword").field("type", "keyword").endObject().endObject();
        builder.endObject().endObject().endObject().endObject();

        DocumentMapper mapper = createDocumentMapper(builder);
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "value")));
        //field defined as runtime field as well as under properties: no dynamic updates, the field gets indexed
        assertNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.rootDoc().getField("field.keyword"));
    }

    public void testParseWithShadowedMultiField() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        builder.startObject("runtime");
        builder.startObject("field").field("type", "keyword").endObject();
        builder.endObject();
        builder.startObject("properties");
        builder.startObject("field").field("type", "text");
        builder.startObject("fields").startObject("keyword").field("type", "keyword").endObject().endObject();
        builder.endObject().endObject().endObject().endObject();

        DocumentMapper mapper = createDocumentMapper(builder);
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "value")));
        //field defined as runtime field as well as under properties: no dynamic updates, the field gets indexed
        assertNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("field"));
        assertNotNull(doc.rootDoc().getField("field.keyword"));
    }

    public void testParseWithShadowedNestedField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("runtime");
            {
                b.startObject("child.name").field("type", "keyword").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("child");
                {
                    b.field("type", "nested");
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("name", "alice");
            b.field("age", 42);
            b.startArray("child");
            {
                b.startObject().field("name", "bob").field("age", 12).endObject();
                b.startObject().field("name", "charlie").field("age", 10).endObject();
            }
            b.endArray();
        }));
        assertEquals(3, doc.docs().size());
        assertEquals("alice", doc.rootDoc().getField("name").stringValue());
        assertNull(doc.docs().get(0).getField("child.name"));   // shadowed by the runtime field
        assertEquals(12L, doc.docs().get(0).getField("child.age").numericValue());
        assertNull(doc.docs().get(1).getField("child.name"));
        assertEquals(10L, doc.docs().get(1).getField("child.age").numericValue());
    }

    public void testRuntimeFieldAndArrayChildren() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("runtime");
            {
                b.startObject("object").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.startObject("object");
                b.array("array", 1, 2, 3);
                b.field("foo", "bar");
                b.endObject();
            }));
            assertNotNull(doc.rootDoc().getField("object.foo"));
            assertNotNull(doc.rootDoc().getField("object.array"));
        }

        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.startArray("object");
                {
                    b.startObject().array("array", 1, 2, 3).endObject();
                    b.startObject().field("foo", "bar").endObject();
                }
                b.endArray();
            }));
            assertNotNull(doc.rootDoc().getField("object.foo"));
            assertNotNull(doc.rootDoc().getField("object.array"));
        }
    }

    public void testRuntimeFieldDoesNotShadowObjectChildren() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", "true");
            b.startObject("runtime");
            {
                b.startObject("location").field("type", "keyword").endObject();
                b.startObject("country").field("type", "keyword").endObject();
            }
            b.endObject();
            b.startObject("properties");
            {
                b.startObject("timestamp").field("type", "date").endObject();
                b.startObject("concrete").field("type", "keyword").endObject();
            }
            b.endObject();
        }));

        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.field("timestamp", "1998-04-30T14:30:17-05:00");
                b.startObject("location");
                {
                    b.field("lat", 13.5);
                    b.field("lon", 34.89);
                }
                b.endObject();
                b.field("country", "de");
                b.field("concrete", "foo");
            }));

            assertNotNull(doc.rootDoc().getField("timestamp"));
            assertNotNull(doc.rootDoc().getField("_source"));
            assertNotNull(doc.rootDoc().getField("location.lat"));
            assertNotNull(doc.rootDoc().getField("location.lon"));
            assertNotNull(doc.rootDoc().getField("concrete"));
            assertNull(doc.rootDoc().getField("country"));
        }

        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.field("timestamp", "1998-04-30T14:30:17-05:00");
                b.startArray("location");
                {
                    b.startObject().field("lat", 13.5).field("lon", 34.89).endObject();
                    b.startObject().field("lat", 14.5).field("lon", 89.33).endObject();
                }
                b.endArray();
                b.field("country", "de");
                b.field("concrete", "foo");
            }));

            assertNotNull(doc.rootDoc().getField("timestamp"));
            assertNotNull(doc.rootDoc().getField("_source"));
            assertThat(doc.rootDoc().getFields("location.lat").length, equalTo(4));
            assertThat(doc.rootDoc().getFields("location.lon").length, equalTo(4));
            assertNotNull(doc.rootDoc().getField("concrete"));
            assertNull(doc.rootDoc().getField("country"));
        }

        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.field("timestamp", "1998-04-30T14:30:17-05:00");
                b.startObject("location");
                {
                    b.array("lat", 13.5, 14.5);
                    b.array("lon", 34.89, 89.33);
                }
                b.endObject();
                b.field("country", "de");
                b.field("concrete", "foo");
            }));

            assertNotNull(doc.rootDoc().getField("timestamp"));
            assertNotNull(doc.rootDoc().getField("_source"));
            assertThat(doc.rootDoc().getFields("location.lat").length, equalTo(4));
            assertThat(doc.rootDoc().getFields("location.lon").length, equalTo(4));
            assertNotNull(doc.rootDoc().getField("concrete"));
            assertNull(doc.rootDoc().getField("country"));
        }

    }

    public void testFieldDisabled() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo").field("enabled", false).endObject();
            b.startObject("bar").field("type", "integer").endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("foo", "1234");
            b.field("bar", 10);
        }));
        assertNull(doc.rootDoc().getField("foo"));
        assertNotNull(doc.rootDoc().getField("bar"));
        assertNotNull(doc.rootDoc().getField(IdFieldMapper.NAME));
    }

    public void testDotsWithFieldDisabled() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("enabled", false)));
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", 111)));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", new int[]{1, 2, 3})));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", Collections.singletonMap("key", "value"))));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> {
                b.field("field.bar", "string value");
                b.field("blub", 222);
            }));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
            assertNotNull(doc.rootDoc().getField("blub"));
        }
    }

    public void testDotsWithExistingMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo");
            {
                b.startObject("properties");
                {
                    b.startObject("bar");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("baz").field("type", "integer").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("foo.bar.baz", 123);
            b.startObject("foo");
            {
                b.field("bar.baz", 456);
            }
            b.endObject();
            b.startObject("foo.bar");
            {
                b.field("baz", 789);
            }
            b.endObject();
        }));
        assertNull(doc.dynamicMappingsUpdate()); // no update!

        IndexableField[] fields = doc.rootDoc().getFields("foo.bar.baz");
        assertEquals(6, fields.length);
        assertEquals(123, fields[0].numericValue());
        assertEquals("123", fields[1].stringValue());
        assertEquals(456, fields[2].numericValue());
        assertEquals("456", fields[3].stringValue());
        assertEquals(789, fields[4].numericValue());
        assertEquals("789", fields[5].stringValue());
    }

    public void testDotsWithExistingNestedMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "nested");
            b.startObject("properties");
            {
                b.startObject("bar").field("type", "integer").endObject();
            }
            b.endObject();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(source(b -> b.field("field.bar", 123))));
        assertEquals(
                "Cannot add a value for field [field.bar] since one of the intermediate objects is mapped as a nested object: [field]",
                e.getMessage());
    }

    public void testUnexpectedFieldMappingType() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo").field("type", "long").endObject();
            b.startObject("bar").field("type", "boolean").endObject();
        }));
        {
            MapperException exception = expectThrows(MapperException.class,
                    () -> mapper.parse(source(b -> b.field("foo", true))));
            assertThat(exception.getMessage(), containsString("failed to parse field [foo] of type [long] in document with id '1'"));
        }
        {
            MapperException exception = expectThrows(MapperException.class,
                    () -> mapper.parse(source(b -> b.field("bar", "bar"))));
            assertThat(exception.getMessage(), containsString("failed to parse field [bar] of type [boolean] in document with id '1'"));
        }
    }

    public void testDotsWithDynamicNestedMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("objects_as_nested");
                    {
                        b.field("match_mapping_type", "object");
                        b.startObject("mapping").field("type", "nested").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        MapperParsingException e = expectThrows(MapperParsingException.class,
                () -> mapper.parse(source(b -> b.field("foo.bar", 42))));
        assertEquals(
                "It is forbidden to create dynamic nested objects ([foo]) through `copy_to` or dots in field names",
                e.getMessage());
    }

    public void testNestedHaveIdAndTypeFields() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "nested");
                b.startObject("properties");
                {
                    b.startObject("bar").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("baz").field("type", "keyword").endObject();
        }));

        // Verify in the case where only a single type is allowed that the _id field is added to nested documents:
        ParsedDocument result = mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().field("bar", "value1").endObject();
            }
            b.endArray();
            b.field("baz", "value2");
        }));
        assertEquals(2, result.docs().size());
        // Nested document:
        assertNotNull(result.docs().get(0).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(0).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.NESTED_FIELD_TYPE, result.docs().get(0).getField(IdFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(0).getField(NestedPathFieldMapper.NAME));
        assertEquals("foo", result.docs().get(0).getField(NestedPathFieldMapper.NAME).stringValue());
        assertEquals("value1", result.docs().get(0).getField("foo.bar").binaryValue().utf8ToString());
        // Root document:
        assertNotNull(result.docs().get(1).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(1).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(IdFieldMapper.Defaults.FIELD_TYPE, result.docs().get(1).getField(IdFieldMapper.NAME).fieldType());
        assertNull(result.docs().get(1).getField(NestedPathFieldMapper.NAME));
        assertEquals("value2", result.docs().get(1).getField("baz").binaryValue().utf8ToString());
    }

    public void testPropagateDynamicWithExistingMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.field("dynamic", true);
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "something").endObject()));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar"));
    }

    public void testPropagateDynamicWithDynamicMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.field("dynamic", true);
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", "something").endObject();
            }
            b.endObject();
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("foo.bar.baz"));
    }

    public void testPropagateDynamicRuntimeWithDynamicMapper() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.field("dynamic", "runtime");
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("foo");
            {
                b.field("baz", "test");
                b.startObject("bar").field("baz", "something").endObject();
            }
            b.endObject();
        }));
        assertNull(doc.rootDoc().getField("foo.bar.baz"));
        assertEquals("{\"_doc\":{\"dynamic\":\"false\"," +
            "\"runtime\":{\"foo.bar.baz\":{\"type\":\"keyword\"},\"foo.baz\":{\"type\":\"keyword\"}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRootFallback() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            {
                b.startObject("foo");
                {
                    b.field("type", "object");
                    b.startObject("properties").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "something").endObject()));
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("foo.bar"));
    }

    DocumentMapper createDummyMapping() throws Exception {
        return createMapperService().documentMapper();
    }

    MapperService createMapperService() throws Exception {
        return createMapperService(mapping(b -> {
            b.startObject("y").field("type", "object").endObject();
            b.startObject("x");
            {
                b.startObject("properties");
                {
                    b.startObject("subx");
                    {
                        b.field("type", "object");
                        b.startObject("properties");
                        {
                            b.startObject("subsubx").field("type", "object").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
    }

    // creates an object mapper, which is about 100x harder than it should be....
    private static ObjectMapper createObjectMapper(String name) {
        ContentPath path = new ContentPath(0);
        String[] nameParts = name.split("\\.");
        for (int i = 0; i < nameParts.length - 1; ++i) {
            path.add(nameParts[i]);
        }
        return new ObjectMapper.Builder(nameParts[nameParts.length - 1]).enabled(true).build(path);
    }

    public void testEmptyMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        assertNull(DocumentParser.createDynamicUpdate(docMapper.mappers(), Collections.emptyList(), Collections.emptyList()));
    }

    public void testSingleMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), updates, Collections.emptyList());
        assertNotNull(mapping);
        assertNotNull(mapping.getRoot().getMapper("foo"));
    }

    public void testSingleRuntimeFieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<RuntimeField> updates = Collections.singletonList(new TestRuntimeField("foo", "any"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), Collections.emptyList(), updates);
        assertNotNull(mapping);
        assertNull(mapping.getRoot().getMapper("foo"));
        assertNotNull(mapping.getRoot().getRuntimeField("foo"));
    }

    public void testSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("x.foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), updates, Collections.emptyList());
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)xMapper).getMapper("foo"));
        assertNull(((ObjectMapper)xMapper).getMapper("subx"));
    }

    public void testRuntimeSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<RuntimeField> updates = Collections.singletonList(new TestRuntimeField("x.foo", "any"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), Collections.emptyList(), updates);
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNull(xMapper);
        assertNotNull(mapping.getRoot().getRuntimeField("x.foo"));
    }

    public void testMultipleSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = new ArrayList<>();
        updates.add(new MockFieldMapper("x.foo"));
        updates.add(new MockFieldMapper("x.bar"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), updates, Collections.emptyList());
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)xMapper).getMapper("foo"));
        assertNotNull(((ObjectMapper)xMapper).getMapper("bar"));
        assertNull(((ObjectMapper)xMapper).getMapper("subx"));
    }

    public void testDeepSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = Collections.singletonList(new MockFieldMapper("x.subx.foo"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), updates, Collections.emptyList());
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        Mapper subxMapper = ((ObjectMapper)xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)subxMapper).getMapper("foo"));
        assertNull(((ObjectMapper)subxMapper).getMapper("subsubx"));
    }

    public void testDeepSubfieldAfterSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        List<Mapper> updates = new ArrayList<>();
        updates.add(new MockFieldMapper("x.a"));
        updates.add(new MockFieldMapper("x.subx.b"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), updates, Collections.emptyList());
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)xMapper).getMapper("a"));
        Mapper subxMapper = ((ObjectMapper)xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)subxMapper).getMapper("b"));
    }

    public void testObjectMappingUpdate() throws Exception {
        MapperService mapperService = createMapperService();
        DocumentMapper docMapper = mapperService.documentMapper();
        List<Mapper> updates = new ArrayList<>();
        updates.add(createObjectMapper("foo"));
        updates.add(createObjectMapper("foo.bar"));
        updates.add(new MockFieldMapper("foo.bar.baz"));
        updates.add(new MockFieldMapper("foo.field"));
        Mapping mapping = DocumentParser.createDynamicUpdate(docMapper.mappers(), updates, Collections.emptyList());
        assertNotNull(mapping);
        Mapper fooMapper = mapping.getRoot().getMapper("foo");
        assertNotNull(fooMapper);
        assertTrue(fooMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)fooMapper).getMapper("field"));
        Mapper barMapper = ((ObjectMapper)fooMapper).getMapper("bar");
        assertTrue(barMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper)barMapper).getMapper("baz"));
    }

    public void testDynamicGeoPointArrayWithTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping");
                        {
                            b.field("type", "geo_point");
                            b.field("doc_values", false);
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startArray().value(0).value(0).endArray();
                b.startArray().value(1).value(1).endArray();
            }
            b.endArray();
        }));
        assertEquals(2, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicLongArrayWithTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicFalseLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicStrictLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray())));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicRuntimeLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("{\"foo\":{\"type\":\"long\"}}", Strings.toString(foo));
    }

    public void testDynamicRuntimeDoubleArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0.25).value(1.43).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("{\"foo\":{\"type\":\"double\"}}", Strings.toString(foo));
    }

    public void testDynamicRuntimeStringArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value("test1").value("test2").endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("{\"foo\":{\"type\":\"keyword\"}}", Strings.toString(foo));
    }

    public void testDynamicRuntimeBooleanArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(true).value(false).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("{\"foo\":{\"type\":\"boolean\"}}", Strings.toString(foo));
    }

    public void testDynamicRuntimeDateArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value("2020-12-15").value("2020-12-09").endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("{\"foo\":{\"type\":\"date\"}}", Strings.toString(foo));
    }

    public void testMappedGeoPointArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> {
            b.field("type", "geo_point");
            b.field("doc_values", false);
        }));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("field");
            {
                b.startArray().value(0).value(0).endArray();
                b.startArray().value(1).value(1).endArray();
            }
            b.endArray();
        }));
        assertEquals(2, doc.rootDoc().getFields("field").length);
    }

    public void testMappedLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("field").length);
    }

    public void testDynamicObjectWithTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping");
                        {
                            b.field("type", "object");
                            b.startObject("properties");
                            {
                                b.startObject("bar").field("type", "keyword").endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()));
        assertEquals(2, doc.rootDoc().getFields("foo.bar").length);
    }

    public void testDynamicFalseObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar").length);
    }

    public void testDynamicStrictObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject())));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicFalseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bar", "baz")));
        assertEquals(0, doc.rootDoc().getFields("bar").length);
    }

    public void testDynamicStrictValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.field("bar", "baz"))));
        assertEquals("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicFalseNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("bar")));
        assertEquals(0, doc.rootDoc().getFields("bar").length);
    }

    public void testDynamicStrictNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.nullField("bar"))));
        assertEquals("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed", exception.getMessage());
    }

    public void testMappedNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("foo")));
        assertEquals(0, doc.rootDoc().getFields("foo").length);
    }

    public void testDynamicBigInteger() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("big-integer-to-keyword");
                    {
                        b.field("match", "big-*");
                        b.field("match_mapping_type", "long");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        ParsedDocument doc = mapper.parse(source(b -> b.field("big-integer", value)));

        IndexableField[] fields = doc.rootDoc().getFields("big-integer");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef(value.toString()), fields[0].binaryValue());
    }

    public void testDynamicBigDecimal() throws Exception {

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("big-decimal-to-scaled-float");
                    {
                        b.field("match", "big-*");
                        b.field("match_mapping_type", "double");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        BigDecimal value = BigDecimal.valueOf(Double.MAX_VALUE).add(BigDecimal.valueOf(10.1));
        ParsedDocument doc = mapper.parse(source(b -> b.field("big-decimal", value)));

        IndexableField[] fields = doc.rootDoc().getFields("big-decimal");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef(value.toString()), fields[0].binaryValue());
    }

    public void testDynamicDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("foo");
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
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(4, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("foo");
        assertNotNull(fooMapper);
        assertThat(fooMapper, instanceOf(ObjectMapper.class));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertNotNull(barMapper);
        assertThat(barMapper, instanceOf(ObjectMapper.class));
        Mapper bazMapper = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(bazMapper);
        assertThat(bazMapper, instanceOf(NumberFieldMapper.class));
    }

    public void testWithDynamicTemplates() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("points");
                    {
                        b.field("match", "none"); // do not map anything
                        b.startObject("mapping");
                        {
                            b.field("type", "geo_point");
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        String field = randomFrom("loc", "foo.loc", "foo.bar.loc");

        ParsedDocument doc = mapper.parse(source("1", b -> b.field(field, "41.12,-71.34"), null, null, Map.of(field, "points")));
        IndexableField[] fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(2));
        assertThat(fields[0].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[1].fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> b.field(field, new double[]{-71.34, 41.12}), null, null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(2));
        assertThat(fields[0].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[1].fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> {
            b.startObject(field);
            b.field("lat", "-71.34");
            b.field("lon", 41.12);
            b.endObject();
        }, null, null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(2));
        assertThat(fields[0].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[1].fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(
            source("1", b -> b.field(field, new String[] { "41.12,-71.34", "43,-72.34" }), null, null, Map.of(field, "points"))
        );
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(4));
        assertThat(fields[0].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[1].fieldType(), sameInstance(LatLonDocValuesField.TYPE));
        assertThat(fields[2].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[3].fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> {
            b.startArray(field);
            b.startObject();
            b.field("lat", -71.34);
            b.field("lon", 41.12);
            b.endObject();

            b.startObject();
            b.field("lat", -71.34);
            b.field("lon", 41.12);
            b.endObject();
            b.endArray();
        }, null, null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(4));
        assertThat(fields[0].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[1].fieldType(), sameInstance(LatLonDocValuesField.TYPE));
        assertThat(fields[2].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[3].fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> {
            b.startObject("address");
            b.field("home", "43,-72.34");
            b.endObject();
        }, null, null, Map.of("address.home", "points")));
        fields = doc.rootDoc().getFields("address.home");
        assertThat(fields, arrayWithSize(2));
        assertThat(fields[0].fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields[1].fieldType(), sameInstance(LatLonDocValuesField.TYPE));
    }

    public void testDynamicTemplatesNotFound() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("booleans");
                    {
                        b.field("match", "none");
                        b.startObject("mapping");
                        {
                            b.field("type", "boolean");
                            b.field("store", false);
                            b.field("doc_values", false);
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        String field = randomFrom("foo", "foo.bar", "foo.bar.baz");
        ParsedDocument doc = mapper.parse(source("1", b -> b.field(field, "true"), null, null, Map.of(field, "booleans")));
        IndexableField[] fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(1));
        assertThat(fields[0].fieldType(), sameInstance(BooleanFieldMapper.Defaults.FIELD_TYPE));
        MapperParsingException error = expectThrows(MapperParsingException.class, () ->
            mapper.parse(source("1", b -> b.field(field, "hello"), null, null, Map.of(field, "foo_bar"))));
        assertThat(error.getMessage(),
            containsString("Can't find dynamic template for dynamic template name [foo_bar] of field [" + field + "]"));
    }

    public void testWrongTypeDynamicTemplate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("booleans");
                    {
                        b.field("match", "none");
                        b.startObject("mapping");
                        {
                            b.field("type", "boolean");
                            b.field("store", false);
                            b.field("doc_values", false);
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        String field = randomFrom("foo.bar", "foo.bar.baz");
        MapperParsingException error = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source("1", b -> b.field(field, "true"), null, null, Map.of("foo", "booleans"))));
        assertThat(error.getMessage(),
            containsString("Field [foo] must be an object; but it's configured as [boolean] in dynamic template [booleans]"));

        ParsedDocument doc = mapper.parse(source("1", b -> b.field(field, "true"), null, null, Map.of(field, "booleans")));
        IndexableField[] fields = doc.rootDoc().getFields(field);
        assertThat(fields, arrayWithSize(1));
        assertThat(fields[0].fieldType(), sameInstance(BooleanFieldMapper.Defaults.FIELD_TYPE));
    }

    public void testDynamicDottedFieldNameLongArrayWithExistingParent() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field.bar.baz").value(0).value(1).endArray()));

        assertEquals(4, doc.rootDoc().getFields("field.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("field");
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
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        MapperParsingException exception = expectThrows(MapperParsingException.class,
                () -> mapper.parse(source(b -> b.startArray("field.bar.baz").value(0).value(1).endArray())));
        assertEquals("Could not dynamically add mapping for field [field.bar.baz]. "
                + "Existing mapping for [field] must be of type object but found [long].", exception.getMessage());
    }

    public void testDynamicFalseDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicStrictDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray())));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("foo");
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
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("foo");
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
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar.baz", 0)));
        assertEquals(2, doc.rootDoc().getFields("field.bar.baz").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("field");
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
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        MapperParsingException exception = expectThrows(MapperParsingException.class,
                () -> mapper.parse(source(b -> b.field("field.bar.baz", 0))));
        assertEquals("Could not dynamically add mapping for field [field.bar.baz]. "
                + "Existing mapping for [field] must be of type object but found [long].", exception.getMessage());
    }

    public void testDynamicFalseDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").length);
    }

    public void testDynamicStrictDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.field("foo.bar.baz", 0))));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDynamicDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("foo");
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
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("georule");
                    {
                        b.field("match", "foo*");
                        b.startObject("mapping").field("type", "object").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));

        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("foo");
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
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("field.bar.baz").field("a", 0).endObject()));
        assertEquals(2, doc.rootDoc().getFields("field.bar.baz.a").length);
        Mapper fooMapper = doc.dynamicMappingsUpdate().getRoot().getMapper("field");
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
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        MapperParsingException exception = expectThrows(MapperParsingException.class,
                () -> mapper.parse(source(b -> b.startObject("field.bar.baz").field("a", 0).endObject())));
        assertEquals("Could not dynamically add mapping for field [field.bar.baz]. "
                + "Existing mapping for [field] must be of type object but found [long].", exception.getMessage());
    }

    public void testDynamicFalseDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz.a").length);
    }

    public void testDynamicStrictDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        StrictDynamicMappingException exception = expectThrows(StrictDynamicMappingException.class,
                () -> mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject())));
        assertEquals("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed", exception.getMessage());
    }

    public void testDocumentContainsMetadataField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        MapperParsingException e = expectThrows(MapperParsingException.class, () ->
            mapper.parse(source(b -> b.field("_field_names", 0))));
        assertTrue(e.getCause().getMessage(),
            e.getCause().getMessage().contains("Field [_field_names] is a metadata field and cannot be added inside a document."));

        mapper.parse(source(b -> b.field("foo._field_names", 0))); // parses without error
    }

    public void testDocumentContainsAllowedMetadataField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        {
            // A metadata field that parses a value fails to parse a null value
            MapperParsingException e = expectThrows(MapperParsingException.class, () ->
                mapper.parse(source(b -> b.nullField(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE))));
            assertTrue(e.getMessage(), e.getMessage().contains("failed to parse field [_mock_metadata]"));
        }
        {
            // A metadata field that parses a value fails to parse an object
            MapperParsingException e = expectThrows(MapperParsingException.class, () ->
                mapper.parse(source(b -> b.field(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE)
                    .startObject().field("sub-field", "true").endObject())));
            assertTrue(e.getMessage(), e.getMessage().contains("failed to parse field [_mock_metadata]"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b ->
                b.field(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE, "mock-metadata-field-value")
            ));
            IndexableField field = doc.rootDoc().getField(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE);
            assertEquals("mock-metadata-field-value", field.stringValue());
        }
    }

    public void testSimpleMapper() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {
            b.startObject("name");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("first");
                    {
                        b.field("type", "text");
                        b.field("store", "true");
                        b.field("index", false);
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        LuceneDocument doc = docMapper.parse(source(b -> {
            b.startObject("name");
            {
                b.field("first", "shay");
                b.field("last", "banon");
            }
            b.endObject();
        })).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, mapping);
        String builtMapping = mapperService.documentMapper().mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = createDocumentMapper(builtMapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        LuceneDocument doc = builtDocMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(builtDocMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(builtDocMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(mapping);

        assertThat((String) docMapper.mapping().getMeta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        LuceneDocument doc = docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1-notype-noid.json"));
        LuceneDocument doc = docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(docMapper.idFieldMapper().name()), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");

        DocumentMapper docMapper = createDocumentMapper(mapping);

        assertThat((String) docMapper.mapping().getMeta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        DocumentMapper builtDocMapper = createDocumentMapper(builtMapping);
        assertThat((String) builtDocMapper.mapping().getMeta().get("param1"), equalTo("value1"));
    }

    public void testNoDocumentSent() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(mapping(b -> {}));
        BytesReference json = new BytesArray("".getBytes(StandardCharsets.UTF_8));
        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> docMapper.parse(new SourceToParse("test", "1", json, XContentType.JSON)));
        assertThat(e.getMessage(), equalTo("failed to parse, document is empty"));
    }

    public void testNoLevel() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("test1", "value1");
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    // TODO do we still need all this tests for 'type' at the bottom level now that
    // we no longer have types?

    public void testTypeLevel() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsValue() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("type", "value_type");
            b.field("test1", "value1");
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));

        assertThat(doc.rootDoc().get("type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsValue() throws Exception {

        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("type", "value_type");
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type"), equalTo("value_type"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsObject() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type").field("type_field", "type_value").endObject();
            b.field("test1", "value1");
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));

        // in this case, we analyze the type object as the actual document, and ignore the other same level fields
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
    }

    public void testTypeLevelWithFieldTypeAsObject() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.startObject("type").field("type_field", "type_value").endObject();
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsValueNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.field("test2", "value2");
                b.field("type", "type_value");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsValueNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.field("type", "type_value");
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testNoLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("test1", "value1");
            b.startObject("type").field("type_field", "type_value").endObject();
            b.field("test2", "value2");
            b.startObject("inner").field("inner_field", "inner_value").endObject();
        }));

        // when the type is not the first one, we don't confuse it...
        assertThat(doc.rootDoc().get("type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("inner.inner_field"), equalTo("inner_value"));
    }

    public void testTypeLevelWithFieldTypeAsObjectNotFirst() throws Exception {
        DocumentMapper defaultMapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("type");
            {
                b.field("test1", "value1");
                b.startObject("type").field("type_field", "type_value").endObject();
                b.field("test2", "value2");
                b.startObject("inner").field("inner_field", "inner_value").endObject();
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("type.type.type_field"), equalTo("type_value"));
        assertThat(doc.rootDoc().get("type.test1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("type.test2"), equalTo("value2"));
        assertThat(doc.rootDoc().get("type.inner.inner_field"), equalTo("inner_value"));
    }

    public void testDynamicDateDetectionDisabledOnNumbers() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.startArray("dynamic_date_formats").value("yyyy").endArray()));

        // Even though we matched the dynamic format, we do not match on numbers,
        // which are too likely to be false positives
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "2016")));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update);
        Mapper dateMapper = update.getRoot().getMapper("foo");
        assertNotNull(dateMapper);
        assertThat(dateMapper, not(instanceOf(DateFieldMapper.class)));
    }

    public void testDynamicDateDetectionEnabledWithNoSpecialCharacters() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.startArray("dynamic_date_formats").value("yyyy MM").endArray()));

        // We should have generated a date field
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "2016 12")));
        Mapping update = doc.dynamicMappingsUpdate();
        assertNotNull(update);
        Mapper dateMapper = update.getRoot().getMapper("foo");
        assertNotNull(dateMapper);
        assertThat(dateMapper, instanceOf(DateFieldMapper.class));
    }

    public void testDynamicFieldsStartingAndEndingWithDot() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        merge(mapperService, dynamicMapping(mapperService.documentMapper().parse(source(b -> {
            b.startArray("top.");
            {
                b.startObject();
                {
                    b.startArray("foo.");
                    {
                        b.startObject().field("thing", "bah").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        })).dynamicMappingsUpdate()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> mapperService.documentMapper().parse(source(b -> {
            b.startArray("top.");
            {
                b.startObject();
                {
                    b.startArray("foo.");
                    {
                        b.startObject();
                        {
                            b.startObject("bar.");
                            {
                                b.startObject("aoeu").field("a", 1).field("b", 2).endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        })));

        assertThat(e.getMessage(),
                containsString("object field starting or ending with a [.] makes object resolution ambiguous: [top..foo..bar]"));
    }

    public void testDynamicFieldsEmptyName() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));

        IllegalArgumentException emptyFieldNameException = expectThrows(IllegalArgumentException.class,
                () -> mapper.parse(source(b -> {
                    b.startArray("top.");
                    {
                        b.startObject();
                        {
                            b.startObject("aoeu").field("a", 1).field(" ", 2).endObject();
                        }
                        b.endObject();
                    }
                    b.endArray();
                })));

        assertThat(emptyFieldNameException.getMessage(), containsString(
                "object field cannot contain only whitespace: ['top.aoeu. ']"));
    }

    public void testBlankFieldNames() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        MapperParsingException err = expectThrows(MapperParsingException.class, () ->
                mapper.parse(source(b -> b.field("", "foo"))));
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot be an empty string"));

        err = expectThrows(MapperParsingException.class, () ->
                mapper.parse(source(b -> b.startObject("foo").field("", "bar").endObject())));
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot be an empty string"));
    }

    public void testDotsOnlyFieldNames() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        MapperParsingException err = expectThrows(
            MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field(randomFrom(".", "..", "..."), "bar")))
        );
        assertThat(err.getCause(), notNullValue());
        assertThat(err.getCause().getMessage(), containsString("field name cannot contain only dots"));
    }

    public void testWriteToFieldAlias() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
            b.startObject("concrete-field").field("type", "keyword").endObject();
        }));

        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("alias-field", "value"))));

        assertEquals("Cannot write to a field alias [alias-field].", exception.getCause().getMessage());
    }

     public void testCopyToFieldAlias() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
            b.startObject("concrete-field").field("type", "keyword").endObject();
            b.startObject("text-field");
            {
                b.field("type", "text");
                b.field("copy_to", "alias-field");
            }
            b.endObject();
        }));

        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("text-field", "value"))));

        assertEquals("Cannot copy to a field alias [alias-field].", exception.getCause().getMessage());
    }

    public void testDynamicDottedFieldNameWithFieldAlias() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("alias-field");
            {
                b.field("type", "alias");
                b.field("path", "concrete-field");
            }
            b.endObject();
            b.startObject("concrete-field").field("type", "keyword").endObject();
        }));

        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("alias-field.dynamic-field").field("type", "keyword").endObject())));

        assertEquals("Could not dynamically add mapping for field [alias-field.dynamic-field]. "
            + "Existing mapping for [alias-field] must be of type object but found [alias].", exception.getMessage());
    }

    public void testMultifieldOverwriteFails() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("message");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                {
                    b.startObject("text");
                    {
                        b.field("type", "text");
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        MapperParsingException exception = expectThrows(MapperParsingException.class,
            () -> mapper.parse(source(b -> b.field("message", "original").field("message.text", "overwrite"))));

        assertEquals("Could not dynamically add mapping for field [message.text]. "
            + "Existing mapping for [message] must be of type object but found [keyword].", exception.getMessage());
    }

    public void testTypeless() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject().startObject("type").startObject("properties")
                .startObject("foo").field("type", "keyword").endObject()
                .endObject().endObject().endObject());
        DocumentMapper mapper = createDocumentMapper(mapping);

        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "1234")));
        assertNull(doc.dynamicMappingsUpdate()); // no update since we reused the existing type
    }

    public void testParseWithFlattenedField() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "flattened")));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("field");
            b.field("first", "first");
            b.field("second", "second");
            b.endObject();
        }));
        assertNull(doc.dynamicMappingsUpdate());
        assertNotNull(doc.rootDoc().getField("field"));
        assertNull(doc.rootDoc().getField("field.first"));
        assertNull(doc.rootDoc().getField("field.second"));
    }

    public void testDynamicShadowingOfRuntimeSubfields() throws IOException {

        // Create mappings with a runtime field called 'obj' that produces two subfields,
        // 'obj.foo' and 'obj.bar'

        DocumentMapper mapper = createDocumentMapper(topMapping(b -> {
            b.startObject("runtime");
            b.startObject("obj").field("type", "test-composite").endObject();
            b.endObject();
        }));

        // Incoming documents should not create mappings for 'obj.foo' fields, as they will
        // be shadowed by the runtime fields; but other subfields are fine and should be
        // indexed

        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("obj");
            b.field("foo", "ignored");
            b.field("baz", "indexed");
            b.field("bar", "ignored");
            b.endObject();
        }));

        assertNull(doc.rootDoc().getField("obj.foo"));
        assertNotNull(doc.rootDoc().getField("obj.baz"));
        assertNull(doc.rootDoc().getField("obj.bar"));
        assertNotNull(doc.dynamicMappingsUpdate());
    }

    /**
     * Mapper plugin providing a mock metadata field mapper implementation that supports setting its value
     */
    private static final class DocumentParserTestsPlugin extends Plugin implements MapperPlugin {
        /**
         * A mock metadata field mapper that supports being set from the document source.
         */
        private static final class MockMetadataMapper extends MetadataFieldMapper {
            private static final String CONTENT_TYPE = "_mock_metadata";
            private static final String FIELD_NAME = "_mock_metadata";

            protected MockMetadataMapper() {
                super(new KeywordFieldMapper.KeywordFieldType(FIELD_NAME));
            }

            @Override
            protected void parseCreateField(DocumentParserContext context) throws IOException {
                if (context.parser().currentToken() == XContentParser.Token.VALUE_STRING) {
                    context.doc().add(new StringField(FIELD_NAME, context.parser().text(), Field.Store.YES));
                } else {
                    throw new IllegalArgumentException("Field [" + fieldType().name() + "] must be a string.");
                }
            }

            @Override
            protected String contentType() {
                return CONTENT_TYPE;
            }

            private static final TypeParser PARSER = new FixedTypeParser(c -> new MockMetadataMapper());
        }

        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap(MockMetadataMapper.CONTENT_TYPE, MockMetadataMapper.PARSER);
        }

        @Override
        public Map<String, RuntimeField.Parser> getRuntimeFields() {
            return Collections.singletonMap(
                "test-composite",
                new RuntimeField.Parser(n -> new RuntimeField.Builder(n) {
                    @Override
                    protected RuntimeField createRuntimeField(MappingParserContext parserContext)
                    {
                        return new TestRuntimeField(n, List.of(
                            new KeywordFieldMapper.KeywordFieldType(n + ".foo"),
                            new KeywordFieldMapper.KeywordFieldType(n + ".bar")
                        ));
                    }

                    @Override
                    protected RuntimeField createChildRuntimeField(
                        MappingParserContext parserContext,
                        String parentName,
                        Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory
                    ) {
                        throw new UnsupportedOperationException();
                    }
                })
            );
        }
    }
}
