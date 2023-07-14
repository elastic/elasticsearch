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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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
        // field defined as runtime field but not under properties: no dynamic updates, the field does not get indexed
        assertNull(doc.dynamicMappingsUpdate());
        assertNull(doc.rootDoc().getField("field"));
    }

    public void testParseWithRuntimeFieldArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(runtimeFieldMapping(b -> b.field("type", "keyword")));
        ParsedDocument doc = mapper.parse(source(b -> b.array("field", "value1", "value2")));
        // field defined as runtime field but not under properties: no dynamic updates, the field does not get indexed
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
        // field defined as runtime field as well as under properties: no dynamic updates, the field gets indexed
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
        // field defined as runtime field as well as under properties: no dynamic updates, the field gets indexed
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
        // field defined as runtime field as well as under properties: no dynamic updates, the field gets indexed
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
            assertThat(doc.rootDoc().getFields("location.lat"), hasSize(2));
            assertThat(doc.rootDoc().getFields("location.lon"), hasSize(2));
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
            assertThat(doc.rootDoc().getFields("location.lat"), hasSize(2));
            assertThat(doc.rootDoc().getFields("location.lon"), hasSize(2));
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
            ParsedDocument doc = mapper.parse(source(b -> {
                b.field("field.bar", "string value");
                b.field("blub", 222);
            }));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
            assertNotNull(doc.rootDoc().getField("blub"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", 111)));
            assertNull(doc.rootDoc().getField("field"));
            assertNull(doc.rootDoc().getField("bar"));
            assertNull(doc.rootDoc().getField("field.bar"));
        }
        {
            ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", new int[] { 1, 2, 3 })));
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

        List<IndexableField> fields = doc.rootDoc().getFields("foo.bar.baz");
        assertEquals(3, fields.size());
        assertEquals("IntField <foo.bar.baz:123>", fields.get(0).toString());
        assertEquals("IntField <foo.bar.baz:456>", fields.get(1).toString());
        assertEquals("IntField <foo.bar.baz:789>", fields.get(2).toString());
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

        ParsedDocument doc = mapper.parse(source(b -> b.field("field.bar", 123)));
        assertEquals(123, doc.docs().get(0).getNumericValue("field.bar"));
    }

    public void testUnexpectedFieldMappingType() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("foo").field("type", "long").endObject();
            b.startObject("bar").field("type", "boolean").endObject();
        }));
        {
            DocumentParsingException exception = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.field("foo", true)))
            );
            assertThat(exception.getMessage(), containsString("failed to parse field [foo] of type [long] in document with id '1'"));
        }
        {
            DocumentParsingException exception = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.field("bar", "bar")))
            );
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

        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar", 42)));
        assertEquals(42L, doc.docs().get(0).getNumericValue("foo.bar"));
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
        assertEquals(StringField.TYPE_NOT_STORED, result.docs().get(0).getField(IdFieldMapper.NAME).fieldType());
        assertNotNull(result.docs().get(0).getField(NestedPathFieldMapper.NAME));
        assertEquals("foo", result.docs().get(0).getField(NestedPathFieldMapper.NAME).stringValue());
        assertEquals("value1", result.docs().get(0).getField("foo.bar").binaryValue().utf8ToString());
        // Root document:
        assertNotNull(result.docs().get(1).getField(IdFieldMapper.NAME));
        assertEquals(Uid.encodeId("1"), result.docs().get(1).getField(IdFieldMapper.NAME).binaryValue());
        assertEquals(StringField.TYPE_STORED, result.docs().get(1).getField(IdFieldMapper.NAME).fieldType());
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
        assertEquals(
            """
                {"_doc":{"dynamic":"false","runtime":{"foo.bar.baz":{"type":"keyword"},"foo.baz":{"type":"keyword"}}}}""",
            Strings.toString(doc.dynamicMappingsUpdate())
        );
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
        return createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "runtime*");
                        b.startObject("runtime").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
            b.startObject("properties");
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
            b.endObject();
        }));
    }

    public void testEmptyMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> {}));
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testSingleMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("foo", 10)));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        assertNotNull(mapping.getRoot().getMapper("foo"));
    }

    public void testSingleRuntimeFieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("runtime-field", "10")));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        assertNull(mapping.getRoot().getMapper("runtime-field"));
        assertNotNull(mapping.getRoot().getRuntimeField("runtime-field"));
    }

    public void testSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("x.foo", 10)));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) xMapper).getMapper("foo"));
        assertNull(((ObjectMapper) xMapper).getMapper("subx"));
    }

    public void testRuntimeSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = docMapper.parse(source(b -> b.field("runtime.foo", 10)));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("runtime");
        assertNull(xMapper);
        assertNotNull(mapping.getRoot().getRuntimeField("runtime.foo"));
    }

    public void testMultipleSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> {
            b.field("x.foo", 10);
            b.field("x.bar", 20);
        }));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) xMapper).getMapper("foo"));
        assertNotNull(((ObjectMapper) xMapper).getMapper("bar"));
        assertNull(((ObjectMapper) xMapper).getMapper("subx"));
    }

    public void testDeepSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> b.field("x.subx.foo", 10)));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        Mapper subxMapper = ((ObjectMapper) xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) subxMapper).getMapper("foo"));
        assertNull(((ObjectMapper) subxMapper).getMapper("subsubx"));
    }

    public void testDeepSubfieldAfterSubfieldMappingUpdate() throws Exception {
        DocumentMapper docMapper = createDummyMapping();
        ParsedDocument doc = docMapper.parse(source(b -> {
            b.field("x.a", 10);
            b.field("x.subx.b", 10);
        }));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper xMapper = mapping.getRoot().getMapper("x");
        assertNotNull(xMapper);
        assertTrue(xMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) xMapper).getMapper("a"));
        Mapper subxMapper = ((ObjectMapper) xMapper).getMapper("subx");
        assertTrue(subxMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) subxMapper).getMapper("b"));
    }

    public void testObjectMappingUpdate() throws Exception {
        MapperService mapperService = createMapperService();
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument doc = docMapper.parse(source(b -> {
            b.startObject("foo");
            b.startObject("bar");
            b.field("baz", 10);
            b.endObject();
            b.field("field", 10);
            b.endObject();
        }));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper fooMapper = mapping.getRoot().getMapper("foo");
        assertNotNull(fooMapper);
        assertTrue(fooMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) fooMapper).getMapper("field"));
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertTrue(barMapper instanceof ObjectMapper);
        assertNotNull(((ObjectMapper) barMapper).getMapper("baz"));
    }

    public void testEmptyObjectGetsMapped() throws Exception {
        MapperService mapperService = createMapperService();
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument doc = docMapper.parse(source(b -> {
            b.startObject("foo");
            b.endObject();
        }));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper foo = mapping.getRoot().getMapper("foo");
        assertThat(foo, instanceOf(ObjectMapper.class));
        assertEquals(0, ((ObjectMapper) foo).mappers.size());
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
        assertEquals(2, doc.rootDoc().getFields("foo").size());
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
        assertEquals(2, doc.rootDoc().getFields("foo").size());
    }

    public void testDynamicLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(2, doc.rootDoc().getFields("foo").size());
    }

    public void testDynamicFalseLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
    }

    public void testDynamicStrictLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed")
        );
    }

    public void testDynamicRuntimeLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("""
            {"foo":{"type":"long"}}""", Strings.toString(foo));
    }

    public void testDynamicRuntimeDoubleArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(0.25).value(1.43).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("""
            {"foo":{"type":"double"}}""", Strings.toString(foo));
    }

    public void testDynamicRuntimeStringArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value("test1").value("test2").endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("""
            {"foo":{"type":"keyword"}}""", Strings.toString(foo));
    }

    public void testDynamicRuntimeBooleanArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value(true).value(false).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("""
            {"foo":{"type":"boolean"}}""", Strings.toString(foo));
    }

    public void testDynamicRuntimeDateArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "runtime")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value("2020-12-15").value("2020-12-09").endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
        RuntimeField foo = doc.dynamicMappingsUpdate().getRoot().getRuntimeField("foo");
        assertEquals("""
            {"foo":{"type":"date"}}""", Strings.toString(foo));
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
        assertEquals(2, doc.rootDoc().getFields("field").size());
    }

    public void testMappedLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field").value(0).value(1).endArray()));
        assertEquals(2, doc.rootDoc().getFields("field").size());
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
        assertEquals(1, doc.rootDoc().getFields("foo.bar").size());
    }

    public void testDynamicFalseObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar").size());
    }

    public void testDynamicStrictObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("foo").field("bar", "baz").endObject()))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed")
        );
    }

    public void testDynamicFalseValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("bar", "baz")));
        assertEquals(0, doc.rootDoc().getFields("bar").size());
    }

    public void testDynamicStrictValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("bar", "baz")))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed")
        );
    }

    public void testDynamicFalseNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("bar")));
        assertEquals(0, doc.rootDoc().getFields("bar").size());
    }

    public void testDynamicStrictNull() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.nullField("bar")))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [bar] within [_doc] is not allowed")
        );
    }

    public void testMappedNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapper.parse(source(b -> b.nullField("foo")));
        assertEquals(0, doc.rootDoc().getFields("foo").size());
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

        List<IndexableField> fields = doc.rootDoc().getFields("big-integer");
        assertEquals(1, fields.size());
        assertEquals(new BytesRef(value.toString()), fields.get(0).binaryValue());
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

        List<IndexableField> fields = doc.rootDoc().getFields("big-decimal");
        assertEquals(1, fields.size());
        assertEquals(new BytesRef(value.toString()), fields.get(0).binaryValue());
    }

    public void testDynamicDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));

        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").size());
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
        assertEquals(2, doc.rootDoc().getFields("foo.bar.baz").size());
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

        ParsedDocument doc = mapper.parse(source("1", b -> b.field(field, "41.12,-71.34"), null, Map.of(field, "points")));
        List<IndexableField> fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(1).fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> b.field(field, new double[] { -71.34, 41.12 }), null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(1).fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> {
            b.startObject(field);
            b.field("lat", "-71.34");
            b.field("lon", 41.12);
            b.endObject();
        }, null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(1).fieldType(), sameInstance(LatLonDocValuesField.TYPE));
        doc = mapper.parse(source("1", b -> b.field(field, new String[] { "41.12,-71.34", "43,-72.34" }), null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(4));
        assertThat(fields.get(0).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(1).fieldType(), sameInstance(LatLonDocValuesField.TYPE));
        assertThat(fields.get(2).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(3).fieldType(), sameInstance(LatLonDocValuesField.TYPE));

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
        }, null, Map.of(field, "points")));
        fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(4));
        assertThat(fields.get(0).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(1).fieldType(), sameInstance(LatLonDocValuesField.TYPE));
        assertThat(fields.get(2).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(3).fieldType(), sameInstance(LatLonDocValuesField.TYPE));

        doc = mapper.parse(source("1", b -> {
            b.startObject("address");
            b.field("home", "43,-72.34");
            b.endObject();
        }, null, Map.of("address.home", "points")));
        fields = doc.rootDoc().getFields("address.home");
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).fieldType(), sameInstance(LatLonPoint.TYPE));
        assertThat(fields.get(1).fieldType(), sameInstance(LatLonDocValuesField.TYPE));
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
        ParsedDocument doc = mapper.parse(source("1", b -> b.field(field, "true"), null, Map.of(field, "booleans")));
        List<IndexableField> fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(1));
        assertThat(fields.get(0).fieldType(), sameInstance(StringField.TYPE_NOT_STORED));
        DocumentParsingException error = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source("1", b -> b.field(field, "hello"), null, Map.of(field, "foo_bar")))
        );
        assertThat(
            error.getMessage(),
            containsString("Can't find dynamic template for dynamic template name [foo_bar] of field [" + field + "]")
        );
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
        DocumentParsingException error = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source("1", b -> b.field(field, "true"), null, Map.of("foo", "booleans")))
        );
        assertThat(error.getMessage(), containsString("failed to parse field [foo] of type [boolean]"));

        ParsedDocument doc = mapper.parse(source("1", b -> b.field(field, "true"), null, Map.of(field, "booleans")));
        List<IndexableField> fields = doc.rootDoc().getFields(field);
        assertThat(fields, hasSize(1));
        assertThat(fields.get(0).fieldType(), sameInstance(StringField.TYPE_NOT_STORED));
    }

    public void testDynamicDottedFieldNameLongArrayWithExistingParent() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("field.bar.baz").value(0).value(1).endArray()));

        assertEquals(2, doc.rootDoc().getFields("field.bar.baz").size());
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
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("field.bar.baz").value(0).value(1).endArray()))
        );
        assertThat(exception.getMessage(), containsString("failed to parse field [field] of type [long]"));
    }

    public void testDynamicFalseDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").size());
    }

    public void testDynamicStrictDottedFieldNameLongArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startArray("foo.bar.baz").value(0).value(1).endArray()))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed")
        );
    }

    public void testDynamicDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(1, doc.rootDoc().getFields("foo.bar.baz").size());
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
        assertEquals(1, doc.rootDoc().getFields("foo.bar.baz").size());
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
        assertEquals(1, doc.rootDoc().getFields("field.bar.baz").size());
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
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("field.bar.baz", 0)))
        );
        assertThat(exception.getMessage(), containsString("failed to parse field [field] of type [long]"));
    }

    public void testDynamicFalseDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo.bar.baz", 0)));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz").size());
    }

    public void testDynamicStrictDottedFieldNameLong() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("foo.bar.baz", 0)))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed")
        );
    }

    public void testDynamicDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(1, doc.rootDoc().getFields("foo.bar.baz.a").size());
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

        assertEquals(1, doc.rootDoc().getFields("foo.bar.baz.a").size());
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
        assertEquals(1, doc.rootDoc().getFields("field.bar.baz.a").size());
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
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("field.bar.baz").field("a", 0).endObject()))
        );
        assertThat(exception.getMessage(), containsString("failed to parse field [field] of type [long]"));
    }

    public void testDynamicFalseDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "false")));
        ParsedDocument doc = mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()));
        assertEquals(0, doc.rootDoc().getFields("foo.bar.baz.a").size());
    }

    public void testDynamicStrictDottedFieldNameObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("dynamic", "strict")));
        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("foo.bar.baz").field("a", 0).endObject()))
        );
        assertThat(
            exception.getMessage(),
            containsString("mapping set to strict, dynamic introduction of [foo] within [_doc] is not allowed")
        );
    }

    public void testDocumentContainsMetadataField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("_field_names", 0)))
        );
        assertTrue(
            e.getCause().getMessage(),
            e.getCause().getMessage().contains("Field [_field_names] is a metadata field and cannot be added inside a document.")
        );

        mapper.parse(source(b -> b.field("foo._field_names", 0))); // parses without error
    }

    public void testDocumentContainsAllowedMetadataField() throws Exception {
        DocumentMapper mapper = createDocumentMapper("""
            { "_doc": { "dynamic" : "strict", "properties" : { "object" : { "type" : "object" } } } }
            """);
        {
            // A metadata field that parses a value fails to parse a null value
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(b -> b.nullField(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE)))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("failed to parse field [_mock_metadata]"));
        }
        {
            // A metadata field that parses a value fails to parse an object
            DocumentParsingException e = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(
                    source(
                        b -> b.field(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE)
                            .startObject()
                            .field("sub-field", "true")
                            .endObject()
                    )
                )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("failed to parse field [_mock_metadata]"));
        }
        {
            ParsedDocument doc = mapper.parse(
                source(b -> b.field(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE, "mock-metadata-field-value"))
            );
            IndexableField field = doc.rootDoc().getField(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE);
            assertEquals("mock-metadata-field-value", field.stringValue());
        }
        {
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
                b.startObject("object");
                b.field(DocumentParserTestsPlugin.MockMetadataMapper.CONTENT_TYPE, "mock-metadata-field-value");
                b.endObject();
            })));
            assertTrue(e.getMessage(), e.getMessage().contains("dynamic introduction of [_mock_metadata] within [object]"));
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
        LuceneDocument doc = builtDocMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(IdFieldMapper.NAME), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(builtDocMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(mapping);

        assertThat((String) docMapper.mapping().getMeta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        LuceneDocument doc = docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(IdFieldMapper.NAME), equalTo(Uid.encodeId("1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").name()), equalTo("shay"));
    }

    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createDocumentMapper(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1-notype-noid.json"));
        LuceneDocument doc = docMapper.parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();
        assertThat(doc.getBinaryValue(IdFieldMapper.NAME), equalTo(Uid.encodeId("1")));
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
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> docMapper.parse(new SourceToParse("1", json, XContentType.JSON))
        );
        assertThat(e.getMessage(), equalTo("[0:0] failed to parse, document is empty"));
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

    private void dynamicTrueOrDynamicRuntimeTest(Consumer<DocumentMapper> mapperServiceConsumer) throws Exception {
        for (XContentBuilder xContentBuilder : new XContentBuilder[] { mapping(b -> {}), topMapping(b -> b.field("dynamic", "runtime")) }) {
            mapperServiceConsumer.accept(createMapperService(xContentBuilder).documentMapper());
        }
    }

    public void testDynamicFieldStartingWithDot() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
                {".foo":1}
                """)));
            // TODO isn't this a misleading error?
            assertThat(e.getCause().getMessage(), containsString("field name cannot contain only whitespace: ['.foo']"));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/28948")
    public void testDynamicFieldEndingWithDot() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception e = expectThrows(MapperParsingException.class, () -> mapper.parse(source("""
                {"foo.":1}
                """)));
            // TODO possibly throw a clearer error?
            assertThat(e.getCause().getMessage(), containsString("field name cannot contain only whitespace: ['foo.']"));
        });
    }

    public void testDynamicDottedFieldWithTrailingDots() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
                {"top..foo":1}
                """)));
            // TODO isn't this a misleading error?
            assertThat(e.getCause().getMessage(), containsString("field name cannot contain only whitespace: ['top..foo']"));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/28948")
    public void testDynamicDottedFieldEndingWithDot() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
                {"top.foo.":1}
                """)));
            // TODO possibly throw a clearer error?
            assertThat(e.getCause().getMessage(), containsString("field name cannot contain only whitespace: ['top.foo.']"));
        });
    }

    public void testDynamicFieldsStartingAndEndingWithDot() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
                {"top..foo.":1}
                """)));
            // TODO isn't this a misleading error?
            assertThat(e.getCause().getMessage(), containsString("field name cannot contain only whitespace: ['top..foo.']"));
        });
    }

    public void testDynamicDottedFieldWithTrailingWhitespace() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
                {"top. .foo":1}
                """)));
            // TODO isn't this a misleading error?
            assertThat(
                e.getCause().getMessage(),
                containsString("field name starting or ending with a [.] makes object resolution ambiguous: [top. .foo]")
            );
        });
    }

    public void testDynamicFieldsEmptyName() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            Exception exception = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
                b.startArray("top");
                {
                    b.startObject();
                    {
                        b.startObject("aoeu").field("a", 1).field(" ", 2).endObject();
                    }
                    b.endObject();
                }
                b.endArray();
            })));

            assertThat(exception.getMessage(), containsString("Field name cannot contain only whitespace: [top.aoeu. ]"));
        });
    }

    public void testBlankFieldNames() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            {
                DocumentParsingException e = expectThrows(
                    DocumentParsingException.class,
                    () -> mapper.parse(source(b -> b.field("", "foo")))
                );
                assertThat(e.getCause(), notNullValue());
                assertThat(e.getCause().getMessage(), containsString("field name cannot be an empty string"));
            }
            {
                DocumentParsingException e = expectThrows(
                    DocumentParsingException.class,
                    () -> mapper.parse(source(b -> b.field(" ", "foo")))
                );
                assertThat(e.getMessage(), containsString("Field name cannot contain only whitespace: [ ]"));
            }
            {
                DocumentParsingException e = expectThrows(
                    DocumentParsingException.class,
                    () -> mapper.parse(source(b -> b.startObject("foo").field("", "bar").endObject()))
                );
                assertThat(e.getCause(), notNullValue());
                assertThat(e.getCause().getMessage(), containsString("field name cannot be an empty string"));
            }
        });
    }

    public void testEmptyFieldNameSubobjectsFalse() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mappingNoSubobjects(b -> {}));
        DocumentParsingException err = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("", "foo"))));
        assertThat(err.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(err.getCause().getMessage(), containsString("Field name cannot be an empty string"));
    }

    public void testBlankFieldNameSubobjectsFalse() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mappingNoSubobjects(b -> {}));
        DocumentParsingException err = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.field("  ", "foo"))));
        assertThat(err.getMessage(), containsString("Field name cannot contain only whitespace: [  ]"));
    }

    public void testDotsOnlyFieldNames() throws Exception {
        dynamicTrueOrDynamicRuntimeTest(mapper -> {
            String[] fieldNames = { ".", "..", "..." };
            for (String fieldName : fieldNames) {
                DocumentParsingException err = expectThrows(
                    DocumentParsingException.class,
                    () -> mapper.parse(source(b -> b.field(fieldName, "bar")))
                );
                assertThat(err.getCause(), notNullValue());
                assertThat(err.getCause().getMessage(), containsString("field name cannot contain only dots"));
            }
        });
    }

    // these combinations are not accepted by default, but they are when subobjects are disabled
    public static final String[] VALID_FIELD_NAMES_NO_SUBOBJECTS = new String[] {
        ".foo",
        "foo.",
        "top..foo",
        "top.foo.",
        "top..foo.",
        "top. .foo",
        ".",
        "..",
        "..." };

    public void testDynamicFieldEdgeCaseNamesSubobjectsFalse() throws Exception {
        MapperService mapperService = createMapperService(mappingNoSubobjects(b -> {}));
        for (String fieldName : VALID_FIELD_NAMES_NO_SUBOBJECTS) {
            ParsedDocument doc = mapperService.documentMapper().parse(source("{\"" + fieldName + "\":1}"));
            merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
            assertNotNull(mapperService.fieldType(fieldName));
        }
    }

    public void testSubobjectsFalseWithInnerObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("metrics.service").field("type", "object").field("subobjects", false).endObject())
        );
        DocumentParsingException err = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
            {
              "metrics": {
                "service": {
                  "time" : {
                    "max" : 10
                  }
                }
              }
            }
            """)));
        assertEquals(
            "[4:16] Tried to add subobject [time] to object [metrics.service] which does not support subobjects",
            err.getMessage()
        );
    }

    public void testSubobjectsFalseWithInnerDottedObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("metrics.service").field("type", "object").field("subobjects", false).endObject())
        );
        DocumentParsingException err = expectThrows(DocumentParsingException.class, () -> mapper.parse(source("""
            {
              "metrics": {
                "service": {
                  "test.with.dots" : {
                    "max" : 10
                  }
                }
              }
            }
            """)));
        assertEquals(
            "[4:26] Tried to add subobject [test.with.dots] to object [metrics.service] which does not support subobjects",
            err.getMessage()
        );
    }

    public void testSubobjectsFalseRootWithInnerObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mappingNoSubobjects(xContentBuilder -> {}));
        ParsedDocument doc = mapper.parse(source("""
            {
              "time" : {
                "measured" : 10,
                "max" : 500,
                "min" : 1
              }
            }
            """));

        Mapping mappingsUpdate = doc.dynamicMappingsUpdate();
        assertNotNull(mappingsUpdate);
        assertNotNull(mappingsUpdate.getRoot().getMapper("time.measured"));
        assertNotNull(mappingsUpdate.getRoot().getMapper("time.min"));
        assertNotNull(mappingsUpdate.getRoot().getMapper("time.max"));

        assertNotNull(doc.rootDoc().getField("time.measured"));
        assertNotNull(doc.rootDoc().getField("time.min"));
        assertNotNull(doc.rootDoc().getField("time.max"));
    }

    public void testSubobjectsFalseRoot() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mappingNoSubobjects(xContentBuilder -> {}));
        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics.service.time" : 10,
              "metrics.service.time.max" : 500,
              "metrics.service.test.with.dots" : "value"
            }
            """));

        Mapping mappingsUpdate = doc.dynamicMappingsUpdate();
        assertNotNull(mappingsUpdate);
        assertNotNull(mappingsUpdate.getRoot().getMapper("metrics.service.time"));
        assertNotNull(mappingsUpdate.getRoot().getMapper("metrics.service.time.max"));
        assertNotNull(mappingsUpdate.getRoot().getMapper("metrics.service.test.with.dots"));

        assertNotNull(doc.rootDoc().getField("metrics.service.time"));
        assertNotNull(doc.rootDoc().getField("metrics.service.time.max"));
        assertNotNull(doc.rootDoc().getField("metrics.service.test.with.dots"));
    }

    public void testSubobjectsFalseStructuredPath() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("metrics.service").field("type", "object").field("subobjects", false).endObject())
        );
        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics": {
                "service": {
                  "time" : 10,
                  "time.max" : 500,
                  "time.min" : 1,
                  "test.with.dots" : "value"
                },
                "object.inner.field": "value"
              }
            }
            """));
        assertNoSubobjects(doc);
    }

    public void testSubobjectsFalseFlatPaths() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("metrics.service").field("type", "object").field("subobjects", false).endObject())
        );
        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics.service.time" : 10,
              "metrics.service.time.max" : 500,
              "metrics.service.time.min" : 1,
              "metrics.service.test.with.dots" : "value",
              "metrics.object.inner.field" : "value"
            }
            """));
        assertNoSubobjects(doc);
    }

    public void testSubobjectsFalseMixedSubobjectPaths() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("metrics.service").field("type", "object").field("subobjects", false).endObject())
        );
        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics": {
                "service.time": 10,
                "service": {
                  "time.max" : 500,
                  "time.min" : 1
                },
                "object.inner.field" : "value"
              },
              "metrics.service.test.with.dots" : "value"
            }
            """));
        assertNoSubobjects(doc);
    }

    public void testSubobjectsFalseArrayOfObjects() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            mapping(b -> b.startObject("metrics.service").field("type", "object").field("subobjects", false).endObject())
        );
        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics": {
                "service": [
                  {
                    "time" : 10,
                    "time.max" : 500,
                    "time.min" : 1,
                    "test.with.dots" : "value1"
                  },
                  {
                    "time" : 5,
                    "time.max" : 600,
                    "time.min" : 3,
                    "test.with.dots" : "value2"
                  }
                ],
                "object.inner.field": "value"
              }
            }
            """));
        assertNoSubobjects(doc);
    }

    private static void assertNoSubobjects(ParsedDocument doc) {
        Mapping mappingsUpdate = doc.dynamicMappingsUpdate();
        assertNotNull(mappingsUpdate);
        Mapper metrics = mappingsUpdate.getRoot().mappers.get("metrics");
        assertThat(metrics, instanceOf(ObjectMapper.class));
        ObjectMapper metricsObject = (ObjectMapper) metrics;
        Mapper service = metricsObject.getMapper("service");
        assertThat(service, instanceOf(ObjectMapper.class));
        ObjectMapper serviceObject = (ObjectMapper) service;
        assertNotNull(serviceObject.getMapper("time"));
        assertNotNull(serviceObject.getMapper("time.max"));
        assertNotNull(serviceObject.getMapper("time.min"));
        assertNotNull(serviceObject.getMapper("test.with.dots"));
        Mapper object = metricsObject.getMapper("object");
        assertThat(object, instanceOf(ObjectMapper.class));
        ObjectMapper objectObject = (ObjectMapper) object;
        Mapper inner = objectObject.getMapper("inner");
        assertThat(inner, instanceOf(ObjectMapper.class));
        ObjectMapper innerObject = (ObjectMapper) inner;
        assertNotNull(innerObject.getMapper("field"));

        assertNotNull(doc.rootDoc().getField("metrics.object.inner.field"));
        assertNotNull(doc.rootDoc().getField("metrics.service.time"));
        assertNotNull(doc.rootDoc().getField("metrics.service.time.max"));
        assertNotNull(doc.rootDoc().getField("metrics.service.time.min"));
        assertNotNull(doc.rootDoc().getField("metrics.service.test.with.dots"));
    }

    public void testSubobjectsFalseParseGeoPoint() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("metrics");
            {
                b.field("type", "object").field("subobjects", false);
                b.startObject("properties");
                {
                    b.startObject("location");
                    b.field("type", "geo_point");
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument parsedDocument = mapper.parse(source("""
            {
              "metrics": {
                "location" : {
                  "lat": 41.12,
                  "lon": -71.34
                }
              }
            }
            """));
        assertNotNull(parsedDocument.rootDoc().getField("metrics.location"));
        assertNull(parsedDocument.dynamicMappingsUpdate());
    }

    public void testSubobjectsFalseParentDynamicFalse() throws Exception {
        // verify that we read the dynamic value properly from the parent mapper. DocumentParser#dynamicOrDefault splits the field
        // name where dots are found, but it does that only for the parent prefix e.g. metrics.service and not for the leaf suffix time.max
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("metrics");
            {
                b.field("dynamic", false);
                b.startObject("properties");
                {
                    b.startObject("service");
                    b.field("type", "object").field("subobjects", false);
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));
        ParsedDocument doc = mapper.parse(source("""
            {
              "metrics": {
                "service": {
                  "time" : 10,
                  "time.max" : 500
                }
              }
            }
            """));

        assertNull(doc.rootDoc().getField("time"));
        assertNull(doc.rootDoc().getField("time.max"));
        assertNull(doc.dynamicMappingsUpdate());
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

        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("alias-field", "value")))
        );

        assertEquals("[1:16] Cannot write to a field alias [alias-field].", exception.getMessage());
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

        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("text-field", "value")))
        );

        assertEquals("[1:15] Cannot copy to a field alias [alias-field].", exception.getMessage());
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

        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.startObject("alias-field.dynamic-field").field("type", "keyword").endObject()))
        );

        assertEquals("[1:2] Cannot write to a field alias [alias-field].", exception.getMessage());
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

        DocumentParsingException exception = expectThrows(
            DocumentParsingException.class,
            () -> mapper.parse(source(b -> b.field("message", "original").field("message.text", "overwrite")))
        );
        assertThat(exception.getMessage(), containsString("failed to parse field [message] of type [keyword]"));
    }

    public void testTypeless() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("foo")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
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

    public void testDynamicFalseMatchesRoutingPath() throws IOException {
        DocumentMapper mapper = createMapperService(
            Settings.builder()
                .put(getIndexSettings())
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim.*")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build(),
            mapping(b -> {
                b.startObject("dim");
                b.field("type", "object");
                b.field("dynamic", false);
                b.endObject();
            })
        ).documentMapper();

        Exception e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startObject("dim");
            b.field("foo", "bar");
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo("[1:15] All fields matching [routing_path] must be mapped but [dim.foo] was declared as [dynamic: false]")
        );

        e = expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> {
            b.startObject("dim");
            b.startObject("foo").field("bar", "baz").endObject();
            b.endObject();
        })));
        assertThat(
            e.getMessage(),
            equalTo("[1:15] All fields matching [routing_path] must be mapped but [dim.foo] was declared as [dynamic: false]")
        );
    }

    public void testDocumentDescriptionInTsdb() throws IOException {
        DocumentMapper mapper = createMapperService(
            Settings.builder()
                .put(getIndexSettings())
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "uid")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                .build(),
            mapping(b -> {
                b.startObject("foo").field("type", "long").endObject();
                b.startObject("uid").field("type", "keyword").field("time_series_dimension", true).endObject();
            })
        ).documentMapper();
        {
            DocumentParsingException exception = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(null, b -> b.field("foo", true), null))
            );
            assertThat(
                exception.getMessage(),
                containsString("failed to parse field [foo] of type [long] in a time series document. Preview of field's value: 'true'")
            );
        }
        {
            DocumentParsingException exception = expectThrows(
                DocumentParsingException.class,
                () -> mapper.parse(source(null, b -> b.field("@timestamp", "2021-04-28T00:01:00Z").field("foo", true), null))
            );
            assertThat(
                exception.getMessage(),
                equalTo(
                    "[1:44] failed to parse field [foo] of type [long] in a time series document at "
                        + "[2021-04-28T00:01:00.000Z]. Preview of field's value: 'true'"
                )
            );
        }
    }

    public void testMergeSubfieldWhileBuildingMappers() throws Exception {
        MapperService mapperService = createMapperService();
        /*
        We had a bug (https://github.com/elastic/elasticsearch/issues/88573) building an object mapper (ObjectMapper.Builder#buildMappers).
        A sub-field that already exists is merged with the existing one. As a result, the leaf field would get the wrong field path
        (missing the first portion of its path). The only way to trigger this scenario for dynamic mappings is to either allow duplicate
        JSON keys or ingest the same field with dots collapsed as well as expanded within the same document. Note that the two fields with
        same name need to be part  of the same mappings (hence the same document). If they are in two distinct mappings they are properly
        merged as part of RootObjectMapper#merge.
         */
        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "foo" : {
                "bar" : {
                  "baz" : 1
                }
              },
              "foo.bar.baz" : 2
            }
            """));
        Mapping mapping = doc.dynamicMappingsUpdate();
        assertNotNull(mapping);
        Mapper fooMapper = mapping.getRoot().getMapper("foo");
        assertNotNull(fooMapper);
        assertTrue(fooMapper instanceof ObjectMapper);
        Mapper barMapper = ((ObjectMapper) fooMapper).getMapper("bar");
        assertTrue(barMapper instanceof ObjectMapper);
        Mapper baz = ((ObjectMapper) barMapper).getMapper("baz");
        assertNotNull(baz);
        assertEquals("foo.bar.baz", baz.name());
        assertEquals("baz", baz.simpleName());
        List<IndexableField> fields = doc.rootDoc().getFields("foo.bar.baz");
        assertEquals(2, fields.size());
        String[] fieldStrings = fields.stream().map(Object::toString).toArray(String[]::new);
        assertArrayEquals(new String[] { "LongField <foo.bar.baz:1>", "LongField <foo.bar.baz:2>" }, fieldStrings);

        // merge without going through toXContent and reparsing, otherwise the potential leaf path issue gets fixed on its own
        Mapping newMapping = MapperService.mergeMappings(mapperService.documentMapper(), mapping, MapperService.MergeReason.MAPPING_UPDATE);
        DocumentMapper newDocMapper = new DocumentMapper(mapperService.documentParser(), newMapping, newMapping.toCompressedXContent());
        ParsedDocument doc2 = newDocMapper.parse(source("""
            {
              "foo" : {
                "bar" : {
                  "baz" : 10
                }
              }
            }
            """));
        assertNull(doc2.dynamicMappingsUpdate());
        List<IndexableField> fields2 = doc2.rootDoc().getFields("foo.bar.baz");
        assertEquals(1, fields2.size());
        assertEquals("LongField <foo.bar.baz:10>", fields2.get(0).toString());
    }

    public void testDeeplyNestedDocument() throws Exception {
        int depth = 10000;

        DocumentMapper docMapper = createMapperService(Settings.builder().put(getIndexSettings()).build(), mapping(b -> {}))
            .documentMapper();
        // hits the mapping object depth limit (defaults to 20)
        DocumentParsingException mpe = expectThrows(DocumentParsingException.class, () -> docMapper.parse(source(b -> {
            for (int i = 0; i < depth; i++) {
                b.startObject("obj");
            }
            b.field("foo", 10);
            for (int i = 0; i < depth; i++) {
                b.endObject();
            }
        })));
        assertThat(mpe.getCause().getMessage(), containsString("Limit of mapping depth [20] has been exceeded due to object field"));

        // check that multiple-dotted field name underneath an object mapper with subobjects=false does not trigger this
        DocumentMapper docMapper2 = createMapperService(mappingNoSubobjects(xContentBuilder -> {})).documentMapper();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("obj.");
        }
        sb.append("foo");
        docMapper2.parse(source(b -> { b.field(sb.toString(), 10); }));
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

            @Override
            public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
                return new StringStoredFieldFieldLoader(name(), simpleName(), null) {
                    @Override
                    protected void write(XContentBuilder b, Object value) throws IOException {
                        BytesRef ref = (BytesRef) value;
                        b.utf8Value(ref.bytes, ref.offset, ref.length);
                    }
                };
            }

            private static final TypeParser PARSER = new FixedTypeParser(c -> new MockMetadataMapper());
        }

        @Override
        public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
            return Collections.singletonMap(MockMetadataMapper.CONTENT_TYPE, MockMetadataMapper.PARSER);
        }

        @Override
        public Map<String, RuntimeField.Parser> getRuntimeFields() {
            return Collections.singletonMap("test-composite", new RuntimeField.Parser(n -> new RuntimeField.Builder(n) {
                @Override
                protected RuntimeField createRuntimeField(MappingParserContext parserContext) {
                    return new TestRuntimeField(
                        n,
                        List.of(
                            new TestRuntimeField.TestRuntimeFieldType(n + ".foo", KeywordFieldMapper.CONTENT_TYPE),
                            new TestRuntimeField.TestRuntimeFieldType(n + ".bar", KeywordFieldMapper.CONTENT_TYPE)
                        )
                    );
                }

                @Override
                protected RuntimeField createChildRuntimeField(
                    MappingParserContext parserContext,
                    String parentName,
                    Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory,
                    OnScriptError onScriptError
                ) {
                    throw new UnsupportedOperationException();
                }
            }));
        }
    }
}
