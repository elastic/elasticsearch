/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class DynamicMappingTests extends MapperServiceTestCase {

    private XContentBuilder dynamicMapping(String dynamicValue, CheckedConsumer<XContentBuilder, IOException> buildFields)
        throws IOException {
        return topMapping(b -> {
            b.field("dynamic", dynamicValue);
            b.startObject("properties");
            buildFields.accept(b);
            b.endObject();
        });
    }

    public void testDynamicTrue() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(dynamicMapping("true",
            b -> b.startObject("field1").field("type", "text").endObject()));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        }));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));

        assertEquals("{\"_doc\":{\"dynamic\":\"true\",\"" +
                "properties\":{\"field2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntime() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(dynamicMapping("runtime",
            b -> b.startObject("field1").field("type", "text").endObject()));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        }));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertNull(doc.rootDoc().get("field2"));

        assertEquals("{\"_doc\":{\"dynamic\":\"runtime\"," +
                "\"runtime\":{\"field2\":{\"type\":\"keyword\"}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicFalse() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(dynamicMapping("false",
            b -> b.startObject("field1").field("type", "text").endObject()));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        }));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());

        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testDynamicStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(dynamicMapping("strict",
            b -> b.startObject("field1").field("type", "text").endObject()));

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class,
            () -> defaultMapper.parse(source(b -> {
                b.field("field1", "value1");
                b.field("field2", "value2");
            })));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [_doc] is not allowed"));

        e = expectThrows(StrictDynamicMappingException.class,
            () -> defaultMapper.parse(source(b -> {
                b.field("field1", "value1");
                b.nullField("field2");
            })));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [_doc] is not allowed"));
    }

    public void testDynamicFalseWithInnerObjectButDynamicSetOnRoot() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(dynamicMapping("false", b -> {
            b.startObject("obj1");
            {
                b.startObject("properties");
                {
                    b.startObject("field1").field("type", "text").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startObject("obj1");
            {
                b.field("field1", "value1");
                b.field("field2", "value2");
            }
            b.endObject();
        }));

        assertThat(doc.rootDoc().get("obj1.field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("obj1.field2"), nullValue());
    }

    public void testDynamicStrictWithInnerObjectButDynamicSetOnRoot() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(dynamicMapping("strict", b -> {
            b.startObject("obj1");
            {
                b.startObject("properties");
                {
                    b.startObject("field1").field("type", "text").endObject();
                }
                b.endObject();
            }
            b.endObject();
        }));

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () ->
            defaultMapper.parse(source(b -> {
                b.startObject("obj1");
                {
                    b.field("field1", "value1");
                    b.field("field2", "value2");
                }
                b.endObject();
            })));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [obj1] is not allowed"));
    }

    public void testDynamicMappingOnEmptyString() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("empty_field", "")));
        assertNotNull(doc.rootDoc().getField("empty_field"));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        MappedFieldType fieldType = mapperService.fieldType("empty_field");
        assertNotNull(fieldType);
    }

    public void testDynamicRuntimeMappingOnEmptyString() throws Exception {
        MapperService mapperService = createMapperService(dynamicMapping("runtime", b -> {}));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("empty_field", "")));
        assertNull(doc.rootDoc().getField("empty_field"));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        MappedFieldType fieldType = mapperService.fieldType("empty_field");
        assertNotNull(fieldType);
    }

    public void testDynamicMappingsNotNeeded() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));
        ParsedDocument doc = mapper.parse(source(b -> b.field("field", "bar")));
        // field is already defined in mappings
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "bar")));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertEquals(
            "{\"_doc\":{\"properties\":{\"foo\":{\"type\":\"text\",\"fields\":" +
                "{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));

    }

    public void testDynamicFieldOnIncorrectDate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "2020-01-01T01-01-01Z")));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertEquals(
            "{\"_doc\":{\"properties\":{\"foo\":{\"type\":\"text\",\"fields\":" +
                "{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicUpdateWithRuntimeField() throws Exception {
        MapperService mapperService = createMapperService(runtimeFieldMapping(b -> b.field("type", "keyword")));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("test", "value")));
        assertEquals("{\"_doc\":{\"properties\":{" +
            "\"test\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate().getRoot()));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        Mapping merged = mapperService.documentMapper().mapping();
        assertNotNull(merged.getRoot().getMapper("test"));
        assertEquals(1, merged.getRoot().runtimeFields().size());
        assertNotNull(merged.getRoot().getRuntimeField("field"));
    }

    public void testDynamicUpdateWithRuntimeFieldDottedName() throws Exception {
        MapperService mapperService = createMapperService(runtimeMapping(
            b -> b.startObject("path1.path2.path3.field").field("type", "keyword").endObject()));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("path1").startObject("path2").startObject("path3");
            b.field("field", "value");
            b.endObject().endObject().endObject();
        }));
        RootObjectMapper root = doc.dynamicMappingsUpdate().getRoot();
        assertEquals(0, root.runtimeFields().size());
        {
            //the runtime field is defined but the object structure is not, hence it is defined under properties
            Mapper path1 = root.getMapper("path1");
            assertThat(path1, instanceOf(ObjectMapper.class));
            Mapper path2 = ((ObjectMapper) path1).getMapper("path2");
            assertThat(path2, instanceOf(ObjectMapper.class));
            Mapper path3 = ((ObjectMapper) path2).getMapper("path3");
            assertThat(path3, instanceOf(ObjectMapper.class));
            assertFalse(path3.iterator().hasNext());
        }
        assertNull(doc.rootDoc().getField("path1.path2.path3.field"));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        Mapping merged = mapperService.documentMapper().mapping();
        {
            Mapper path1 = merged.getRoot().getMapper("path1");
            assertThat(path1, instanceOf(ObjectMapper.class));
            Mapper path2 = ((ObjectMapper) path1).getMapper("path2");
            assertThat(path2, instanceOf(ObjectMapper.class));
            Mapper path3 = ((ObjectMapper) path2).getMapper("path3");
            assertThat(path3, instanceOf(ObjectMapper.class));
            assertFalse(path3.iterator().hasNext());
        }
        assertEquals(1, merged.getRoot().runtimeFields().size());
        assertNotNull(merged.getRoot().getRuntimeField("path1.path2.path3.field"));
    }

    public void testIncremental() throws Exception {
        // Make sure that mapping updates are incremental, this is important for performance otherwise
        // every new field introduction runs in linear time with the total number of fields
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "text")));

        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("field", "bar");
            b.field("bar", "baz");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());

        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), containsString("{\"bar\":"));
        // field is NOT in the update
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), not(containsString("{\"field\":")));
    }

    public void testIntroduceTwoFields() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.field("foo", "bar");
            b.field("bar", "baz");
        }));

        assertNotNull(doc.dynamicMappingsUpdate());
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), containsString("\"foo\":{"));
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), containsString("\"bar\":{"));
    }

    public void testObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", "foo").endObject();
            }
            b.endObject();
        }));

        assertNotNull(doc.dynamicMappingsUpdate());
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()),
            containsString("{\"foo\":{\"properties\":{\"bar\":{\"properties\":{\"baz\":{\"type\":\"text\""));
    }

    public void testDynamicRuntimeFieldWithinObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(dynamicMapping("runtime", b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", 1).endObject();
            }
            b.endObject();
        }));

        assertEquals("{\"_doc\":{\"dynamic\":\"runtime\"," +
            "\"runtime\":{\"foo.bar.baz\":{\"type\":\"long\"}}," +
            "\"properties\":{\"foo\":{\"properties\":{\"bar\":{\"type\":\"object\"}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeMappingDynamicObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(dynamicMapping("runtime",
            b -> b.startObject("dynamic_object").field("type", "object").field("dynamic", true).endObject()));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("dynamic_object");
            {
                b.startObject("foo").startObject("bar").field("baz", 1).endObject().endObject();
            }
            b.endObject();
            b.startObject("object");
            {
                b.startObject("foo").startObject("bar").field("baz", 1).endObject().endObject();
            }
            b.endObject();
        }));

        assertEquals("{\"_doc\":{\"dynamic\":\"runtime\"," +
                "\"runtime\":{\"object.foo.bar.baz\":{\"type\":\"long\"}}," +
                "\"properties\":{\"dynamic_object\":{\"dynamic\":\"true\"," +
                "\"properties\":{\"foo\":{" + "\"properties\":{\"bar\":{" + "\"properties\":{\"baz\":" + "{\"type\":\"long\"}}}}}}}," +
                "\"object\":{\"properties\":{\"foo\":{\"properties\":{\"bar\":{\"type\":\"object\"}}}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicMappingDynamicRuntimeObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(dynamicMapping("true",
            b -> b.startObject("runtime_object").field("type", "object").field("dynamic", "runtime").endObject()));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("runtime_object");
            {
                b.startObject("foo").startObject("bar").field("baz", "text").endObject().endObject();
            }
            b.endObject();
            b.startObject("object");
            {
                b.startObject("foo").startObject("bar").field("baz", "text").endObject().endObject();
            }
            b.endObject();
        }));

        assertEquals("{\"_doc\":{\"dynamic\":\"true\",\"" +
                "runtime\":{\"runtime_object.foo.bar.baz\":{\"type\":\"keyword\"}}," +
                "\"properties\":{\"object\":{\"properties\":{\"foo\":{\"properties\":{\"bar\":{\"properties\":{" +
                "\"baz\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}}}}," +
                "\"runtime_object\":{\"dynamic\":\"runtime\",\"properties\":{\"foo\":{\"properties\":{\"bar\":{\"type\":\"object\"}}}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value("bar").value("baz").endArray()));

        assertNotNull(doc.dynamicMappingsUpdate());
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()),
            containsString("{\"foo\":{\"type\":\"text\""));
    }

    public void testInnerDynamicMapping() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "object")));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startObject("field");
            {
                b.startObject("bar").field("baz", "foo").endObject();
            }
            b.endObject();
        }));

        assertNotNull(doc.dynamicMappingsUpdate());
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()),
            containsString("{\"field\":{\"properties\":{\"bar\":{\"properties\":{\"baz\":{\"type\":\"text\""));
    }

    public void testComplexArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> {
            b.startArray("foo");
            {
                b.startObject().field("bar", "baz").endObject();
                b.startObject().field("baz", 3).endObject();
            }
            b.endArray();
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertEquals("{\"_doc\":{\"properties\":{\"foo\":{\"properties\":{\"bar\":{\"type\":\"text\",\"fields\":{" +
            "\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"baz\":{\"type\":\"long\"}}}}}}",
            Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testReuseExistingMappings() throws Exception {
        // Even if the dynamic type of our new field is long, we already have a mapping for the same field
        // of type string so it should be mapped as a string
        DocumentMapper newMapper = createDocumentMapper(mapping(b -> {
            b.startObject("my_field1").field("type", "text").field("store", "true").endObject();
            b.startObject("my_field2").field("type", "integer").field("store", "false").endObject();
            b.startObject("my_field3").field("type", "long").field("doc_values", "false").endObject();
            b.startObject("my_field4").field("type", "float").field("index", "false").endObject();
            b.startObject("my_field5").field("type", "double").field("store", "true").endObject();
            b.startObject("my_field6").field("type", "date").field("doc_values", "false").endObject();
            b.startObject("my_field7").field("type", "boolean").field("doc_values", "false").endObject();
        }));

        ParsedDocument doc = newMapper.parse(source(b -> {
            b.field("my_field1", 42);
            b.field("my_field2", 43);
            b.field("my_field3", 44);
            b.field("my_field4", 45);
            b.field("my_field5", 46);
            b.field("my_field6", Instant.now().toEpochMilli());
            b.field("my_field7", true);
        }));
        assertNull(doc.dynamicMappingsUpdate());

        MapperParsingException e = expectThrows(MapperParsingException.class,
            () -> newMapper.parse(source(b -> b.field("my_field2", "foobar"))));
        assertThat(e.getMessage(), containsString("failed to parse field [my_field2] of type [integer]"));
    }

    public void testMixTemplateMultiFieldAndMappingReuse() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("template1");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping");
                        {
                            b.field("type", "text");
                            b.startObject("fields");
                            {
                                b.startObject("raw").field("type", "keyword").endObject();
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
        assertNull(mapperService.documentMapper().mappers().getMapper("field.raw"));

        ParsedDocument parsed = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNotNull(parsed.dynamicMappingsUpdate());

        merge(mapperService, dynamicMapping(parsed.dynamicMappingsUpdate()));
        assertNotNull(mapperService.documentMapper().mappers().getMapper("field.raw"));
        parsed = mapperService.documentMapper().parse(source(b -> b.field("field", "foo")));
        assertNull(parsed.dynamicMappingsUpdate());
    }

    public void testDefaultFloatingPointMappings() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("numeric_detection", true)));
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.jsonBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.yamlBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.smileBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.cborBuilder());
    }

    private void doTestDefaultFloatingPointMappings(DocumentMapper mapper, XContentBuilder builder) throws IOException {
        BytesReference source = BytesReference.bytes(builder.startObject()
                .field("foo", 3.2f) // float
                .field("bar", 3.2d) // double
                .field("baz", (double) 3.2f) // double that can be accurately represented as a float
                .field("quux", "3.2") // float detected through numeric detection
                .endObject());
        ParsedDocument parsedDocument = mapper.parse(new SourceToParse("index", "id",
            source, builder.contentType()));
        Mapping update = parsedDocument.dynamicMappingsUpdate();
        assertNotNull(update);
        assertThat(((FieldMapper) update.getRoot().getMapper("foo")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.getRoot().getMapper("bar")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.getRoot().getMapper("baz")).fieldType().typeName(), equalTo("float"));
        assertThat(((FieldMapper) update.getRoot().getMapper("quux")).fieldType().typeName(), equalTo("float"));
    }

    public void testNumericDetectionEnabled() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("numeric_detection", true)));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("s_long", "100");
            b.field("s_double", "100.0");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        Mapper mapper = mapperService.documentMapper().mappers().getMapper("s_long");
        assertThat(mapper.typeName(), equalTo("long"));

        mapper = mapperService.documentMapper().mappers().getMapper("s_double");
        assertThat(mapper.typeName(), equalTo("float"));
    }

    public void testNumericDetectionEnabledDynamicRuntime() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("numeric_detection", true).field("dynamic", "runtime")));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("s_long", "100");
            b.field("s_double", "100.0");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        assertThat(mapperService.fieldType("s_long").typeName(), equalTo("long"));
        assertThat(mapperService.fieldType("s_double").typeName(), equalTo("double"));
    }

    public void testNumericDetectionDefault() throws Exception {
        MapperService mapperService = createMapperService(mapping(b -> {}));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("s_long", "100");
            b.field("s_double", "100.0");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        Mapper mapper = mapperService.documentMapper().mappers().getMapper("s_long");
        assertThat(mapper, instanceOf(TextFieldMapper.class));

        mapper = mapperService.documentMapper().mappers().getMapper("s_double");
        assertThat(mapper, instanceOf(TextFieldMapper.class));
    }

    public void testNumericDetectionDefaultDynamicRuntime() throws Exception {
        MapperService mapperService = createMapperService(dynamicMapping("runtime", b -> {}));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("s_long", "100");
            b.field("s_double", "100.0");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        assertThat(mapperService.fieldType("s_long").typeName(), equalTo("keyword"));
        assertThat(mapperService.fieldType("s_double").typeName(), equalTo("keyword"));
    }

    public void testDateDetectionInheritsFormat() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_date_formats").value("yyyy-MM-dd").endArray();
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("dates");
                    {
                        b.field("match_mapping_type", "date");
                        b.field("match", "*2");
                        b.startObject("mapping").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("dates");
                    {
                        b.field("match_mapping_type", "date");
                        b.field("match", "*3");
                        b.startObject("mapping").field("format", "yyyy-MM-dd||epoch_millis").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));

        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.field("date1", "2016-11-20");
            b.field("date2", "2016-11-20");
            b.field("date3", "2016-11-20");
        }));
        assertNotNull(doc.dynamicMappingsUpdate());

        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));

        DateFieldMapper dateMapper1 = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("date1");
        DateFieldMapper dateMapper2 = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("date2");
        DateFieldMapper dateMapper3 = (DateFieldMapper) mapperService.documentMapper().mappers().getMapper("date3");
        // inherited from dynamic date format
        assertEquals("yyyy-MM-dd", dateMapper1.fieldType().dateTimeFormatter().pattern());
        // inherited from dynamic date format since the mapping in the template did not specify a format
        assertEquals("yyyy-MM-dd", dateMapper2.fieldType().dateTimeFormatter().pattern());
        // not inherited from the dynamic date format since the template defined an explicit format
        assertEquals("yyyy-MM-dd||epoch_millis", dateMapper3.fieldType().dateTimeFormatter().pattern());
    }

    public void testDynamicTemplateOrder() throws IOException {
        // https://github.com/elastic/elasticsearch/issues/18625
        // elasticsearch used to apply templates that do not have a match_mapping_type first
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("type-based");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("mapping").field("type", "keyword").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("path-based");
                    {
                        b.field("path_match", "foo");
                        b.startObject("mapping").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("foo", "abc")));
        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        assertThat(mapperService.fieldType("foo"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
    }

    public void testDynamicTemplateRuntimeMatchMappingType() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match_mapping_type", "string");
                        b.startObject("runtime").field("type", "long").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("s", "hello");
            b.field("l", 1);
        }));
        assertEquals("{\"_doc\":{\"runtime\":{\"s\":{\"type\":\"long\"}},\"properties\":{\"l\":{\"type\":\"long\"}}}}",
            Strings.toString(parsedDoc.dynamicMappingsUpdate()));
    }

    public void testDynamicTemplateRuntimeMatch() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("match", "field*");
                        b.startObject("runtime").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("field_string", "hello");
            b.field("field_long", 1);
            b.field("field_boolean", true);
            b.field("concrete_string", "text");
            b.startObject("field_object");
            b.field("field_date", "2020-12-15");
            b.field("concrete_date", "2020-12-15");
            b.endObject();
            b.startArray("field_array");
            b.startObject();
            b.field("field_double", 1.25);
            b.field("concrete_double", 1.25);
            b.endObject();
            b.endArray();
        }));
        assertEquals("{\"_doc\":{\"runtime\":{" +
                "\"field_array.field_double\":{\"type\":\"double\"}," +
                "\"field_boolean\":{\"type\":\"boolean\"}," +
                "\"field_long\":{\"type\":\"long\"}," +
                "\"field_object.field_date\":{\"type\":\"date\"}," +
                "\"field_string\":{\"type\":\"keyword\"}}," +
                "\"properties\":" +
                "{\"concrete_string\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}," +
                "\"field_array\":{\"properties\":{\"concrete_double\":{\"type\":\"float\"}}}," +
                "\"field_object\":{\"properties\":{\"concrete_date\":{\"type\":\"date\"}}}}}}",
            Strings.toString(parsedDoc.dynamicMappingsUpdate()));
    }

    public void testDynamicTemplateRuntimePathMatch() throws Exception {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("test");
                    {
                        b.field("path_match", "object.*");
                        b.field("path_unmatch", "*.concrete*");
                        b.startObject("runtime").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("double", 1.23);
            b.startObject("object");
            {
                b.field("date", "2020-12-15");
                b.field("long", 1);
                b.startObject("object").field("string", "hello").field("concrete", false).endObject();
            }
            b.endObject();
            b.startObject("concrete").field("boolean", true).endObject();
        }));
        assertEquals("{\"_doc\":{\"runtime\":{" +
                "\"object.date\":{\"type\":\"date\"}," +
                "\"object.long\":{\"type\":\"long\"}," +
                "\"object.object.string\":{\"type\":\"keyword\"}}," +
                "\"properties\":" + "{" +
                "\"concrete\":{\"properties\":{\"boolean\":{\"type\":\"boolean\"}}}," +
                "\"double\":{\"type\":\"float\"}," +
                "\"object\":{\"properties\":{\"object\":{\"properties\":{\"concrete\":{\"type\":\"boolean\"}}}}}}}}",
            Strings.toString(parsedDoc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeWithDynamicTemplate() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> {
            b.field("dynamic", "runtime");
            b.startArray("dynamic_templates");
            {
                b.startObject();
                {
                    b.startObject("concrete");
                    {
                        b.field("match", "concrete*");
                        b.startObject("mapping").endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endArray();
        }));
        DocumentMapper docMapper = mapperService.documentMapper();
        ParsedDocument parsedDoc = docMapper.parse(source(b -> {
            b.field("double", 1.23);
            b.field("concrete_double", 1.23);
        }));
        assertEquals("{\"_doc\":{\"dynamic\":\"runtime\"," +
                "\"runtime\":{" + "\"double\":{\"type\":\"double\"}}," +
                "\"properties\":{\"concrete_double\":{\"type\":\"float\"}}}}",
            Strings.toString(parsedDoc.dynamicMappingsUpdate()));
    }
}
