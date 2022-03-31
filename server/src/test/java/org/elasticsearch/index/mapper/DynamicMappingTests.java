/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.arrayWithSize;
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
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping("true", b -> b.startObject("field1").field("type", "text").endObject())
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        }));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), equalTo("value2"));

        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "true",
                "properties": {
                  "field2": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntime() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping("runtime", b -> b.startObject("field1").field("type", "text").endObject())
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        }));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertNull(doc.rootDoc().get("field2"));

        assertEquals("""
            {"_doc":{"dynamic":"runtime","runtime":{"field2":{"type":"keyword"}}}}""", Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicFalse() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping("false", b -> b.startObject("field1").field("type", "text").endObject())
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        }));

        assertThat(doc.rootDoc().get("field1"), equalTo("value1"));
        assertThat(doc.rootDoc().get("field2"), nullValue());

        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testDynamicStrict() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping("strict", b -> b.startObject("field1").field("type", "text").endObject())
        );

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse(source(b -> {
            b.field("field1", "value1");
            b.field("field2", "value2");
        })));
        assertThat(e.getMessage(), equalTo("mapping set to strict, dynamic introduction of [field2] within [_doc] is not allowed"));

        e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse(source(b -> {
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

        StrictDynamicMappingException e = expectThrows(StrictDynamicMappingException.class, () -> defaultMapper.parse(source(b -> {
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

    public void testDynamicField() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "bar")));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "properties": {
                  "foo": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));

    }

    public void testDynamicFieldOnIncorrectDate() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.field("foo", "2020-01-01T01-01-01Z")));
        assertNotNull(doc.dynamicMappingsUpdate());
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "properties": {
                  "foo": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicUpdateWithRuntimeField() throws Exception {
        MapperService mapperService = createMapperService(runtimeFieldMapping(b -> b.field("type", "keyword")));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("test", "value")));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "properties": {
                  "test": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate().getRoot()));
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        Mapping merged = mapperService.documentMapper().mapping();
        assertNotNull(merged.getRoot().getMapper("test"));
        assertEquals(1, merged.getRoot().runtimeFields().size());
        assertNotNull(merged.getRoot().getRuntimeField("field"));
    }

    public void testDynamicUpdateWithRuntimeFieldDottedName() throws Exception {
        MapperService mapperService = createMapperService(
            runtimeMapping(b -> b.startObject("path1.path2.path3.field").field("type", "keyword").endObject())
        );
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("path1").startObject("path2").startObject("path3");
            b.field("field", "value");
            b.endObject().endObject().endObject();
        }));
        RootObjectMapper root = doc.dynamicMappingsUpdate().getRoot();
        assertEquals(0, root.runtimeFields().size());
        {
            // the runtime field is defined but the object structure is not, hence it is defined under properties
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
        MapperService mapperService = createMapperService(mapping(b -> {}));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
            b.startObject("foo");
            {
                b.startObject("bar").field("baz", "foo").endObject();
            }
            b.endObject();
        }));

        assertNotNull(doc.dynamicMappingsUpdate());
        merge(mapperService, dynamicMapping(doc.dynamicMappingsUpdate()));
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), containsString("""
            {"foo":{"properties":{"bar":{"properties":{"baz":{"type":"text\""""));
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

        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "foo.bar.baz": {
                    "type": "long"
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeMappingDynamicObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            dynamicMapping("runtime", b -> b.startObject("dynamic_object").field("type", "object").field("dynamic", true).endObject())
        );
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

        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "object.foo.bar.baz": {
                    "type": "long"
                  }
                },
                "properties": {
                  "dynamic_object": {
                    "dynamic": "true",
                    "properties": {
                      "foo": {
                        "properties": {
                          "bar": {
                            "properties": {
                              "baz": {
                                "type": "long"
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicMappingDynamicRuntimeObject() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            dynamicMapping("true", b -> b.startObject("runtime_object").field("type", "object").field("dynamic", "runtime").endObject())
        );
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

        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "true",
                "runtime": {
                  "runtime_object.foo.bar.baz": {
                    "type": "keyword"
                  }
                },
                "properties": {
                  "object": {
                    "properties": {
                      "foo": {
                        "properties": {
                          "bar": {
                            "properties": {
                              "baz": {
                                "type": "text",
                                "fields": {
                                  "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testArray() throws Exception {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {}));
        ParsedDocument doc = mapper.parse(source(b -> b.startArray("foo").value("bar").value("baz").endArray()));

        assertNotNull(doc.dynamicMappingsUpdate());
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), containsString("""
            {"foo":{"type":"text\""""));
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
        assertThat(Strings.toString(doc.dynamicMappingsUpdate()), containsString("""
            {"field":{"properties":{"bar":{"properties":{"baz":{"type":"text\""""));
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
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "properties": {
                  "foo": {
                    "properties": {
                      "bar": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "baz": {
                        "type": "long"
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
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

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> newMapper.parse(source(b -> b.field("my_field2", "foobar")))
        );
        assertThat(e.getMessage(), containsString("failed to parse field [my_field2] of type [integer]"));
    }

    public void testDefaultFloatingPointMappings() throws IOException {
        DocumentMapper mapper = createDocumentMapper(topMapping(b -> b.field("numeric_detection", true)));
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.jsonBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.yamlBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.smileBuilder());
        doTestDefaultFloatingPointMappings(mapper, XContentFactory.cborBuilder());
    }

    private void doTestDefaultFloatingPointMappings(DocumentMapper mapper, XContentBuilder builder) throws IOException {
        BytesReference source = BytesReference.bytes(
            builder.startObject()
                .field("foo", 3.2f) // float
                .field("bar", 3.2d) // double
                .field("baz", (double) 3.2f) // double that can be accurately represented as a float
                .field("quux", "3.2") // float detected through numeric detection
                .endObject()
        );
        ParsedDocument parsedDocument = mapper.parse(new SourceToParse("id", source, builder.contentType()));
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

    public void testDynamicRuntimeLeafFields() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(dynamicMapping("runtime", builder -> {}));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.field("long", 123);
            b.field("double", 123.456);
            b.field("string", "text");
            b.field("boolean", true);
            b.field("date", "2020-12-15");
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "boolean": {
                    "type": "boolean"
                  },
                  "date": {
                    "type": "date"
                  },
                  "double": {
                    "type": "double"
                  },
                  "long": {
                    "type": "long"
                  },
                  "string": {
                    "type": "keyword"
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeWithDynamicDateFormats() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", ObjectMapper.Dynamic.RUNTIME);
            b.array("dynamic_date_formats", "dd/MM/yyyy", "dd-MM-yyyy");
        }));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.field("date1", "15/12/2020");
            b.field("date2", "15-12-2020");
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "date1": {
                    "type": "date",
                    "format": "dd/MM/yyyy"
                  },
                  "date2": {
                    "type": "date",
                    "format": "dd-MM-yyyy"
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeWithinObjects() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(topMapping(b -> {
            b.field("dynamic", false);
            b.startObject("properties");
            b.startObject("dynamic_true").field("type", "object").field("dynamic", true).endObject();
            b.startObject("dynamic_runtime").field("type", "object").field("dynamic", ObjectMapper.Dynamic.RUNTIME).endObject();
            b.endObject();
        }));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.startObject("anything").field("field", "text").endObject();
            b.startObject("dynamic_true").field("field1", "text").startObject("child").field("field2", "text").endObject().endObject();
            b.startObject("dynamic_runtime").field("field3", "text").startObject("child").field("field4", "text").endObject().endObject();
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "false",
                "runtime": {
                  "dynamic_runtime.child.field4": {
                    "type": "keyword"
                  },
                  "dynamic_runtime.field3": {
                    "type": "keyword"
                  }
                },
                "properties": {
                  "dynamic_true": {
                    "dynamic": "true",
                    "properties": {
                      "child": {
                        "properties": {
                          "field2": {
                            "type": "text",
                            "fields": {
                              "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                              }
                            }
                          }
                        }
                      },
                      "field1": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      }
                    }
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    public void testDynamicRuntimeDotsInFieldNames() throws IOException {
        DocumentMapper documentMapper = createDocumentMapper(dynamicMapping("runtime", builder -> {}));
        ParsedDocument doc = documentMapper.parse(source(b -> {
            b.field("one.two.three.four", "1234");
            b.field("one.two.three", 123);
            b.array("one.two", 1.2, 1.2, 1.2);
            b.field("one", "one");
        }));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic": "runtime",
                "runtime": {
                  "one": {
                    "type": "keyword"
                  },
                  "one.two": {
                    "type": "double"
                  },
                  "one.two.three": {
                    "type": "long"
                  },
                  "one.two.three.four": {
                    "type": "keyword"
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }

    // test for https://github.com/elastic/elasticsearch/issues/65333
    public void testDottedFieldDynamicFalse() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping("false", b -> b.startObject("myfield").field("type", "keyword").endObject())
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.field("myfield", "value1");
            b.array("something.myfield", "value2", "value3");
        }));

        assertThat(doc.rootDoc().getFields("myfield"), arrayWithSize(2));
        for (IndexableField field : doc.rootDoc().getFields("myfield")) {
            assertThat(field.binaryValue(), equalTo(new BytesRef("value1")));
        }
        // dynamic is false, so `something.myfield` should be ignored entirely. It used to be merged with myfield by mistake.
        assertThat(doc.rootDoc().getFields("something.myfield"), arrayWithSize(0));

        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testArraysDynamicFalse() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping("false", b -> b.startObject("myarray").field("type", "keyword").endObject())
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.array("unmapped", "unknown1", "unknown2");
            b.array("myarray", "array1", "array2");
        }));

        assertThat(doc.rootDoc().getFields("myarray"), arrayWithSize(4));
        assertThat(doc.rootDoc().getFields("unmapped"), arrayWithSize(0));
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testArraysOfObjectsRootDynamicFalse() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping(
                "false",
                b -> b.startObject("objects")
                    .startObject("properties")
                    .startObject("subfield")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startArray("objects");
            b.startObject().field("subfield", "sub").field("unmapped", "unmapped").endObject();
            b.endArray();
            b.startArray("unmapped");
            b.startObject().field("subfield", "unmapped").endObject();
            b.endArray();
        }));

        assertThat(doc.rootDoc().getFields("objects.subfield"), arrayWithSize(2));
        assertThat(doc.rootDoc().getFields("objects.unmapped"), arrayWithSize(0));
        assertThat(doc.rootDoc().getFields("unmapped.subfield"), arrayWithSize(0));
        assertNull(doc.dynamicMappingsUpdate());
    }

    public void testArraysOfObjectsDynamicFalse() throws IOException {
        DocumentMapper defaultMapper = createDocumentMapper(
            dynamicMapping(
                "true",
                b -> b.startObject("objects")
                    .field("dynamic", false)
                    .startObject("properties")
                    .startObject("subfield")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        ParsedDocument doc = defaultMapper.parse(source(b -> {
            b.startArray("objects");
            b.startObject().field("subfield", "sub").field("unmapped", "unmapped").endObject();
            b.endArray();
            b.field("myfield", 2);
        }));

        assertThat(doc.rootDoc().getFields("myfield"), arrayWithSize(2));
        assertThat(doc.rootDoc().getFields("objects.subfield"), arrayWithSize(2));
        assertThat(doc.rootDoc().getFields("objects.unmapped"), arrayWithSize(0));
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "_doc": {
                "dynamic":"true",
                "properties":{
                  "myfield":{
                    "type":"long"
                  }
                }
              }
            }"""), Strings.toString(doc.dynamicMappingsUpdate()));
    }
}
