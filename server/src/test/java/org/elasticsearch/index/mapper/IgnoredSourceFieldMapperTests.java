/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.FieldMaskingReader;
import org.elasticsearch.xcontent.XContentBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class IgnoredSourceFieldMapperTests extends MapperServiceTestCase {

    private DocumentMapper getDocumentMapperWithFieldLimit() throws IOException {
        return createMapperService(
            Settings.builder()
                .put("index.mapping.total_fields.limit", 2)
                .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
                .build(),
            syntheticSourceMapping(b -> {
                b.startObject("foo").field("type", "keyword").endObject();
                b.startObject("bar").field("type", "object").endObject();
            })
        ).documentMapper();
    }

    private ParsedDocument getParsedDocumentWithFieldLimit(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        DocumentMapper mapper = getDocumentMapperWithFieldLimit();
        return mapper.parse(source(build));
    }

    private String getSyntheticSourceWithFieldLimit(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        DocumentMapper documentMapper = getDocumentMapperWithFieldLimit();
        return syntheticSource(documentMapper, build);
    }

    private MapperService createMapperServiceWithStoredArraySource(XContentBuilder mappings) throws IOException {
        Settings settings = Settings.builder()
            .put(getIndexSettings())
            .put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), "arrays")
            .build();
        return createMapperService(settings, mappings);
    }

    public void testIgnoredBoolean() throws IOException {
        boolean value = randomBoolean();
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredString() throws IOException {
        String value = randomAlphaOfLength(5);
        assertEquals("{\"my_value\":\"" + value + "\"}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredInt() throws IOException {
        int value = randomInt();
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredLong() throws IOException {
        long value = randomLong();
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredFloat() throws IOException {
        float value = randomFloat();
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredDouble() throws IOException {
        double value = randomDouble();
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredBigInteger() throws IOException {
        BigInteger value = randomBigInteger();
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testIgnoredBytes() throws IOException {
        byte[] value = randomByteArrayOfLength(10);
        assertEquals(
            "{\"my_value\":\"" + Base64.getEncoder().encodeToString(value) + "\"}",
            getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value))
        );
    }

    public void testIgnoredObjectBoolean() throws IOException {
        boolean value = randomBoolean();
        assertEquals("{\"my_object\":{\"my_value\":" + value + "}}", getSyntheticSourceWithFieldLimit(b -> {
            b.startObject("my_object").field("my_value", value).endObject();
        }));
    }

    public void testIgnoredArray() throws IOException {
        assertEquals("{\"my_array\":[{\"int_value\":10},{\"int_value\":20}]}", getSyntheticSourceWithFieldLimit(b -> {
            b.startArray("my_array");
            b.startObject().field("int_value", 10).endObject();
            b.startObject().field("int_value", 20).endObject();
            b.endArray();
        }));
    }

    public void testEncodeFieldToMap() throws IOException {
        String value = randomAlphaOfLength(5);
        ParsedDocument parsedDocument = getParsedDocumentWithFieldLimit(b -> b.field("my_value", value));
        byte[] bytes = parsedDocument.rootDoc().getField(IgnoredSourceFieldMapper.NAME).binaryValue().bytes;
        IgnoredSourceFieldMapper.MappedNameValue mappedNameValue = IgnoredSourceFieldMapper.decodeAsMap(bytes);
        assertEquals("my_value", mappedNameValue.nameValue().name());
        assertEquals(value, mappedNameValue.map().get("my_value"));
    }

    @SuppressWarnings("unchecked")
    public void testEncodeObjectToMapAndDecode() throws IOException {
        String value = randomAlphaOfLength(5);
        ParsedDocument parsedDocument = getParsedDocumentWithFieldLimit(
            b -> { b.startObject("my_object").field("my_value", value).endObject(); }
        );
        byte[] bytes = parsedDocument.rootDoc().getField(IgnoredSourceFieldMapper.NAME).binaryValue().bytes;
        IgnoredSourceFieldMapper.MappedNameValue mappedNameValue = IgnoredSourceFieldMapper.decodeAsMap(bytes);
        assertEquals("my_object", mappedNameValue.nameValue().name());
        assertEquals(value, ((Map<String, ?>) mappedNameValue.map().get("my_object")).get("my_value"));
        assertArrayEquals(bytes, IgnoredSourceFieldMapper.encodeFromMap(mappedNameValue, mappedNameValue.map()));
    }

    public void testEncodeArrayToMapAndDecode() throws IOException {
        ParsedDocument parsedDocument = getParsedDocumentWithFieldLimit(b -> {
            b.startArray("my_array");
            b.startObject().field("int_value", 10).endObject();
            b.startObject().field("int_value", 20).endObject();
            b.endArray();
        });
        byte[] bytes = parsedDocument.rootDoc().getField(IgnoredSourceFieldMapper.NAME).binaryValue().bytes;
        IgnoredSourceFieldMapper.MappedNameValue mappedNameValue = IgnoredSourceFieldMapper.decodeAsMap(bytes);
        assertEquals("my_array", mappedNameValue.nameValue().name());
        assertThat((List<?>) mappedNameValue.map().get("my_array"), Matchers.contains(Map.of("int_value", 10), Map.of("int_value", 20)));
        assertArrayEquals(bytes, IgnoredSourceFieldMapper.encodeFromMap(mappedNameValue, mappedNameValue.map()));
    }

    public void testMultipleIgnoredFieldsRootObject() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);
        String syntheticSource = getSyntheticSourceWithFieldLimit(b -> {
            b.field("boolean_value", booleanValue);
            b.field("int_value", intValue);
            b.field("string_value", stringValue);
        });
        assertEquals(String.format(Locale.ROOT, """
            {"boolean_value":%s,"int_value":%s,"string_value":"%s"}""", booleanValue, intValue, stringValue), syntheticSource);
    }

    public void testMultipleIgnoredFieldsSameObject() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);
        String syntheticSource = getSyntheticSourceWithFieldLimit(b -> {
            b.startObject("bar");
            {
                b.field("boolean_value", booleanValue);
                b.field("string_value", stringValue);
                b.field("int_value", intValue);
            }
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"bar":{"boolean_value":%s,"int_value":%s,"string_value":"%s"}}""", booleanValue, intValue, stringValue), syntheticSource);
    }

    public void testMultipleIgnoredFieldsManyObjects() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);
        String syntheticSource = getSyntheticSourceWithFieldLimit(b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.startObject("to");
                {
                    b.field("int_value", intValue);
                    b.startObject("some");
                    {
                        b.startObject("deeply");
                        {
                            b.startObject("nested");
                            b.field("string_value", stringValue);
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        });
        assertEquals(
            String.format(
                Locale.ROOT,
                """
                    {"boolean_value":%s,"path":{"to":{"int_value":%s,"some":{"deeply":{"nested":{"string_value":"%s"}}}}}}""",
                booleanValue,
                intValue,
                stringValue
            ),
            syntheticSource
        );
    }

    public void testIgnoredDynamicArrayNestedInObject() throws IOException {
        int intValue = randomInt();

        String syntheticSource = getSyntheticSourceWithFieldLimit(b -> {
            b.startObject("bar");
            b.field("a", List.of(intValue, intValue));
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"bar":{"a":[%s,%s]}}""", intValue, intValue), syntheticSource);
    }

    public void testDisabledRootObjectSingleField() throws IOException {
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject();
            b.field("enabled", false);
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> { b.field("name", name); });
        assertEquals(String.format(Locale.ROOT, """
            {"name":"%s"}""", name), syntheticSource);
    }

    public void testDisabledRootObjectManyFields() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);

        DocumentMapper documentMapper = createMapperService(topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject();
            b.field("enabled", false);
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.startObject("to");
                {
                    b.field("int_value", intValue);
                    b.startObject("some");
                    {
                        b.startObject("deeply");
                        {
                            b.startObject("nested");
                            b.field("string_value", stringValue);
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        });
        assertEquals(
            String.format(
                Locale.ROOT,
                """
                    {"boolean_value":%s,"path":{"to":{"int_value":%s,"some":{"deeply":{"nested":{"string_value":"%s"}}}}}}""",
                booleanValue,
                intValue,
                stringValue
            ),
            syntheticSource
        );
    }

    public void testDisabledObjectSingleField() throws IOException {
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("enabled", false).endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.field("name", name);
            }
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":{"name":"%s"}}""", name), syntheticSource);
    }

    public void testDisabledObjectContainsArray() throws IOException {
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("enabled", false).endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().field("foo", "A").field("bar", "B").endObject();
                b.startObject().field("foo", "C").field("bar", "D").endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":[{"foo":"A","bar":"B"},{"foo":"C","bar":"D"}]}""", syntheticSource);
    }

    public void testDisabledObjectManyFields() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);

        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path").field("type", "object").field("enabled", false).endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startObject("to");
                {
                    b.startObject("some");
                    {
                        b.startObject("deeply");
                        {
                            b.startObject("nested");
                            b.field("string_value", stringValue);
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        });
        assertEquals(
            String.format(
                Locale.ROOT,
                """
                    {"boolean_value":%s,"path":{"int_value":%s,"to":{"some":{"deeply":{"nested":{"string_value":"%s"}}}}}}""",
                booleanValue,
                intValue,
                stringValue
            ),
            syntheticSource
        );
    }

    public void testDisabledSubObject() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                    b.startObject("to").field("type", "object").field("enabled", false).endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startObject("to");
                {
                    b.field("name", name);
                }
                b.endObject();
            }
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"boolean_value":%s,"path":{"int_value":%s,"to":{"name":"%s"}}}""", booleanValue, intValue, name), syntheticSource);
    }

    public void testDisabledSubobjectContainsArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                    b.startObject("to").field("type", "object").field("enabled", false).endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startArray("to");
                {
                    b.startObject().field("foo", "A").field("bar", "B").endObject();
                    b.startObject().field("foo", "C").field("bar", "D").endObject();
                }
                b.endArray();
            }
            b.endObject();
        });
        assertEquals(
            String.format(Locale.ROOT, """
                {"boolean_value":%s,"path":{"int_value":%s,"to":[{"foo":"A","bar":"B"},{"foo":"C","bar":"D"}]}}""", booleanValue, intValue),
            syntheticSource
        );
    }

    public void testMixedDisabledEnabledObjects() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String foo = randomAlphaOfLength(20);
        String bar = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                    b.startObject("to").field("type", "object");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("foo").field("type", "object").field("enabled", false).endObject();
                            b.startObject("bar").field("type", "object");
                            {
                                b.startObject("properties");
                                {
                                    b.startObject("name").field("type", "keyword").endObject();
                                }
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startObject("to");
                {
                    b.startObject("foo").field("name", foo).endObject();
                    b.startObject("bar").field("name", bar).endObject();
                }
                b.endObject();
            }
            b.endObject();
        });
        assertEquals(
            String.format(
                Locale.ROOT,
                """
                    {"boolean_value":%s,"path":{"int_value":%s,"to":{"bar":{"name":"%s"},"foo":{"name":"%s"}}}}""",
                booleanValue,
                intValue,
                bar,
                foo
            ),
            syntheticSource
        );
    }

    public void testIndexStoredArraySourceRootValueArray() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("int_value").field("type", "integer").endObject();
            b.startObject("bool_value").field("type", "boolean").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.array("int_value", new int[] { 30, 20, 10 });
            b.field("bool_value", true);
        });
        assertEquals("""
            {"bool_value":true,"int_value":[30,20,10]}""", syntheticSource);
    }

    public void testIndexStoredArraySourceRootValueArrayDisabled() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("int_value").field("type", "integer").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "none").endObject();
            b.startObject("bool_value").field("type", "boolean").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.array("int_value", new int[] { 30, 20, 10 });
            b.field("bool_value", true);
        });
        assertEquals("""
            {"bool_value":true,"int_value":[10,20,30]}""", syntheticSource);
    }

    public void testIndexStoredArraySourceSingleLeafElement() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("int_value").field("type", "integer").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> b.array("int_value", new int[] { 10 }));
        assertEquals("{\"int_value\":10}", syntheticSource);
        ParsedDocument doc = documentMapper.parse(source(syntheticSource));
        assertNull(doc.rootDoc().getField("_ignored_source"));
    }

    public void testIndexStoredArraySourceSingleLeafElementAndNull() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("value").field("type", "keyword").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> b.array("value", new String[] { "foo", null }));
        assertEquals("{\"value\":[\"foo\",null]}", syntheticSource);
    }

    public void testIndexStoredArraySourceSingleObjectElement() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties");
            {
                b.startObject("int_value").field("type", "integer").endObject();
            }
            b.endObject().endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path").startObject().field("int_value", 10).endObject().endArray();
        });
        assertEquals("{\"path\":[{\"int_value\":10}]}", syntheticSource);
    }

    public void testFieldStoredArraySourceRootValueArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("int_value").field("type", "integer").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "arrays").endObject();
            b.startObject("string_value").field("type", "keyword").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "all").endObject();
            b.startObject("bool_value").field("type", "boolean").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.array("int_value", new int[] { 30, 20, 10 });
            b.array("string_value", "C", "B", "A");
            b.field("bool_value", true);
        });
        assertEquals("""
            {"bool_value":true,"int_value":[30,20,10],"string_value":["C","B","A"]}""", syntheticSource);
    }

    public void testFieldStoredSourceRootValue() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("default").field("type", "float").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "none").endObject();
            b.startObject("source_kept").field("type", "float").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "all").endObject();
            b.startObject("bool_value").field("type", "boolean").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("default", 10);
            b.field("source_kept", 10);
            b.field("bool_value", true);
        });
        assertEquals("""
            {"bool_value":true,"default":10.0,"source_kept":10}""", syntheticSource);
    }

    public void testIndexStoredArraySourceRootObjectArray() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("bool_value").field("type", "boolean").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            b.startObject().field("int_value", 20).endObject();
            b.startObject().field("int_value", 10).endObject();
            b.endArray();
            b.field("bool_value", true);
        });
        assertEquals("""
            {"bool_value":true,"path":[{"int_value":20},{"int_value":10}]}""", syntheticSource);
    }

    public void testIndexStoredArraySourceRootObjectArrayWithBypass() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.field("synthetic_source_keep", "none");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("bool_value").field("type", "boolean").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            b.startObject().field("int_value", 20).endObject();
            b.startObject().field("int_value", 10).endObject();
            b.endArray();
            b.field("bool_value", true);
        });
        assertEquals("""
            {"bool_value":true,"path":{"int_value":[10,20]}}""", syntheticSource);
    }

    public void testIndexStoredArraySourceNestedValueArray() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                    b.startObject("bool_value").field("type", "boolean").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.array("int_value", new int[] { 30, 20, 10 });
                b.field("bool_value", true);
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"bool_value":true,"int_value":[30,20,10]}}""", syntheticSource);
    }

    public void testIndexStoredArraySourceNestedValueArrayDisabled() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "none").endObject();
                    b.startObject("bool_value").field("type", "boolean").endObject();
                    b.startObject("obj").field("type", "object").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "none");
                    b.startObject("properties");
                    {
                        b.startObject("foo").field("type", "integer").endObject();
                    }
                    b.endObject().endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.array("int_value", new int[] { 30, 20, 10 });
                b.field("bool_value", true);
                b.startArray("obj");
                {
                    b.startObject().field("foo", 2).endObject();
                    b.startObject().field("foo", 1).endObject();
                }
                b.endArray();
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"bool_value":true,"int_value":[10,20,30],"obj":{"foo":[1,2]}}}""", syntheticSource);
    }

    public void testFieldStoredArraySourceNestedValueArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "arrays").endObject();
                    b.startObject("string_value").field("type", "keyword").field(Mapper.SYNTHETIC_SOURCE_KEEP_PARAM, "all").endObject();
                    b.startObject("bool_value").field("type", "boolean").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.array("int_value", new int[] { 30, 20, 10 });
                b.array("string_value", "C", "B", "A");
                b.field("bool_value", true);
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"bool_value":true,"int_value":[30,20,10],"string_value":["C","B","A"]}}""", syntheticSource);
    }

    public void testFieldStoredSourceNestedValue() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("default").field("type", "float").field("synthetic_source_keep", "none").endObject();
                    b.startObject("source_kept").field("type", "float").field("synthetic_source_keep", "all").endObject();
                    b.startObject("bool_value").field("type", "boolean").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.field("default", 10);
                b.field("source_kept", 10);
                b.field("bool_value", true);
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"bool_value":true,"default":10.0,"source_kept":10}}""", syntheticSource);
    }

    public void testIndexStoredArraySourceNestedObjectArray() throws IOException {
        DocumentMapper documentMapper = createMapperServiceWithStoredArraySource(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("to");
                    {
                        b.field("type", "object");
                        b.startObject("properties");
                        {
                            b.startObject("int_value").field("type", "integer").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                    b.startObject("bool_value").field("type", "boolean").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startArray("to");
                b.startObject().field("int_value", 10).endObject();
                b.startObject().field("int_value", 20).endObject();
                b.endArray();
                b.field("bool_value", true);
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"bool_value":true,"to":[{"int_value":10},{"int_value":20}]}}""", syntheticSource);
    }

    public void testRootArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.field("synthetic_source_keep", "arrays");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            b.startObject().field("int_value", 10).endObject();
            b.startObject().field("int_value", 20).endObject();
            b.endArray();
        });
        assertEquals("""
            {"path":[{"int_value":10},{"int_value":20}]}""", syntheticSource);
    }

    public void testNestedArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("to").field("type", "object").field("synthetic_source_keep", "arrays");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("some").field("type", "object");
                            {
                                b.startObject("properties");
                                {
                                    b.startObject("name").field("type", "keyword").endObject();
                                }
                                b.endObject();
                            }
                            b.endObject();
                            b.startObject("another").field("type", "object");
                            {
                                b.startObject("properties");
                                {
                                    b.startObject("name").field("type", "keyword").endObject();
                                }
                                b.endObject();
                            }
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.startArray("to");
                {
                    b.startObject();
                    {
                        b.startObject("some").field("name", "A").endObject();
                    }
                    b.endObject();
                    b.startObject();
                    {
                        b.startObject("some").field("name", "B").endObject();
                    }
                    b.endObject();
                    b.startObject();
                    {
                        b.startObject("another").field("name", "C").endObject();
                    }
                    b.endObject();
                }
                b.endArray();
            }
            b.endObject();
        });
        assertEquals(
            String.format(Locale.ROOT, """
                {"boolean_value":%s,"path":{"to":[{"some":{"name":"A"}},{"some":{"name":"B"}},{"another":{"name":"C"}}]}}""", booleanValue),
            syntheticSource
        );
    }

    public void testConflictingFieldNameAfterArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties");
            {
                b.startObject("to").startObject("properties");
                {
                    b.startObject("id").field("type", "integer").field("synthetic_source_keep", "arrays").endObject();
                }
                b.endObject().endObject();
                b.startObject("id").field("type", "float").endObject();
            }
            b.endObject().endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startArray("to");
                {
                    b.startObject().array("id", 1, 20, 3).endObject();
                    b.startObject().field("id", 10).endObject();
                }
                b.endArray();
                b.field("id", "0.1");
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"id":0.1,"to":{"id":[1,20,3,10]}}}""", syntheticSource);
    }

    public void testArrayWithNestedObjects() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties");
            {
                b.startObject("to").field("type", "nested").startObject("properties");
                {
                    b.startObject("id").field("type", "integer").field("synthetic_source_keep", "arrays").endObject();
                }
                b.endObject().endObject();
            }
            b.endObject().endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().startArray("to");
                {
                    b.startObject().array("id", 1, 20, 3).endObject();
                    b.startObject().field("id", 10).endObject();
                }
                b.endArray().endObject();
                b.startObject().startObject("to").field("id", "0.1").endObject().endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"to":[{"id":[1,20,3]},{"id":10},{"id":0}]}}""", syntheticSource);
    }

    public void testObjectArrayWithinNestedObjects() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties");
            {
                b.startObject("to").field("type", "nested").startObject("properties");
                {
                    b.startObject("obj").startObject("properties");
                    {
                        b.startObject("id").field("type", "integer").field("synthetic_source_keep", "arrays").endObject();
                    }
                    b.endObject().endObject();
                }
                b.endObject().endObject();
            }
            b.endObject().endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startObject("to");
                {
                    b.startArray("obj");
                    {
                        b.startObject().array("id", 1, 20, 3).endObject();
                        b.startObject().field("id", 10).endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"to":{"obj":[{"id":[1,20,3]},{"id":10}]}}}""", syntheticSource);
    }

    public void testObjectArrayWithinNestedObjectsArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties");
            {
                b.startObject("to").field("type", "nested").startObject("properties");
                {
                    b.startObject("obj").startObject("properties");
                    {
                        b.startObject("id").field("type", "integer").field("synthetic_source_keep", "arrays").endObject();
                    }
                    b.endObject().endObject();
                }
                b.endObject().endObject();
            }
            b.endObject().endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startArray("to");
                {
                    b.startObject();
                    {
                        b.startArray("obj");
                        {
                            b.startObject().array("id", 1, 20, 3).endObject();
                            b.startObject().field("id", 10).endObject();
                        }
                        b.endArray();
                    }
                    b.endObject();
                    b.startObject();
                    {
                        b.startArray("obj");
                        {
                            b.startObject().array("id", 200, 300, 500).endObject();
                            b.startObject().field("id", 100).endObject();
                        }
                        b.endArray();
                    }
                    b.endObject();
                }
                b.endArray();
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"to":[{"obj":[{"id":[1,20,3]},{"id":10}]},{"obj":[{"id":[200,300,500]},{"id":100}]}]}}""", syntheticSource);
    }

    public void testArrayWithinArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object").field("synthetic_source_keep", "arrays");
                b.startObject("properties");
                {
                    b.startObject("to").field("type", "object").field("synthetic_source_keep", "arrays");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("name").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject();
                {
                    b.startArray("to");
                    {
                        b.startObject().field("name", "A").endObject();
                        b.startObject().field("name", "B").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.startArray("to");
                    {
                        b.startObject().field("name", "C").endObject();
                        b.startObject().field("name", "D").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":[{"to":[{"name":"A"},{"name":"B"}]},{"to":[{"name":"C"},{"name":"D"}]}]}""", booleanValue), syntheticSource);
    }

    public void testObjectArrayAndValue() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("stored");
                    {
                        b.field("type", "object").field("synthetic_source_keep", "arrays");
                        b.startObject("properties").startObject("leaf").field("type", "integer").endObject().endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject();
                {
                    b.startArray("stored");
                    {
                        b.startObject().field("leaf", 10).endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("stored").field("leaf", 20).endObject();
                }
                b.endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"stored":[{"leaf":10},{"leaf":20}]}}""", syntheticSource);
    }

    public void testDeeplyNestedObjectArrayAndValue() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties").startObject("to").startObject("properties");
            {
                b.startObject("stored");
                {
                    b.field("type", "object").field("store_array_source", true);
                    b.startObject("properties").startObject("leaf").field("type", "integer").endObject().endObject();
                }
                b.endObject();
            }
            b.endObject().endObject().endObject().endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject();
                {
                    b.startObject("to").startArray("stored");
                    {
                        b.startObject().field("leaf", 10).endObject();
                    }
                    b.endArray().endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("to").startObject("stored").field("leaf", 20).endObject().endObject();
                }
                b.endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"to":{"stored":[{"leaf":10},{"leaf":20}]}}}""", syntheticSource);
    }

    public void testObjectArrayAndValueInNestedObject() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").startObject("properties").startObject("to").startObject("properties");
            {
                b.startObject("stored");
                {
                    b.field("type", "nested").field("dynamic", false);
                }
                b.endObject();
            }
            b.endObject().endObject().endObject().endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject();
                {
                    b.startObject("to").startArray("stored");
                    {
                        b.startObject().field("leaf", 10).endObject();
                    }
                    b.endArray().endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("to").startObject("stored").field("leaf", 20).endObject().endObject();
                }
                b.endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"to":{"stored":[{"leaf":10},{"leaf":20}]}}}""", syntheticSource);
    }

    public void testObjectArrayAndValueDisabledObject() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").startObject("properties");
            {
                b.startObject("regular");
                {
                    b.startObject("properties").startObject("leaf").field("type", "integer").endObject().endObject();
                }
                b.endObject();
                b.startObject("disabled").field("type", "object").field("enabled", false);
                {
                    b.startObject("properties").startObject("leaf").field("type", "integer").endObject().endObject();
                }
                b.endObject();
            }
            b.endObject().endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().startArray("disabled").startObject().field("leaf", 10).endObject().endArray().endObject();
                b.startObject().startObject("disabled").field("leaf", 20).endObject().endObject();
                b.startObject().startArray("regular").startObject().field("leaf", 10).endObject().endArray().endObject();
                b.startObject().startObject("regular").field("leaf", 20).endObject().endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"disabled":[{"leaf":10},{"leaf":20}],"regular":{"leaf":[10,20]}}}""", syntheticSource);
    }

    public void testObjectArrayAndValueNonDynamicObject() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").startObject("properties");
            {
                b.startObject("regular");
                {
                    b.startObject("properties").startObject("leaf").field("type", "integer").endObject().endObject();
                }
                b.endObject();
                b.startObject("disabled").field("type", "object").field("dynamic", "false").endObject();
            }
            b.endObject().endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().startArray("disabled").startObject().field("leaf", 10).endObject().endArray().endObject();
                b.startObject().startObject("disabled").field("leaf", 20).endObject().endObject();
                b.startObject().startArray("regular").startObject().field("leaf", 10).endObject().endArray().endObject();
                b.startObject().startObject("regular").field("leaf", 20).endObject().endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"disabled":{"leaf":[10,20]},"regular":{"leaf":[10,20]}}}""", syntheticSource);
    }

    public void testObjectArrayAndValueDynamicRuntimeObject() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").startObject("properties");
            {
                b.startObject("regular");
                {
                    b.startObject("properties").startObject("leaf").field("type", "integer").endObject().endObject();
                }
                b.endObject();
                b.startObject("runtime").field("type", "object").field("dynamic", "runtime").endObject();
            }
            b.endObject().endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().startArray("runtime").startObject().field("leaf", 10).endObject().endArray().endObject();
                b.startObject().startObject("runtime").field("leaf", 20).endObject().endObject();
                b.startObject().startArray("regular").startObject().field("leaf", 10).endObject().endArray().endObject();
                b.startObject().startObject("regular").field("leaf", 20).endObject().endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"regular":{"leaf":[10,20]},"runtime":{"leaf":[10,20]}}}""", syntheticSource);
    }

    public void testDisabledObjectWithinHigherLevelArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("to").field("type", "object").field("enabled", false);
                    {
                        b.startObject("properties");
                        {
                            b.startObject("name").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject();
                {
                    b.startObject("to").field("name", "A").endObject();
                }
                b.endObject();
                b.startObject();
                {
                    b.startObject("to").field("name", "B").endObject();
                }
                b.endObject();
            }
            b.endArray();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":{"to":[{"name":"A"},{"name":"B"}]}}""", booleanValue), syntheticSource);
    }

    public void testStoredArrayWithinHigherLevelArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("to").field("type", "object").field("synthetic_source_keep", "arrays");
                    {
                        b.startObject("properties");
                        {
                            b.startObject("name").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject();
                {
                    b.startArray("to");
                    {
                        b.startObject().field("name", "A").endObject();
                        b.startObject().field("name", "B").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
                b.startObject();
                {
                    b.startArray("to");
                    {
                        b.startObject().field("name", "C").endObject();
                        b.startObject().field("name", "D").endObject();
                    }
                    b.endArray();
                }
                b.endObject();
            }
            b.endArray();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":{"to":[{"name":"A"},{"name":"B"},{"name":"C"},{"name":"D"}]}}""", booleanValue), syntheticSource);
    }

    public void testObjectWithKeepAll() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object").field("synthetic_source_keep", "all");
                b.startObject("properties");
                {
                    b.startObject("a").field("type", "object").endObject();
                    b.startObject("b").field("type", "integer").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("id").field("type", "integer").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startArray("a");
                {
                    b.startObject().field("foo", 30).endObject();
                    b.startObject().field("foo", 20).endObject();
                    b.startObject().field("foo", 10).endObject();
                    b.startObject().field("bar", 20).endObject();
                    b.startObject().field("bar", 10).endObject();
                }
                b.endArray();
                b.array("b", 4, 1, 3, 2);
            }
            b.endObject();
            b.field("id", 10);
        });
        assertEquals("""
            {"id":10,"path":{"a":[{"foo":30},{"foo":20},{"foo":10},{"bar":20},{"bar":10}],"b":[4,1,3,2]}}""", syntheticSource);
    }

    public void testFallbackFieldWithinHigherLevelArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("name").field("type", "keyword").field("doc_values", false).endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {

                b.startObject().field("name", "A").endObject();
                b.startObject().field("name", "B").endObject();
                b.startObject().field("name", "C").endObject();
                b.startObject().field("name", "D").endObject();
            }
            b.endArray();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":{"name":["A","B","C","D"]}}""", booleanValue), syntheticSource);
    }

    public void testFieldOrdering() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("A").field("type", "integer").endObject();
            b.startObject("B").field("type", "object").field("synthetic_source_keep", "arrays");
            {
                b.startObject("properties");
                {
                    b.startObject("X").field("type", "keyword").endObject();
                    b.startObject("Y").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("C").field("type", "integer").endObject();
            b.startObject("D").field("type", "object").field("synthetic_source_keep", "arrays");
            {
                b.startObject("properties");
                {
                    b.startObject("X").field("type", "keyword").endObject();
                    b.startObject("Y").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
            b.startObject("E").field("type", "integer").endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("C", 10);
            b.startArray("D");
            {
                b.startObject().field("Y", 100).endObject();
                b.startObject().field("X", 200).endObject();
            }
            b.endArray();
            b.field("E", 20);
            b.startArray("B");
            {
                b.startObject().field("Y", 300).endObject();
                b.startObject().field("X", 400).endObject();
            }
            b.endArray();
            b.field("A", 30);
        });
        assertEquals("""
            {"A":30,"B":[{"Y":300},{"X":400}],"C":10,"D":[{"Y":100},{"X":200}],"E":20}""", syntheticSource);
    }

    public void testNestedObjectWithField() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "nested");
            {
                b.field("synthetic_source_keep", "all");
                b.startObject("properties");
                {
                    b.startObject("foo").field("type", "keyword").endObject();
                    b.startObject("bar").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(
            documentMapper,
            b -> { b.startObject("path").field("foo", "A").field("bar", "B").endObject(); }
        );
        assertEquals("""
            {"path":{"foo":"A","bar":"B"}}""", syntheticSource);
    }

    public void testNestedObjectWithArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "nested");
            {
                b.field("synthetic_source_keep", "all");
                b.startObject("properties");
                {
                    b.startObject("foo").field("type", "keyword").endObject();
                    b.startObject("bar").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().field("foo", "A").field("bar", "B").endObject();
                b.startObject().field("foo", "C").field("bar", "D").endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":[{"foo":"A","bar":"B"},{"foo":"C","bar":"D"}]}""", syntheticSource);
    }

    public void testNestedSubobjectWithField() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                    b.startObject("to").field("type", "nested");
                    {
                        b.field("synthetic_source_keep", "all");
                        b.startObject("properties");
                        {
                            b.startObject("foo").field("type", "keyword").endObject();
                            b.startObject("bar").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startObject("to").field("foo", "A").field("bar", "B").endObject();
            }
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"boolean_value":%s,"path":{"int_value":%s,"to":{"foo":"A","bar":"B"}}}""", booleanValue, intValue), syntheticSource);
    }

    public void testNestedSubobjectWithArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path");
            {
                b.field("type", "object");
                b.startObject("properties");
                {
                    b.startObject("int_value").field("type", "integer").endObject();
                    b.startObject("to").field("type", "nested");
                    {
                        b.field("synthetic_source_keep", "all");
                        b.startObject("properties");
                        {
                            b.startObject("foo").field("type", "keyword").endObject();
                            b.startObject("bar").field("type", "keyword").endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startArray("to");
                {
                    b.startObject().field("foo", "A").field("bar", "B").endObject();
                    b.startObject().field("foo", "C").field("bar", "D").endObject();
                }
                b.endArray();
            }
            b.endObject();
        });
        assertEquals(
            String.format(Locale.ROOT, """
                {"boolean_value":%s,"path":{"int_value":%s,"to":[{"foo":"A","bar":"B"},{"foo":"C","bar":"D"}]}}""", booleanValue, intValue),
            syntheticSource
        );
    }

    public void testNestedObjectIncludeInRoot() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "nested").field("synthetic_source_keep", "all").field("include_in_root", true);
            {
                b.startObject("properties");
                {
                    b.startObject("foo").field("type", "keyword").endObject();
                    b.startObject("bar").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(
            documentMapper,
            b -> { b.startObject("path").field("foo", "A").field("bar", "B").endObject(); }
        );
        assertEquals("""
            {"path":{"foo":"A","bar":"B"}}""", syntheticSource);
    }

    public void testNoDynamicObjectSingleField() throws IOException {
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "false").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.field("name", name);
            }
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":{"name":"%s"}}""", name), syntheticSource);
    }

    public void testNoDynamicObjectManyFields() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);

        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path").field("type", "object").field("dynamic", "false");
            {
                b.startObject("properties");
                {
                    b.startObject("string_value").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startObject("to");
                {
                    b.startObject("some");
                    {
                        b.startObject("deeply");
                        {
                            b.startObject("nested");
                            b.field("string_value", stringValue);
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.field("string_value", stringValue);
                b.endObject();
            }
            b.endObject();
        });

        assertEquals(String.format(Locale.ROOT, """
            {"boolean_value":%s,"path":{"int_value":%s,"to":{"some":{"deeply":{"nested":{"string_value":"%s"}}},\
            "string_value":"%s"}}}""", booleanValue, intValue, stringValue, stringValue), syntheticSource);
    }

    public void testNoDynamicObjectSimpleArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "false").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().field("name", "foo").endObject();
                b.startObject().field("name", "bar").endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"name":["foo","bar"]}}""", syntheticSource);
    }

    public void testNoDynamicObjectSimpleValueArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "false").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(
            documentMapper,
            b -> { b.startObject("path").array("name", "A", "B", "C", "D").endObject(); }
        );
        assertEquals("""
            {"path":{"name":["A","B","C","D"]}}""", syntheticSource);
    }

    public void testNoDynamicObjectNestedArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "false").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().startObject("to").field("foo", "A").field("bar", "B").endObject().endObject();
                b.startObject().startObject("to").field("foo", "C").field("bar", "D").endObject().endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"to":[{"foo":"A","bar":"B"},{"foo":"C","bar":"D"}]}}""", syntheticSource);
    }

    public void testNoDynamicRootObject() throws IOException {
        DocumentMapper documentMapper = createMapperService(topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject().field("dynamic", "false");
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("foo", "bar");
            b.startObject("path").field("X", "Y").endObject();
            b.array("name", "A", "D", "C", "B");
        });
        assertEquals("""
            {"foo":"bar","name":["A","D","C","B"],"path":{"X":"Y"}}""", syntheticSource);
    }

    public void testRuntimeDynamicObjectSingleField() throws IOException {
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "runtime").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.field("name", name);
            }
            b.endObject();
        });
        assertEquals(String.format(Locale.ROOT, """
            {"path":{"name":"%s"}}""", name), syntheticSource);
    }

    public void testRuntimeDynamicObjectManyFields() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);

        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("boolean_value").field("type", "boolean").endObject();
            b.startObject("path").field("type", "object").field("dynamic", "runtime");
            {
                b.startObject("properties");
                {
                    b.startObject("string_value").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();

        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.field("boolean_value", booleanValue);
            b.startObject("path");
            {
                b.field("int_value", intValue);
                b.startObject("to");
                {
                    b.startObject("some");
                    {
                        b.startObject("deeply");
                        {
                            b.startObject("nested");
                            b.field("string_value", stringValue);
                            b.endObject();
                        }
                        b.endObject();
                    }
                    b.endObject();
                }
                b.field("string_value", stringValue);
                b.endObject();
            }
            b.endObject();
        });

        assertEquals(String.format(Locale.ROOT, """
            {"boolean_value":%s,"path":{"int_value":%s,"to":{"some":{"deeply":{"nested":{"string_value":"%s"}}},\
            "string_value":"%s"}}}""", booleanValue, intValue, stringValue, stringValue), syntheticSource);
    }

    public void testRuntimeDynamicObjectSimpleArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "runtime").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().field("name", "foo").endObject();
                b.startObject().field("name", "bar").endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"name":["foo","bar"]}}""", syntheticSource);
    }

    public void testRuntimeDynamicObjectSimpleValueArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "runtime").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(
            documentMapper,
            b -> { b.startObject("path").array("name", "A", "B", "C", "D").endObject(); }
        );
        assertEquals("""
            {"path":{"name":["A","B","C","D"]}}""", syntheticSource);
    }

    public void testRuntimeDynamicObjectNestedArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path").field("type", "object").field("dynamic", "runtime").endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startArray("path");
            {
                b.startObject().startObject("to").field("foo", "A").field("bar", "B").endObject().endObject();
                b.startObject().startObject("to").field("foo", "C").field("bar", "D").endObject().endObject();
            }
            b.endArray();
        });
        assertEquals("""
            {"path":{"to":[{"foo":"A","bar":"B"},{"foo":"C","bar":"D"}]}}""", syntheticSource);
    }

    public void testDisabledSubObjectWithNameOverlappingParentName() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            b.startObject("properties");
            {
                b.startObject("at").field("type", "object").field("enabled", "false").endObject();
            }
            b.endObject();
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startObject("at").field("foo", "A").endObject();
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"at":{"foo":"A"}}}""", syntheticSource);
    }

    public void testStoredNestedSubObjectWithNameOverlappingParentName() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            b.startObject("properties");
            {
                b.startObject("at").field("type", "nested").field("synthetic_source_keep", "all").endObject();
            }
            b.endObject();
            b.endObject();
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> {
            b.startObject("path");
            {
                b.startObject("at").field("foo", "A").endObject();
            }
            b.endObject();
        });
        assertEquals("""
            {"path":{"at":{"foo":"A"}}}""", syntheticSource);
    }

    public void testCopyToLogicInsideObject() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            b.startObject("properties");
            {
                b.startObject("at").field("type", "keyword").field("copy_to", "copy_top.copy").endObject();
            }
            b.endObject();
            b.endObject();
            b.startObject("copy_top");
            b.startObject("properties");
            {
                b.startObject("copy").field("type", "keyword").endObject();
            }
            b.endObject();
            b.endObject();
        })).documentMapper();

        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.startObject("path");
            b.field("at", "A");
            b.endObject();
        };

        var doc = documentMapper.parse(source(document));
        assertNotNull(doc.docs().get(0).getField("copy_top.copy"));

        var syntheticSource = syntheticSource(documentMapper, document);
        assertEquals("{\"path\":{\"at\":\"A\"}}", syntheticSource);
    }

    public void testDynamicIgnoredObjectWithFlatFields() throws IOException {
        DocumentMapper documentMapper = createMapperService(topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject();
            b.field("dynamic", false);
        })).documentMapper();

        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.startObject("top");
            b.field("file.name", "A");
            b.field("file.line", 10);
            b.endObject();
        };

        var syntheticSource = syntheticSource(documentMapper, document);
        assertEquals("{\"top\":{\"file.name\":\"A\",\"file.line\":10}}", syntheticSource);

        CheckedConsumer<XContentBuilder, IOException> documentWithArray = b -> {
            b.startArray("top");
            b.startObject();
            b.field("file.name", "A");
            b.field("file.line", 10);
            b.endObject();
            b.startObject();
            b.field("file.name", "B");
            b.field("file.line", 20);
            b.endObject();
            b.endArray();
        };

        var syntheticSourceWithArray = syntheticSource(documentMapper, documentWithArray);
        assertEquals("""
            {"top":[{"file.name":"A","file.line":10},{"file.name":"B","file.line":20}]}""", syntheticSourceWithArray);
    }

    public void testDisabledRootObjectWithFlatFields() throws IOException {
        DocumentMapper documentMapper = createMapperService(topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject();
            b.field("enabled", false);
        })).documentMapper();

        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.startObject("top");
            b.field("file.name", "A");
            b.field("file.line", 10);
            b.endObject();
        };

        var syntheticSource = syntheticSource(documentMapper, document);
        assertEquals("{\"top\":{\"file.name\":\"A\",\"file.line\":10}}", syntheticSource);

        CheckedConsumer<XContentBuilder, IOException> documentWithArray = b -> {
            b.startArray("top");
            b.startObject();
            b.field("file.name", "A");
            b.field("file.line", 10);
            b.endObject();
            b.startObject();
            b.field("file.name", "B");
            b.field("file.line", 20);
            b.endObject();
            b.endArray();
        };

        var syntheticSourceWithArray = syntheticSource(documentMapper, documentWithArray);
        assertEquals("""
            {"top":[{"file.name":"A","file.line":10},{"file.name":"B","file.line":20}]}""", syntheticSourceWithArray);
    }

    public void testDisabledObjectWithFlatFields() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("top").field("type", "object").field("enabled", false).endObject();
        })).documentMapper();

        CheckedConsumer<XContentBuilder, IOException> document = b -> {
            b.startObject("top");
            b.field("file.name", "A");
            b.field("file.line", 10);
            b.endObject();
        };

        var syntheticSource = syntheticSource(documentMapper, document);
        assertEquals("{\"top\":{\"file.name\":\"A\",\"file.line\":10}}", syntheticSource);

        CheckedConsumer<XContentBuilder, IOException> documentWithArray = b -> {
            b.startArray("top");
            b.startObject();
            b.field("file.name", "A");
            b.field("file.line", 10);
            b.endObject();
            b.startObject();
            b.field("file.name", "B");
            b.field("file.line", 20);
            b.endObject();
            b.endArray();
        };

        var syntheticSourceWithArray = syntheticSource(documentMapper, documentWithArray);
        assertEquals("""
            {"top":[{"file.name":"A","file.line":10},{"file.name":"B","file.line":20}]}""", syntheticSourceWithArray);
    }

    protected void validateRoundTripReader(String syntheticSource, DirectoryReader reader, DirectoryReader roundTripReader)
        throws IOException {
        // We exclude ignored source field since in some cases it contains an exact copy of a part of document source.
        // Sometime synthetic source is different in this case (structurally but not logically)
        // and since the copy is exact, contents of ignored source are different.
        assertReaderEquals(
            "round trip " + syntheticSource,
            new FieldMaskingReader(Set.of(SourceFieldMapper.RECOVERY_SOURCE_NAME, IgnoredSourceFieldMapper.NAME), reader),
            new FieldMaskingReader(Set.of(SourceFieldMapper.RECOVERY_SOURCE_NAME, IgnoredSourceFieldMapper.NAME), roundTripReader)
        );
    }
}
