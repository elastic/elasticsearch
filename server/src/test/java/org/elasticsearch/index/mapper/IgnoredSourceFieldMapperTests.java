/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;
import java.util.Locale;

public class IgnoredSourceFieldMapperTests extends MapperServiceTestCase {

    private String getSyntheticSourceWithFieldLimit(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        DocumentMapper documentMapper = createMapperService(
            Settings.builder()
                .put("index.mapping.total_fields.limit", 2)
                .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
                .build(),
            syntheticSourceMapping(b -> {
                b.startObject("foo").field("type", "keyword").endObject();
                b.startObject("bar").field("type", "object").endObject();
            })
        ).documentMapper();
        return syntheticSource(documentMapper, build);
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
        assertEquals("{\"my_value\":" + value + "}", getSyntheticSourceWithFieldLimit(b -> b.field("my_value", value)));
    }

    public void testMultipleIgnoredFieldsRootObject() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);
        String syntheticSource = getSyntheticSourceWithFieldLimit(b -> {
            b.field("boolean_value", booleanValue);
            b.field("string_value", stringValue);
            b.field("int_value", intValue);
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

    public void testRootArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object");
                b.field("store_array_source", true);
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
                    b.startObject("to").field("type", "object").field("store_array_source", true);
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

    public void testArrayWithinArray() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("path");
            {
                b.field("type", "object").field("store_array_source", true);
                b.startObject("properties");
                {
                    b.startObject("to").field("type", "object").field("store_array_source", true);
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

    public void testFieldOrdering() throws IOException {
        DocumentMapper documentMapper = createMapperService(syntheticSourceMapping(b -> {
            b.startObject("A").field("type", "integer").endObject();
            b.startObject("B").field("type", "object").field("store_array_source", true);
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
            b.startObject("D").field("type", "object").field("store_array_source", true);
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
            b.startObject("path").field("type", "nested").field("include_in_root", true);
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
            {"path":[{"name":"foo"},{"name":"bar"}]}""", syntheticSource);
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
            {"path":[{"to":{"foo":"A","bar":"B"}},{"to":{"foo":"C","bar":"D"}}]}""", syntheticSource);
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
            {"path":[{"name":"foo"},{"name":"bar"}]}""", syntheticSource);
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
            {"path":[{"to":{"foo":"A","bar":"B"}},{"to":{"foo":"C","bar":"D"}}]}""", syntheticSource);
    }
}
