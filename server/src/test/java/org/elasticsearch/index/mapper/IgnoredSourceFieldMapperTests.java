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
import org.hamcrest.Matchers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Base64;

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
            b.field("int_value", intValue);
            b.field("string_value", stringValue);
        });
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"string_value\":\"" + stringValue + "\""));
    }

    public void testMultipleIgnoredFieldsSameObject() throws IOException {
        boolean booleanValue = randomBoolean();
        int intValue = randomInt();
        String stringValue = randomAlphaOfLength(20);
        String syntheticSource = getSyntheticSourceWithFieldLimit(b -> {
            b.startObject("bar");
            {
                b.field("boolean_value", booleanValue);
                b.field("int_value", intValue);
                b.field("string_value", stringValue);
            }
            b.endObject();
        });
        assertThat(syntheticSource, Matchers.containsString("{\"bar\":{"));
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"string_value\":\"" + stringValue + "\""));
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
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"path\":{\"to\":{\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"some\":{\"deeply\":{\"nested\":{\"string_value\":\"" + stringValue + "\""));
    }

    public void testDisabledRootObjectSingleField() throws IOException {
        String name = randomAlphaOfLength(20);
        DocumentMapper documentMapper = createMapperService(topMapping(b -> {
            b.startObject("_source").field("mode", "synthetic").endObject();
            b.field("enabled", false);
        })).documentMapper();
        var syntheticSource = syntheticSource(documentMapper, b -> { b.field("name", name); });
        assertThat(syntheticSource, Matchers.containsString("\"name\":\"" + name + "\""));
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
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"path\":{\"to\":{\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"some\":{\"deeply\":{\"nested\":{\"string_value\":\"" + stringValue + "\""));
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
        assertEquals("{\"path\":{\"name\":\"" + name + "\"}}", syntheticSource);
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
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"path\":{\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"some\":{\"deeply\":{\"nested\":{\"string_value\":\"" + stringValue + "\""));
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
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"path\":{\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"to\":{\"name\":\"" + name + "\"}}}"));
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
        assertThat(syntheticSource, Matchers.containsString("\"boolean_value\":" + booleanValue));
        assertThat(syntheticSource, Matchers.containsString("\"path\":{\"int_value\":" + intValue));
        assertThat(syntheticSource, Matchers.containsString("\"foo\":{\"name\":\"" + foo + "\"}"));
        assertThat(syntheticSource, Matchers.containsString("\"bar\":{\"name\":\"" + bar + "\"}"));
    }
}
