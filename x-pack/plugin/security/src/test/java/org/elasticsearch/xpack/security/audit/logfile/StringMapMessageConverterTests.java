/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.datatree.DataArray;
import org.elasticsearch.datatree.DataDecimal;
import org.elasticsearch.datatree.DataDouble;
import org.elasticsearch.datatree.DataInteger;
import org.elasticsearch.datatree.DataNull;
import org.elasticsearch.datatree.DataObject;
import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StringMapMessageConverterTests extends ESTestCase {

    public void testScalarsAndNull() {
        DataObject entry = new DataObject();
        entry.with("s", "v");
        entry.put("b", true);
        entry.put("i", 7L);
        entry.put("n", DataNull.INSTANCE);

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("s"), equalTo("v"));
        assertThat(data.get("b"), equalTo("true"));
        assertThat(data.get("i"), equalTo("7"));
        // A DataNull field is dropped rather than emitted as a null-valued entry
        assertThat(data.containsKey("n"), is(false));
    }

    public void testNestedObjectRendersCompactJsonInOrder() {
        DataObject nested = new DataObject();
        nested.with("name", "role");
        nested.put("enabled", true);
        DataObject entry = new DataObject().put("put", nested);

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("put"), equalTo("{\"name\":\"role\",\"enabled\":true}"));
    }

    public void testNestedArrayRendersCompactJson() {
        DataObject entry = new DataObject().put("indices", new DataArray().add("a").add("b"));

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("indices"), equalTo("[\"a\",\"b\"]"));
    }

    public void testIntegerBeyondLongRangeRendersAsBareDigits() {
        DataObject entry = new DataObject().put("huge", new DataInteger(new BigInteger("12345678901234567890")));

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("huge"), equalTo("12345678901234567890"));
    }

    // Metadata carrying an integer beyond long range must stay an unquoted JSON number, matching the historical output
    // rather than the quoted string the earlier stringify policy would have produced.
    public void testNestedIntegerBeyondLongRangeRendersAsUnquotedJsonNumber() {
        DataObject metadata = new DataObject().put("huge", new DataInteger(new BigInteger("12345678901234567890")));
        DataObject entry = new DataObject().put("metadata", metadata);

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("metadata"), equalTo("{\"huge\":12345678901234567890}"));
    }

    public void testDoubleAndDecimalScalarsRenderAsStrings() {
        DataObject entry = new DataObject();
        entry.put("d", new DataDouble(1.5d));
        entry.put("bd", new DataDecimal(new BigDecimal("3.14159265358979323846")));

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("d"), equalTo("1.5"));
        assertThat(data.get("bd"), equalTo("3.14159265358979323846"));
    }

    public void testNestedDoubleAndDecimalRenderAsBareJsonNumbers() {
        DataObject metadata = new DataObject();
        metadata.put("d", new DataDouble(1.5d));
        metadata.put("bd", new DataDecimal(new BigDecimal("3.14159265358979323846")));
        DataObject entry = new DataObject().put("metadata", metadata);

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("metadata"), equalTo("{\"d\":1.5,\"bd\":3.14159265358979323846}"));
    }

    public void testNonStringArrayRendersBareJsonNumbers() {
        DataObject entry = new DataObject().put("nums", new DataArray().add(1L).add(2L));

        Map<String, String> data = StringMapMessageConverter.INSTANCE.convert(entry).getData();
        assertThat(data.get("nums"), equalTo("[1,2]"));
    }
}
