/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.audit.data;

import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DataValuesTests extends ESTestCase {

    public void testScalarConversions() {
        assertThat(DataValues.fromJava(null), sameInstance(DataNull.INSTANCE));
        assertThat(DataValues.fromJava("hello"), equalTo(new DataString("hello")));
        assertThat(DataValues.fromJava(new StringBuilder("built")), equalTo(new DataString("built")));
        assertThat(DataValues.fromJava(true), equalTo(new DataBoolean(true)));
    }

    public void testIntegralNumbersBecomeDataLong() {
        assertThat(DataValues.fromJava((byte) 7), equalTo(new DataLong(7L)));
        assertThat(DataValues.fromJava((short) 8), equalTo(new DataLong(8L)));
        assertThat(DataValues.fromJava(9), equalTo(new DataLong(9L)));
        assertThat(DataValues.fromJava(10L), equalTo(new DataLong(10L)));
        assertThat(DataValues.fromJava(BigInteger.valueOf(11)), equalTo(new DataLong(11L)));
    }

    public void testIntegerBeyondLongBecomesDataInteger() {
        assertThat(
            DataValues.fromJava(new BigInteger("12345678901234567890")),
            equalTo(new DataInteger(new BigInteger("12345678901234567890")))
        );
        assertThat(
            DataValues.fromJava(new BigInteger("-99999999999999999999")),
            equalTo(new DataInteger(new BigInteger("-99999999999999999999")))
        );
    }

    public void testDecimalNumbersBecomeDataDouble() {
        assertThat(DataValues.fromJava(1.5d), equalTo(new DataDouble(1.5d)));
        assertThat(DataValues.fromJava(2.5f), equalTo(new DataDouble(2.5d)));
        assertThat(DataValues.fromJava(new BigDecimal("3.25")), equalTo(new DataDouble(3.25d)));
    }

    public void testDecimalNotRepresentableAsDoubleBecomesDataDecimal() {
        assertThat(
            DataValues.fromJava(new BigDecimal("3.14159265358979323846")),
            equalTo(new DataDecimal(new BigDecimal("3.14159265358979323846")))
        );
    }

    public void testAlreadyConvertedValuesPassThrough() {
        DataObject nested = new DataObject().with("k", "v");
        assertThat(DataValues.fromJava(nested), sameInstance(nested));
    }

    public void testObjectArrayBecomesArray() {
        DataArray array = DataValues.fromJava(new Object[] { "a", 1L, true }).requireArray();
        assertThat(array.size(), equalTo(3));
        assertThat(array.get(0), equalTo(new DataString("a")));
        assertThat(array.get(1), equalTo(new DataLong(1L)));
        assertThat(array.get(2), equalTo(new DataBoolean(true)));
    }

    public void testMapPreservesInsertionOrder() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("z", "first");
        map.put("a", "second");
        map.put("m", "third");

        DataObject object = DataValues.objectFromMap(map);

        assertThat(object.view().keySet(), contains("z", "a", "m"));
        assertThat(object.require("a").requireString(), equalTo("second"));
    }

    public void testNestedStructures() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", "role");
        map.put("indices", List.of("a", "b"));
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("enabled", true);
        map.put("meta", inner);

        DataObject object = DataValues.objectFromMap(map);

        DataArray indices = object.require("indices").requireArray();
        assertThat(indices.size(), equalTo(2));
        assertThat(indices.get(0).requireString(), equalTo("a"));
        assertThat(object.require("meta").requireObject().require("enabled"), equalTo(new DataBoolean(true)));
    }

    public void testUnsupportedValueTypeIsRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataValues.fromJava(new Object()));
        assertThat(e.getMessage(), equalTo("Cannot convert value of type [java.lang.Object] to a DataValue"));
    }

    public void testNonStringKeyIsRejected() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(42, "value");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataValues.objectFromMap(map));
        assertThat(e.getMessage(), equalTo("DataObject field names must be strings but found [java.lang.Integer]"));
    }

    public void testNullKeyIsRejected() {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(null, "value");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataValues.objectFromMap(map));
        assertThat(e.getMessage(), equalTo("DataObject field names must be strings but found [null]"));
    }

    public void testToJavaScalars() {
        assertThat(DataValues.toJava(DataNull.INSTANCE), nullValue());
        assertThat(DataValues.toJava(new DataString("x")), equalTo("x"));
        assertThat(DataValues.toJava(new DataBoolean(true)), equalTo(true));
        assertThat(DataValues.toJava(new DataLong(5L)), equalTo(5L));
        assertThat(DataValues.toJava(new DataDouble(1.5d)), equalTo(1.5d));
        assertThat(
            DataValues.toJava(new DataInteger(new BigInteger("12345678901234567890"))),
            equalTo(new BigInteger("12345678901234567890"))
        );
        assertThat(
            DataValues.toJava(new DataDecimal(new BigDecimal("3.14159265358979323846"))),
            equalTo(new BigDecimal("3.14159265358979323846"))
        );
    }

    public void testToMapAndToListPreserveOrder() {
        DataObject object = new DataObject();
        object.put("z", "1").put("a", DataValues.arrayFrom(List.of("x", "y")));

        Map<String, Object> map = DataValues.toMap(object);
        assertThat(map.keySet(), contains("z", "a"));
        assertThat(map.get("a"), equalTo(List.of("x", "y")));
    }

    public void testRoundTripFromJavaToJava() {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("name", "role");
        original.put("count", 3L);
        original.put("ratio", 1.5d);
        original.put("enabled", true);
        original.put("missing", null);
        original.put("tags", List.of("a", "b"));
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("k", "v");
        original.put("meta", nested);

        assertThat(DataValues.toMap(DataValues.objectFromMap(original)), equalTo(original));
    }
}
