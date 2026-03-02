/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.NO_VALUE_PRESENT;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class StatefulValueTests extends ESTestCase {

    private static final String VALUE = "value";

    public void testUndefined_ReturnsSingleton() {
        assertThat(StatefulValue.undefined(), sameInstance(StatefulValue.undefined()));
    }

    public void testNullInstance_ReturnsSingleton() {
        assertThat(StatefulValue.nullInstance(), sameInstance(StatefulValue.nullInstance()));
    }

    public void testOf_ThrowsWhenNull() {
        expectThrows(NullPointerException.class, () -> StatefulValue.of((String) null));
    }

    public void testOf_ReturnsNewInstanceWithValue() {
        var value = randomAlphaOfLength(10);
        var statefulValue = StatefulValue.of(value);
        assertTrue(statefulValue.isPresent());
        assertThat(statefulValue.get(), is(value));
    }

    public void testIsUndefined_WhenUndefined() {
        assertTrue(StatefulValue.<String>undefined().isUndefined());
    }

    public void testIsUndefined_WhenNull() {
        assertFalse(StatefulValue.<String>nullInstance().isUndefined());
    }

    public void testIsUndefined_WhenWithValue() {
        assertFalse(StatefulValue.of(VALUE).isUndefined());
    }

    public void testIsNull_WhenUndefined() {
        assertFalse(StatefulValue.<String>undefined().isNull());
    }

    public void testIsNull_WhenNull() {
        assertTrue(StatefulValue.<String>nullInstance().isNull());
    }

    public void testIsNull_WhenWithValue() {
        assertFalse(StatefulValue.of(VALUE).isNull());
    }

    public void testIsPresent_WhenUndefined() {
        assertFalse(StatefulValue.<String>undefined().isPresent());
    }

    public void testIsPresent_WhenNull() {
        assertFalse(StatefulValue.<String>nullInstance().isPresent());
    }

    public void testIsPresent_WhenWithValue() {
        assertTrue(StatefulValue.of(VALUE).isPresent());
    }

    public void testGet_ReturnsValueWhenPresent() {
        var value = randomAlphaOfLength(10);
        assertThat(StatefulValue.of(value).get(), is(value));
    }

    public void testGet_ThrowsWhenUndefined() {
        var e = expectThrows(NoSuchElementException.class, () -> StatefulValue.<String>undefined().get());
        assertThat(e.getMessage(), is(NO_VALUE_PRESENT.getMessage()));
    }

    public void testGet_ThrowsWhenNull() {
        var e = expectThrows(NoSuchElementException.class, () -> StatefulValue.<String>nullInstance().get());
        assertThat(e.getMessage(), is(NO_VALUE_PRESENT.getMessage()));
    }

    public void testOrElse_ReturnsValueWhenPresent() {
        var value = randomAlphaOfLength(10);
        var other = randomAlphaOfLength(10);
        assertThat(StatefulValue.of(value).orElse(other), is(value));
    }

    public void testOrElse_ReturnsOtherWhenUndefined() {
        var other = randomAlphaOfLength(10);
        assertThat(StatefulValue.<String>undefined().orElse(other), is(other));
    }

    public void testOrElse_ReturnsOtherWhenNull() {
        var other = randomAlphaOfLength(10);
        assertThat(StatefulValue.<String>nullInstance().orElse(other), is(other));
    }

    public void testEquals_hashCode() {
        {
            var map = new HashMap<>(Map.of(randomAlphaOfLength(10), randomAlphaOfLength(10)));
            var equalMap = new HashMap<>(map);
            assertEquals(StatefulValue.of(map), StatefulValue.of(equalMap));
            assertEquals(StatefulValue.of(map).hashCode(), StatefulValue.of(equalMap).hashCode());
        }
        {
            assertEquals(StatefulValue.undefined().hashCode(), StatefulValue.undefined().hashCode());
            assertEquals(StatefulValue.nullInstance().hashCode(), StatefulValue.nullInstance().hashCode());
        }
    }

    public void testEquals_WhenPresentWithDifferentValues() {
        assertNotEquals(StatefulValue.of("a"), StatefulValue.of("b"));
    }

    public void testEquals_WhenDifferentStates() {
        assertNotEquals(StatefulValue.<String>undefined(), StatefulValue.nullInstance());
        assertNotEquals(StatefulValue.<String>undefined(), StatefulValue.of(VALUE));
        assertNotEquals(StatefulValue.<String>nullInstance(), StatefulValue.of(VALUE));
    }

    public void testSerializationRoundtrip_WhenUndefined() throws IOException {
        var original = StatefulValue.<String>undefined();
        var copy = roundtrip(original);
        assertThat(copy, sameInstance(original));
    }

    public void testSerializationRoundtrip_WhenNull() throws IOException {
        var original = StatefulValue.<String>nullInstance();
        var copy = roundtrip(original);
        assertThat(copy, sameInstance(original));
    }

    public void testSerializationRoundtrip_WhenPresent() throws IOException {
        var value = randomAlphaOfLength(10);
        var original = StatefulValue.of(value);
        var copy = roundtrip(original);
        assertThat(copy, is(original));
        assertTrue(copy.isPresent());
        assertThat(copy.get(), is(value));
    }

    private static StatefulValue<String> roundtrip(StatefulValue<String> original) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            StatefulValue.write(out, original, StreamOutput::writeString);
            try (StreamInput in = out.bytes().streamInput()) {
                return StatefulValue.read(in, StreamInput::readString);
            }
        }
    }
}
