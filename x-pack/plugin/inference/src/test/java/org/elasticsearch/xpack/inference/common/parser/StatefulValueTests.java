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

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.NO_VALUE_PRESENT;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class StatefulValueTests extends ESTestCase {

    private static final String VALUE = "value";

    public void testAbsent_ReturnsSingleton() {
        assertThat(StatefulValue.absent(), sameInstance(StatefulValue.absent()));
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

    public void testIsAbsent_WhenAbsent() {
        assertTrue(StatefulValue.<String>absent().isAbsent());
    }

    public void testIsAbsent_WhenNull() {
        assertFalse(StatefulValue.<String>nullInstance().isAbsent());
    }

    public void testIsAbsent_WhenWithValue() {
        assertFalse(StatefulValue.of(VALUE).isAbsent());
    }

    public void testIsNull_WhenAbsent() {
        assertFalse(StatefulValue.<String>absent().isNull());
    }

    public void testIsNull_WhenNull() {
        assertTrue(StatefulValue.<String>nullInstance().isNull());
    }

    public void testIsNull_WhenWithValue() {
        assertFalse(StatefulValue.of(VALUE).isNull());
    }

    public void testIsPresent_WhenAbsent() {
        assertFalse(StatefulValue.<String>absent().isPresent());
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

    public void testGet_ThrowsWhenAbsent() {
        var e = expectThrows(IllegalStateException.class, () -> StatefulValue.<String>absent().get());
        assertThat(e.getMessage(), is(NO_VALUE_PRESENT.getMessage()));
    }

    public void testGet_ThrowsWhenNull() {
        var e = expectThrows(IllegalStateException.class, () -> StatefulValue.<String>nullInstance().get());
        assertThat(e.getMessage(), is(NO_VALUE_PRESENT.getMessage()));
    }

    public void testOrElse_ReturnsValueWhenPresent() {
        var value = randomAlphaOfLength(10);
        var other = randomAlphaOfLength(10);
        assertThat(StatefulValue.of(value).orElse(other), is(value));
    }

    public void testOrElse_ReturnsOtherWhenAbsent() {
        var other = randomAlphaOfLength(10);
        assertThat(StatefulValue.<String>absent().orElse(other), is(other));
    }

    public void testOrElse_ReturnsOtherWhenNull() {
        var other = randomAlphaOfLength(10);
        assertThat(StatefulValue.<String>nullInstance().orElse(other), is(other));
    }

    public void testEquals_WhenPresentWithSameValue() {
        var value = randomAlphaOfLength(10);
        assertEquals(StatefulValue.of(value), StatefulValue.of(value));
    }

    public void testEquals_WhenPresentWithDifferentValues() {
        assertNotEquals(StatefulValue.of("a"), StatefulValue.of("b"));
    }

    public void testEquals_WhenDifferentStates() {
        assertNotEquals(StatefulValue.<String>absent(), StatefulValue.nullInstance());
        assertNotEquals(StatefulValue.<String>absent(), StatefulValue.of(VALUE));
        assertNotEquals(StatefulValue.<String>nullInstance(), StatefulValue.of(VALUE));
    }

    public void testHashCode_ConsistentWithEquals() {
        var value = randomAlphaOfLength(10);
        assertEquals(StatefulValue.absent().hashCode(), StatefulValue.absent().hashCode());
        assertEquals(StatefulValue.nullInstance().hashCode(), StatefulValue.nullInstance().hashCode());
        assertEquals(StatefulValue.of(value).hashCode(), StatefulValue.of(value).hashCode());
    }

    public void testSerializationRoundtrip_WhenAbsent() throws IOException {
        var original = StatefulValue.<String>absent();
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
