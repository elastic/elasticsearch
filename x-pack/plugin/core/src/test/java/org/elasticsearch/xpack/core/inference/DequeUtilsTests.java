/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class DequeUtilsTests extends ESTestCase {

    public void testEqualsAndHashCodeWithSameObject() {
        var someObject = mock();
        var dequeOne = DequeUtils.of(someObject);
        var dequeTwo = DequeUtils.of(someObject);
        assertTrue(DequeUtils.dequeEquals(dequeOne, dequeTwo));
        assertEquals(DequeUtils.dequeHashCode(dequeOne), DequeUtils.dequeHashCode(dequeTwo));
    }

    public void testEqualsAndHashCodeWithEqualsObject() {
        var dequeOne = DequeUtils.of("the same string");
        var dequeTwo = DequeUtils.of("the same string");
        assertTrue(DequeUtils.dequeEquals(dequeOne, dequeTwo));
        assertEquals(DequeUtils.dequeHashCode(dequeOne), DequeUtils.dequeHashCode(dequeTwo));
    }

    public void testNotEqualsAndHashCode() {
        var dequeOne = DequeUtils.of(mock());
        var dequeTwo = DequeUtils.of(mock());
        assertFalse(DequeUtils.dequeEquals(dequeOne, dequeTwo));
        assertNotEquals(DequeUtils.dequeHashCode(dequeOne), DequeUtils.dequeHashCode(dequeTwo));
    }

    public void testReadFromStream() throws IOException {
        var dequeOne = DequeUtils.of("this is a string");
        var out = new BytesStreamOutput();
        out.writeStringCollection(dequeOne);
        var in = new ByteArrayStreamInput(out.bytes().array());
        var dequeTwo = DequeUtils.readDeque(in, StreamInput::readString);
        assertTrue(DequeUtils.dequeEquals(dequeOne, dequeTwo));
        assertEquals(DequeUtils.dequeHashCode(dequeOne), DequeUtils.dequeHashCode(dequeTwo));
    }
}
