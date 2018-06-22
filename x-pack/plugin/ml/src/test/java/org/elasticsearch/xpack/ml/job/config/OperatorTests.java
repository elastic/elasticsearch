/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Operator;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OperatorTests extends ESTestCase {

    public void testFromString() {
        assertEquals(Operator.fromString("gt"), Operator.GT);
        assertEquals(Operator.fromString("gte"), Operator.GTE);
        assertEquals(Operator.fromString("lte"), Operator.LTE);
        assertEquals(Operator.fromString("lt"), Operator.LT);
        assertEquals(Operator.fromString("Gt"), Operator.GT);
        assertEquals(Operator.fromString("GTE"), Operator.GTE);
    }

    public void testToString() {
        assertEquals("gt", Operator.GT.toString());
        assertEquals("gte", Operator.GTE.toString());
        assertEquals("lte", Operator.LTE.toString());
        assertEquals("lt", Operator.LT.toString());
    }

    public void testTest() {
        assertTrue(Operator.GT.test(1.0, 0.0));
        assertFalse(Operator.GT.test(0.0, 1.0));

        assertTrue(Operator.GTE.test(1.0, 0.0));
        assertTrue(Operator.GTE.test(1.0, 1.0));
        assertFalse(Operator.GTE.test(0.0, 1.0));

        assertTrue(Operator.LT.test(0.0, 1.0));
        assertFalse(Operator.LT.test(0.0, 0.0));

        assertTrue(Operator.LTE.test(0.0, 1.0));
        assertTrue(Operator.LTE.test(1.0, 1.0));
        assertFalse(Operator.LTE.test(1.0, 0.0));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.GT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.GTE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.LT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.LTE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.GT));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.GTE));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.LT));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.LTE));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(7, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                Operator.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown Operator ordinal ["));
            }
        }
    }
}