/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OperatorTests extends ESTestCase {

    public void testFromString() {
        assertEquals(Operator.fromString("eq"), Operator.EQ);
        assertEquals(Operator.fromString("gt"), Operator.GT);
        assertEquals(Operator.fromString("gte"), Operator.GTE);
        assertEquals(Operator.fromString("lte"), Operator.LTE);
        assertEquals(Operator.fromString("lt"), Operator.LT);
        assertEquals(Operator.fromString("match"), Operator.MATCH);
        assertEquals(Operator.fromString("Gt"), Operator.GT);
        assertEquals(Operator.fromString("EQ"), Operator.EQ);
        assertEquals(Operator.fromString("GTE"), Operator.GTE);
        assertEquals(Operator.fromString("Match"), Operator.MATCH);
    }

    public void testToString() {
        assertEquals("eq", Operator.EQ.toString());
        assertEquals("gt", Operator.GT.toString());
        assertEquals("gte", Operator.GTE.toString());
        assertEquals("lte", Operator.LTE.toString());
        assertEquals("lt", Operator.LT.toString());
        assertEquals("match", Operator.MATCH.toString());
    }

    public void testTest() {
        assertTrue(Operator.GT.expectsANumericArgument());
        assertTrue(Operator.GT.test(1.0, 0.0));
        assertFalse(Operator.GT.test(0.0, 1.0));

        assertTrue(Operator.GTE.expectsANumericArgument());
        assertTrue(Operator.GTE.test(1.0, 0.0));
        assertTrue(Operator.GTE.test(1.0, 1.0));
        assertFalse(Operator.GTE.test(0.0, 1.0));

        assertTrue(Operator.EQ.expectsANumericArgument());
        assertTrue(Operator.EQ.test(0.0, 0.0));
        assertFalse(Operator.EQ.test(1.0, 0.0));

        assertTrue(Operator.LT.expectsANumericArgument());
        assertTrue(Operator.LT.test(0.0, 1.0));
        assertFalse(Operator.LT.test(0.0, 0.0));

        assertTrue(Operator.LTE.expectsANumericArgument());
        assertTrue(Operator.LTE.test(0.0, 1.0));
        assertTrue(Operator.LTE.test(1.0, 1.0));
        assertFalse(Operator.LTE.test(1.0, 0.0));
    }

    public void testMatch() {
        assertFalse(Operator.MATCH.expectsANumericArgument());
        assertFalse(Operator.MATCH.test(0.0, 1.0));

        Pattern pattern = Pattern.compile("^aa.*");

        assertTrue(Operator.MATCH.match(pattern, "aaaaa"));
        assertFalse(Operator.MATCH.match(pattern, "bbaaa"));
    }

    public void testValidOrdinals() {
        assertThat(Operator.EQ.ordinal(), equalTo(0));
        assertThat(Operator.GT.ordinal(), equalTo(1));
        assertThat(Operator.GTE.ordinal(), equalTo(2));
        assertThat(Operator.LT.ordinal(), equalTo(3));
        assertThat(Operator.LTE.ordinal(), equalTo(4));
        assertThat(Operator.MATCH.ordinal(), equalTo(5));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.EQ.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.GT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.GTE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.LT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.LTE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(4));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.MATCH.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(5));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.EQ));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.GT));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.GTE));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.LT));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(4);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.LTE));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(5);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.MATCH));
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