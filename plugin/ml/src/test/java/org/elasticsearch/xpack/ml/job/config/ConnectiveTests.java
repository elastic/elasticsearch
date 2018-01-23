/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Connective;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ConnectiveTests extends ESTestCase {

    public void testForString() {
        assertEquals(Connective.OR, Connective.fromString("or"));
        assertEquals(Connective.OR, Connective.fromString("OR"));
        assertEquals(Connective.AND, Connective.fromString("and"));
        assertEquals(Connective.AND, Connective.fromString("AND"));
    }

    public void testToString() {
        assertEquals("or", Connective.OR.toString());
        assertEquals("and", Connective.AND.toString());
    }

    public void testValidOrdinals() {
        assertThat(Connective.OR.ordinal(), equalTo(0));
        assertThat(Connective.AND.ordinal(), equalTo(1));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Connective.OR.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Connective.AND.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Connective.readFromStream(in), equalTo(Connective.OR));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Connective.readFromStream(in), equalTo(Connective.AND));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(2, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                Connective.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown Connective ordinal ["));
            }
        }
    }
}
