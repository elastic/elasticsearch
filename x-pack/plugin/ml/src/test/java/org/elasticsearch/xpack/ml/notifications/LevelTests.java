/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.notifications;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.notifications.Level;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LevelTests extends ESTestCase {

    public void testFromString() {
        assertEquals(Level.INFO, Level.fromString("info"));
        assertEquals(Level.INFO, Level.fromString("INFO"));
        assertEquals(Level.ACTIVITY, Level.fromString("activity"));
        assertEquals(Level.ACTIVITY, Level.fromString("ACTIVITY"));
        assertEquals(Level.WARNING, Level.fromString("warning"));
        assertEquals(Level.WARNING, Level.fromString("WARNING"));
        assertEquals(Level.ERROR, Level.fromString("error"));
        assertEquals(Level.ERROR, Level.fromString("ERROR"));
    }

    public void testToString() {
        assertEquals("info", Level.INFO.toString());
        assertEquals("activity", Level.ACTIVITY.toString());
        assertEquals("warning", Level.WARNING.toString());
        assertEquals("error", Level.ERROR.toString());
    }

    public void testValidOrdinals() {
        assertThat(Level.INFO.ordinal(), equalTo(0));
        assertThat(Level.ACTIVITY.ordinal(), equalTo(1));
        assertThat(Level.WARNING.ordinal(), equalTo(2));
        assertThat(Level.ERROR.ordinal(), equalTo(3));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Level.INFO.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Level.ACTIVITY.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Level.WARNING.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Level.ERROR.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Level.readFromStream(in), equalTo(Level.INFO));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Level.readFromStream(in), equalTo(Level.ACTIVITY));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Level.readFromStream(in), equalTo(Level.WARNING));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Level.readFromStream(in), equalTo(Level.ERROR));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(4, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                Level.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown Level ordinal ["));
            }
        }
    }

}
