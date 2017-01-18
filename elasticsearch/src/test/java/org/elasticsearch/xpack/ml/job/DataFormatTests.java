/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import java.io.IOException;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.DataDescription.DataFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataFormatTests extends ESTestCase {

    public void testDataFormatForString() {
        assertEquals(DataFormat.DELIMITED, DataFormat.forString("delineated"));
        assertEquals(DataFormat.DELIMITED, DataFormat.forString("DELINEATED"));
        assertEquals(DataFormat.DELIMITED, DataFormat.forString("delimited"));
        assertEquals(DataFormat.DELIMITED, DataFormat.forString("DELIMITED"));

        assertEquals(DataFormat.JSON, DataFormat.forString("json"));
        assertEquals(DataFormat.JSON, DataFormat.forString("JSON"));

        assertEquals(DataFormat.SINGLE_LINE, DataFormat.forString("single_line"));
        assertEquals(DataFormat.SINGLE_LINE, DataFormat.forString("SINGLE_LINE"));
    }

    public void testValidOrdinals() {
        assertThat(DataFormat.JSON.ordinal(), equalTo(0));
        assertThat(DataFormat.DELIMITED.ordinal(), equalTo(1));
        assertThat(DataFormat.SINGLE_LINE.ordinal(), equalTo(2));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFormat.JSON.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFormat.DELIMITED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFormat.SINGLE_LINE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFormat.readFromStream(in), equalTo(DataFormat.JSON));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFormat.readFromStream(in), equalTo(DataFormat.DELIMITED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFormat.readFromStream(in), equalTo(DataFormat.SINGLE_LINE));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(4, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                DataFormat.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown DataFormat ordinal ["));
            }
        }
    }

}
