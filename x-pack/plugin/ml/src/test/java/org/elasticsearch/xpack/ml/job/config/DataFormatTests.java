/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription.DataFormat;

import static org.hamcrest.Matchers.equalTo;

public class DataFormatTests extends ESTestCase {

    public void testFromString() {
        assertEquals(DataFormat.XCONTENT, DataFormat.forString("xcontent"));
        assertEquals(DataFormat.XCONTENT, DataFormat.forString("XCONTENT"));

        // old undocumented option silently modified to the only current valid option
        assertEquals(DataFormat.XCONTENT, DataFormat.forString("delimited"));
        assertEquals(DataFormat.XCONTENT, DataFormat.forString("DELIMITED"));
    }

    public void testToString() {
        assertEquals("xcontent", DataFormat.XCONTENT.toString());
    }

    public void testValidOrdinals() {
        assertThat(DataFormat.XCONTENT.ordinal(), equalTo(0));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFormat.XCONTENT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFormat.readFromStream(in), equalTo(DataFormat.XCONTENT));
            }
        }
        // old undocumented option silently modified to the only current valid option
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFormat.readFromStream(in), equalTo(DataFormat.XCONTENT));
            }
        }
    }
}
