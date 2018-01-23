/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats.MemoryStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class MemoryStatusTests extends ESTestCase {

    public void testFromString() {
        assertEquals(MemoryStatus.OK, MemoryStatus.fromString("ok"));
        assertEquals(MemoryStatus.SOFT_LIMIT, MemoryStatus.fromString("soft_limit"));
        assertEquals(MemoryStatus.HARD_LIMIT, MemoryStatus.fromString("hard_limit"));
        assertEquals(MemoryStatus.OK, MemoryStatus.fromString("OK"));
        assertEquals(MemoryStatus.SOFT_LIMIT, MemoryStatus.fromString("SOFT_LIMIT"));
        assertEquals(MemoryStatus.HARD_LIMIT, MemoryStatus.fromString("HARD_LIMIT"));
    }

    public void testToString() {
        assertEquals("ok", MemoryStatus.OK.toString());
        assertEquals("soft_limit", MemoryStatus.SOFT_LIMIT.toString());
        assertEquals("hard_limit", MemoryStatus.HARD_LIMIT.toString());
    }

    public void testValidOrdinals() {
        assertThat(MemoryStatus.OK.ordinal(), equalTo(0));
        assertThat(MemoryStatus.SOFT_LIMIT.ordinal(), equalTo(1));
        assertThat(MemoryStatus.HARD_LIMIT.ordinal(), equalTo(2));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MemoryStatus.OK.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MemoryStatus.SOFT_LIMIT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MemoryStatus.HARD_LIMIT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MemoryStatus.readFromStream(in), equalTo(MemoryStatus.OK));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MemoryStatus.readFromStream(in), equalTo(MemoryStatus.SOFT_LIMIT));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(MemoryStatus.readFromStream(in), equalTo(MemoryStatus.HARD_LIMIT));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(3, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                MemoryStatus.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown MemoryStatus ordinal ["));
            }
        }
    }
}
