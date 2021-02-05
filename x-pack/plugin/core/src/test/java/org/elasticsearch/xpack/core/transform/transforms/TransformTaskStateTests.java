/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransformTaskStateTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(TransformTaskState.STOPPED.ordinal(), equalTo(0));
        assertThat(TransformTaskState.STARTED.ordinal(), equalTo(1));
        assertThat(TransformTaskState.FAILED.ordinal(), equalTo(2));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TransformTaskState.STOPPED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TransformTaskState.STARTED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TransformTaskState.FAILED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(TransformTaskState.fromStream(in), equalTo(TransformTaskState.STOPPED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(TransformTaskState.fromStream(in), equalTo(TransformTaskState.STARTED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(TransformTaskState.fromStream(in), equalTo(TransformTaskState.FAILED));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(3, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                TransformTaskState.fromStream(in);
                fail("Expected IOException");
            } catch(IOException e) {
                assertThat(e.getMessage(), containsString("Unknown TransformTaskState ordinal ["));
            }

        }
    }
}
