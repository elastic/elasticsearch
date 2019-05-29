/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformTaskStateTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(DataFrameTransformTaskState.STOPPED.ordinal(), equalTo(0));
        assertThat(DataFrameTransformTaskState.STARTED.ordinal(), equalTo(1));
        assertThat(DataFrameTransformTaskState.FAILED.ordinal(), equalTo(2));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFrameTransformTaskState.STOPPED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFrameTransformTaskState.STARTED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DataFrameTransformTaskState.FAILED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFrameTransformTaskState.fromStream(in), equalTo(DataFrameTransformTaskState.STOPPED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFrameTransformTaskState.fromStream(in), equalTo(DataFrameTransformTaskState.STARTED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DataFrameTransformTaskState.fromStream(in), equalTo(DataFrameTransformTaskState.FAILED));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(3, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                DataFrameTransformTaskState.fromStream(in);
                fail("Expected IOException");
            } catch(IOException e) {
                assertThat(e.getMessage(), containsString("Unknown DataFrameTransformTaskState ordinal ["));
            }

        }
    }
}
