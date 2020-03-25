/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class IndexerStateEnumTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(IndexerState.STARTED.ordinal(), equalTo(0));
        assertThat(IndexerState.INDEXING.ordinal(), equalTo(1));
        assertThat(IndexerState.STOPPING.ordinal(), equalTo(2));
        assertThat(IndexerState.STOPPED.ordinal(), equalTo(3));
        assertThat(IndexerState.ABORTING.ordinal(), equalTo(4));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexerState.STARTED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexerState.INDEXING.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexerState.STOPPING.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexerState.STOPPED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexerState.ABORTING.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(4));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(IndexerState.fromStream(in), equalTo(IndexerState.STARTED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(IndexerState.fromStream(in), equalTo(IndexerState.INDEXING));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(IndexerState.fromStream(in), equalTo(IndexerState.STOPPING));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(IndexerState.fromStream(in), equalTo(IndexerState.STOPPED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(4);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(IndexerState.fromStream(in), equalTo(IndexerState.ABORTING));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(3, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                IndexerState.fromStream(in);
                fail("Expected IOException");
            } catch(IOException e) {
                assertThat(e.getMessage(), containsString("Unknown IndexerState ordinal ["));
            }

        }
    }
}
