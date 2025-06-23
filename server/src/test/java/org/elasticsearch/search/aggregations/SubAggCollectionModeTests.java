/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SubAggCollectionModeTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(SubAggCollectionMode.DEPTH_FIRST.ordinal(), equalTo(0));
        assertThat(SubAggCollectionMode.BREADTH_FIRST.ordinal(), equalTo(1));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            SubAggCollectionMode.DEPTH_FIRST.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            SubAggCollectionMode.BREADTH_FIRST.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(SubAggCollectionMode.readFromStream(in), equalTo(SubAggCollectionMode.DEPTH_FIRST));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(SubAggCollectionMode.readFromStream(in), equalTo(SubAggCollectionMode.BREADTH_FIRST));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(2, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                SubAggCollectionMode.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown SubAggCollectionMode ordinal ["));
            }

        }
    }
}
