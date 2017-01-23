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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RuleConditionTypeTests extends ESTestCase {

    public void testForString() {
        assertEquals(RuleConditionType.CATEGORICAL, RuleConditionType.forString("categorical"));
        assertEquals(RuleConditionType.CATEGORICAL, RuleConditionType.forString("CATEGORICAL"));
        assertEquals(RuleConditionType.NUMERICAL_ACTUAL, RuleConditionType.forString("numerical_actual"));
        assertEquals(RuleConditionType.NUMERICAL_ACTUAL, RuleConditionType.forString("NUMERICAL_ACTUAL"));
        assertEquals(RuleConditionType.NUMERICAL_TYPICAL, RuleConditionType.forString("numerical_typical"));
        assertEquals(RuleConditionType.NUMERICAL_TYPICAL, RuleConditionType.forString("NUMERICAL_TYPICAL"));
        assertEquals(RuleConditionType.NUMERICAL_DIFF_ABS, RuleConditionType.forString("numerical_diff_abs"));
        assertEquals(RuleConditionType.NUMERICAL_DIFF_ABS, RuleConditionType.forString("NUMERICAL_DIFF_ABS"));
    }

    public void testValidOrdinals() {
        assertThat(RuleConditionType.CATEGORICAL.ordinal(), equalTo(0));
        assertThat(RuleConditionType.NUMERICAL_ACTUAL.ordinal(), equalTo(1));
        assertThat(RuleConditionType.NUMERICAL_TYPICAL.ordinal(), equalTo(2));
        assertThat(RuleConditionType.NUMERICAL_DIFF_ABS.ordinal(), equalTo(3));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            RuleConditionType.CATEGORICAL.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            RuleConditionType.NUMERICAL_ACTUAL.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            RuleConditionType.NUMERICAL_TYPICAL.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            RuleConditionType.NUMERICAL_DIFF_ABS.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(RuleConditionType.readFromStream(in), equalTo(RuleConditionType.CATEGORICAL));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(RuleConditionType.readFromStream(in), equalTo(RuleConditionType.NUMERICAL_ACTUAL));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(RuleConditionType.readFromStream(in), equalTo(RuleConditionType.NUMERICAL_TYPICAL));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(RuleConditionType.readFromStream(in), equalTo(RuleConditionType.NUMERICAL_DIFF_ABS));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(4, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                RuleConditionType.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown RuleConditionType ordinal ["));
            }
        }
    }

}
