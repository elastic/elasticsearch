/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DateIntervalWrapperTests extends ESTestCase {
    public void testValidOrdinals() {
        assertThat(DateIntervalWrapper.IntervalTypeEnum.NONE.ordinal(), equalTo(0));
        assertThat(DateIntervalWrapper.IntervalTypeEnum.FIXED.ordinal(), equalTo(1));
        assertThat(DateIntervalWrapper.IntervalTypeEnum.CALENDAR.ordinal(), equalTo(2));
        assertThat(DateIntervalWrapper.IntervalTypeEnum.LEGACY_INTERVAL.ordinal(), equalTo(3));
        assertThat(DateIntervalWrapper.IntervalTypeEnum.LEGACY_DATE_HISTO.ordinal(), equalTo(4));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DateIntervalWrapper.IntervalTypeEnum.NONE.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DateIntervalWrapper.IntervalTypeEnum.FIXED.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DateIntervalWrapper.IntervalTypeEnum.CALENDAR.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DateIntervalWrapper.IntervalTypeEnum.LEGACY_INTERVAL.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(3));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            DateIntervalWrapper.IntervalTypeEnum.LEGACY_DATE_HISTO.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(4));
            }
        }

    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DateIntervalWrapper.IntervalTypeEnum.fromStream(in), equalTo(DateIntervalWrapper.IntervalTypeEnum.NONE));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DateIntervalWrapper.IntervalTypeEnum.fromStream(in), equalTo(DateIntervalWrapper.IntervalTypeEnum.FIXED));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(DateIntervalWrapper.IntervalTypeEnum.fromStream(in), equalTo(DateIntervalWrapper.IntervalTypeEnum.CALENDAR));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(
                    DateIntervalWrapper.IntervalTypeEnum.fromStream(in),
                    equalTo(DateIntervalWrapper.IntervalTypeEnum.LEGACY_INTERVAL)
                );
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(4);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(
                    DateIntervalWrapper.IntervalTypeEnum.fromStream(in),
                    equalTo(DateIntervalWrapper.IntervalTypeEnum.LEGACY_DATE_HISTO)
                );
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(5, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                DateIntervalWrapper.IntervalTypeEnum.fromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown IntervalTypeEnum ordinal ["));
            }

        }
    }
}
