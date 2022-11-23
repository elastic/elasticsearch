/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class RetentionLeaseTests extends ESTestCase {

    public void testEmptyId() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RetentionLease("", randomNonNegativeLong(), randomNonNegativeLong(), "source")
        );
        assertThat(e, hasToString(containsString("retention lease ID can not be empty")));
    }

    public void testRetainingSequenceNumberOutOfRange() {
        final long retainingSequenceNumber = randomLongBetween(Long.MIN_VALUE, -1);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RetentionLease("id", retainingSequenceNumber, randomNonNegativeLong(), "source")
        );
        assertThat(
            e,
            hasToString(containsString("retention lease retaining sequence number [" + retainingSequenceNumber + "] out of range"))
        );
    }

    public void testTimestampOutOfRange() {
        final long timestamp = randomLongBetween(Long.MIN_VALUE, -1);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RetentionLease("id", randomNonNegativeLong(), timestamp, "source")
        );
        assertThat(e, hasToString(containsString("retention lease timestamp [" + timestamp + "] out of range")));
    }

    public void testEmptySource() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RetentionLease("id", randomNonNegativeLong(), randomNonNegativeLong(), "")
        );
        assertThat(e, hasToString(containsString("retention lease source can not be empty")));
    }

    public void testRetentionLeaseSerialization() throws IOException {
        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomLongBetween(0, Long.MAX_VALUE);
        final long timestamp = randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        final RetentionLease retentionLease = new RetentionLease(id, retainingSequenceNumber, timestamp, source);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            retentionLease.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(retentionLease, equalTo(new RetentionLease(in)));
            }
        }
    }

}
