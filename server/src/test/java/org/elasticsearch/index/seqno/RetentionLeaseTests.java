/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
                () -> new RetentionLease("", randomNonNegativeLong(), randomNonNegativeLong(), "source"));
        assertThat(e, hasToString(containsString("retention lease ID can not be empty")));
    }

    public void testRetainingSequenceNumberOutOfRange() {
        final long retainingSequenceNumber = randomLongBetween(Long.MIN_VALUE, -1);
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new RetentionLease("id", retainingSequenceNumber, randomNonNegativeLong(), "source"));
        assertThat(
                e,
                hasToString(containsString("retention lease retaining sequence number [" + retainingSequenceNumber + "] out of range")));
    }

    public void testTimestampOutOfRange() {
        final long timestamp = randomLongBetween(Long.MIN_VALUE, -1);
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new RetentionLease("id", randomNonNegativeLong(), timestamp, "source"));
        assertThat(e, hasToString(containsString("retention lease timestamp [" + timestamp + "] out of range")));
    }

    public void testEmptySource() {
        final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new RetentionLease("id", randomNonNegativeLong(), randomNonNegativeLong(), ""));
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
