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

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

public class RetentionLeaseTests extends ESTestCase {

    public void testRetainingSequenceNumberOutOfRange() {
        final long retainingSequenceNumber = randomLongBetween(Long.MIN_VALUE, UNASSIGNED_SEQ_NO - 1);
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
                () -> new RetentionLease("id", randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE), timestamp, "source"));
        assertThat(
                e,
                hasToString(containsString("retention lease timestamp [" + timestamp + "] out of range")));
    }

}
