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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class RetentionLeaseStatsTests extends ESTestCase {

    public void testRetentionLeaseStatsSerialization() throws IOException {
        final int length = randomIntBetween(0, 8);
        final Collection<RetentionLease> leases;
        if (length == 0) {
            leases = Collections.emptyList();
        } else {
            leases = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                final String id = randomAlphaOfLength(8);
                final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                final long timestamp = randomNonNegativeLong();
                final String source = randomAlphaOfLength(8);
                leases.add(new RetentionLease(id, retainingSequenceNumber, timestamp, source));
            }
        }
        final RetentionLeaseStats stats = new RetentionLeaseStats(leases);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(new RetentionLeaseStats(in), equalTo(stats));
            }
        }
    }

}
