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

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class RetentionLeasesTests extends ESTestCase {

    public void testRetentionLeasesEncoding() {
        final long version = randomNonNegativeLong();
        final int length = randomIntBetween(0, 8);
        final List<RetentionLease> retentionLeases = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomNonNegativeLong();
            final long timestamp = randomNonNegativeLong();
            final String source = randomAlphaOfLength(8);
            final RetentionLease retentionLease = new RetentionLease(id, retainingSequenceNumber, timestamp, source);
            retentionLeases.add(retentionLease);
        }
        final RetentionLeases decodedRetentionLeases =
                RetentionLeases.decodeRetentionLeases(RetentionLeases.encodeRetentionLeases(new RetentionLeases(version, retentionLeases)));
        assertThat(decodedRetentionLeases.version(), equalTo(version));
        if (length == 0) {
            assertThat(decodedRetentionLeases.leases(), empty());
        } else {
            assertThat(decodedRetentionLeases.leases(), contains(retentionLeases.toArray(new RetentionLease[0])));
        }
    }

}