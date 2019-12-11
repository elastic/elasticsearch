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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RetentionLeasesXContentTests extends AbstractXContentTestCase<RetentionLeases> {

    @Override
    protected RetentionLeases createTestInstance() {
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
        final int length = randomIntBetween(0, 8);
        final List<RetentionLease> leases = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomNonNegativeLong();
            final long timestamp = randomNonNegativeLong();
            final String source = randomAlphaOfLength(8);
            final RetentionLease retentionLease = new RetentionLease(id, retainingSequenceNumber, timestamp, source);
            leases.add(retentionLease);
        }
        return new RetentionLeases(primaryTerm, version, leases);
    }

    @Override
    protected RetentionLeases doParseInstance(final XContentParser parser) throws IOException {
        return RetentionLeases.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

}
