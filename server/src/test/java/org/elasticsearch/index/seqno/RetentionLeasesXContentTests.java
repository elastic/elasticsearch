/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
