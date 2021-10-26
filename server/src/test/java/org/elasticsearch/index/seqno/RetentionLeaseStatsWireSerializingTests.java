/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class RetentionLeaseStatsWireSerializingTests extends AbstractWireSerializingTestCase<RetentionLeaseStats> {

    @Override
    protected RetentionLeaseStats createTestInstance() {
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
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
        return new RetentionLeaseStats(new RetentionLeases(primaryTerm, version, leases));
    }

    @Override
    protected Writeable.Reader<RetentionLeaseStats> instanceReader() {
        return RetentionLeaseStats::new;
    }

}
