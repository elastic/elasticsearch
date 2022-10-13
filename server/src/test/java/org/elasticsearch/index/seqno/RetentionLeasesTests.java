/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class RetentionLeasesTests extends ESTestCase {

    public void testPrimaryTermOutOfRange() {
        final long primaryTerm = randomLongBetween(Long.MIN_VALUE, 0);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RetentionLeases(primaryTerm, randomNonNegativeLong(), Collections.emptyList())
        );
        assertThat(e, hasToString(containsString("primary term must be positive but was [" + primaryTerm + "]")));
    }

    public void testVersionOutOfRange() {
        final long version = randomLongBetween(Long.MIN_VALUE, -1);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RetentionLeases(randomLongBetween(1, Long.MAX_VALUE), version, Collections.emptyList())
        );
        assertThat(e, hasToString(containsString("version must be non-negative but was [" + version + "]")));
    }

    public void testSupersedesByPrimaryTerm() {
        final long lowerPrimaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final RetentionLeases left = new RetentionLeases(lowerPrimaryTerm, randomLongBetween(1, Long.MAX_VALUE), Collections.emptyList());
        final long higherPrimaryTerm = randomLongBetween(lowerPrimaryTerm + 1, Long.MAX_VALUE);
        final RetentionLeases right = new RetentionLeases(higherPrimaryTerm, randomLongBetween(1, Long.MAX_VALUE), Collections.emptyList());
        assertTrue(right.supersedes(left));
        assertTrue(right.supersedes(left.primaryTerm(), left.version()));
        assertFalse(left.supersedes(right));
        assertFalse(left.supersedes(right.primaryTerm(), right.version()));
    }

    public void testSupersedesByVersion() {
        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final long lowerVersion = randomLongBetween(1, Long.MAX_VALUE);
        final long higherVersion = randomLongBetween(lowerVersion + 1, Long.MAX_VALUE);
        final RetentionLeases left = new RetentionLeases(primaryTerm, lowerVersion, Collections.emptyList());
        final RetentionLeases right = new RetentionLeases(primaryTerm, higherVersion, Collections.emptyList());
        assertTrue(right.supersedes(left));
        assertTrue(right.supersedes(left.primaryTerm(), left.version()));
        assertFalse(left.supersedes(right));
        assertFalse(left.supersedes(right.primaryTerm(), right.version()));
    }

    public void testRetentionLeasesRejectsDuplicates() {
        final RetentionLeases retentionLeases = randomRetentionLeases(false);
        final RetentionLease retentionLease = randomFrom(retentionLeases.leases());
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new RetentionLeases(
                retentionLeases.primaryTerm(),
                retentionLeases.version(),
                Stream.concat(retentionLeases.leases().stream(), Stream.of(retentionLease)).toList()
            )
        );
        assertThat(e, hasToString(containsString("duplicate retention lease ID [" + retentionLease.id() + "]")));
    }

    public void testLeasesPreservesIterationOrder() {
        final RetentionLeases retentionLeases = randomRetentionLeases(true);
        if (retentionLeases.leases().isEmpty()) {
            assertThat(retentionLeases.leases(), empty());
        } else {
            assertThat(retentionLeases.leases(), contains(retentionLeases.leases().toArray(new RetentionLease[0])));
        }
    }

    public void testRetentionLeasesMetadataStateFormat() throws IOException {
        final Path path = createTempDir();
        final RetentionLeases retentionLeases = randomRetentionLeases(true);
        RetentionLeases.FORMAT.writeAndCleanup(retentionLeases, path);
        assertThat(RetentionLeases.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path), equalTo(retentionLeases));
    }

    private RetentionLeases randomRetentionLeases(boolean allowEmpty) {
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
        final int length = randomIntBetween(allowEmpty ? 0 : 1, 8);
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

}
