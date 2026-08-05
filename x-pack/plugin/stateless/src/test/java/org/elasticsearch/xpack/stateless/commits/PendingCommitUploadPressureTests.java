/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for the byte-accounting and rejection behaviour of {@link PendingCommitUploadPressure}.
 * These exercise the package-private byte primitives directly; the VBCC-to-bytes translation
 * ({@link PendingCommitUploadPressure#markVbccQueued}/{@link PendingCommitUploadPressure#markVbccUploaded})
 * is covered against real {@code VirtualBatchedCompoundCommit}s in {@code StatelessCommitServiceTests}.
 */
public class PendingCommitUploadPressureTests extends ESTestCase {

    public void testNotOverLimitWhenEmpty() {
        var pressure = createPressure(Settings.EMPTY);
        pressure.checkAndMaybeReject();
    }

    public void testNotOverLimitBelowThreshold() {
        var pressure = createPressure(Settings.EMPTY);
        long limit = pressure.getPendingBytesLimit();
        // Anything up to and including the limit must not be rejected (the check is strictly greater-than).
        pressure.markBytesQueued(randomLongBetween(0, limit));
        pressure.checkAndMaybeReject();
    }

    public void testOverLimitThrows() {
        var pressure = createPressure(Settings.EMPTY);
        long limit = pressure.getPendingBytesLimit();
        pressure.markBytesQueued(limit + randomLongBetween(1, 1_000_000));
        var e = expectThrows(EsRejectedExecutionException.class, pressure::checkAndMaybeReject);
        assertThat(e.getMessage(), containsString("pending_bytes_limit=" + limit));
        assertFalse(e.isExecutorShutdown());
    }

    public void testMarkBytesUploadedDecreasesCount() {
        var pressure = createPressure(Settings.EMPTY);
        long limit = pressure.getPendingBytesLimit();
        long over = randomLongBetween(1, 1_000_000);
        pressure.markBytesQueued(limit + over);
        expectThrows(EsRejectedExecutionException.class, pressure::checkAndMaybeReject);

        // Releasing at least `over` brings the total back to at-or-below the limit, so it no longer rejects.
        pressure.markBytesUploaded(randomLongBetween(over, limit + over));
        pressure.checkAndMaybeReject();
    }

    public void testAccountingBalancesOverRandomOperations() {
        var pressure = createPressure(Settings.EMPTY);
        List<Long> queued = new ArrayList<>();
        int ops = randomIntBetween(1, 50);
        for (int i = 0; i < ops; i++) {
            long bytes = randomLongBetween(1, 1_000_000);
            pressure.markBytesQueued(bytes);
            queued.add(bytes);
        }
        // Releasing every queued amount, in any order, must bring the tracked total back to exactly zero.
        Collections.shuffle(queued, random());
        for (long bytes : queued) {
            pressure.markBytesUploaded(bytes);
        }
        assertThat(pressure.getPendingBytes(), equalTo(0L));
    }

    public void testLimitEqualsMemorySetting() {
        long limitBytes = ByteSizeValue.ofMb(randomIntBetween(1, 500)).getBytes();
        Settings settings = Settings.builder()
            .put(PendingCommitUploadPressure.PENDING_CC_UPLOAD_MEMORY_LIMIT.getKey(), limitBytes + "b")
            .build();
        var pressure = createPressure(settings);
        assertThat(pressure.getPendingBytesLimit(), equalTo(limitBytes));
    }

    private static PendingCommitUploadPressure createPressure(Settings settings) {
        return new PendingCommitUploadPressure(settings);
    }
}
