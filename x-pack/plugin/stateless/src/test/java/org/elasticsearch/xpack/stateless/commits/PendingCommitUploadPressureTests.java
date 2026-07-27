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
        pressure.markBytesQueued(limit);
        pressure.checkAndMaybeReject();
    }

    public void testOverLimitThrows() {
        var pressure = createPressure(Settings.EMPTY);
        long limit = pressure.getPendingBytesLimit();
        pressure.markBytesQueued(limit + 1);
        var e = expectThrows(EsRejectedExecutionException.class, pressure::checkAndMaybeReject);
        assertThat(e.getMessage(), containsString("pending_bytes_limit=" + limit));
        assertFalse(e.isExecutorShutdown());
    }

    public void testMarkBytesUploadedDecreasesCount() {
        var pressure = createPressure(Settings.EMPTY);
        long limit = pressure.getPendingBytesLimit();
        pressure.markBytesQueued(limit + 1);
        expectThrows(EsRejectedExecutionException.class, pressure::checkAndMaybeReject);

        pressure.markBytesUploaded(2);
        pressure.checkAndMaybeReject();
    }

    public void testLimitEqualsMemorySetting() {
        long limitBytes = ByteSizeValue.ofMb(200).getBytes();
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
