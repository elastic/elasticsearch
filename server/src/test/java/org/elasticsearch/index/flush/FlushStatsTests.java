/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.flush;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FlushStatsTests extends AbstractWireSerializingTestCase<FlushStats> {

    @Override
    protected Writeable.Reader<FlushStats> instanceReader() {
        return FlushStats::new;
    }

    @Override
    protected FlushStats createTestInstance() {
        long failedShardClosed = randomLongBetween(0, Integer.MAX_VALUE);
        long failedEngineClosed = randomLongBetween(0, Integer.MAX_VALUE);
        long failedPendingTranslogRecovery = randomLongBetween(0, Integer.MAX_VALUE);
        return new FlushStats(
            randomNonNegativeLong() / 2,
            randomNonNegativeLong() / 2,
            randomNonNegativeLong() / 2,
            randomNonNegativeLong() / 2,
            failedShardClosed + failedEngineClosed + failedPendingTranslogRecovery + randomLongBetween(0, Integer.MAX_VALUE),
            failedShardClosed,
            failedEngineClosed,
            failedPendingTranslogRecovery
        );
    }

    @Override
    protected FlushStats mutateInstance(FlushStats instance) {
        long failed = instance.getFailed();
        long failedShardClosed = instance.getFailedShardClosed();
        long failedEngineClosed = instance.getFailedEngineClosed();
        long failedPendingTranslogRecovery = instance.getFailedPendingTranslogRecovery();
        switch (between(0, 3)) {
            case 0 -> failed++;
            case 1 -> failedShardClosed++;
            case 2 -> failedEngineClosed++;
            case 3 -> failedPendingTranslogRecovery++;
            default -> throw new AssertionError("unexpected branch");
        }
        return new FlushStats(
            instance.getTotal(),
            instance.getPeriodic(),
            instance.getTotalTimeInMillis(),
            instance.getTotalTimeExcludingWaitingOnLockMillis(),
            failed,
            failedShardClosed,
            failedEngineClosed,
            failedPendingTranslogRecovery
        );
    }

    public void testFailureCountsAreNotSentToOldNodes() throws IOException {
        FlushStats stats = new FlushStats(1, 2, 3, 4, 5, 6, 7, 8);

        FlushStats readByOldNode = copyInstance(stats, TransportVersion.minimumCompatible());

        assertThat(readByOldNode, equalTo(new FlushStats(1, 2, 3, 4, 0, 0, 0, 0)));
    }

    public void testAddTotalsSumsFailureCounts() {
        FlushStats sum = new FlushStats(1, 1, 1, 1, 6, 1, 2, 3);

        sum.addTotals(new FlushStats(1, 1, 1, 1, 10, 2, 3, 4));

        assertThat(sum.getFailed(), equalTo(16L));
        assertThat(sum.getFailedShardClosed(), equalTo(3L));
        assertThat(sum.getFailedEngineClosed(), equalTo(5L));
        assertThat(sum.getFailedPendingTranslogRecovery(), equalTo(7L));
    }

    public void testFailureCountsAreRenderedAsXContent() {
        String json = Strings.toString(new FlushStats(1, 2, 3, 4, 5, 6, 7, 8), false, false);

        assertThat(json, containsString("\"failed\":5"));
        assertThat(json, containsString("\"failed_shard_closed\":6"));
        assertThat(json, containsString("\"failed_engine_closed\":7"));
        assertThat(json, containsString("\"failed_pending_translog_recovery\":8"));
    }
}
