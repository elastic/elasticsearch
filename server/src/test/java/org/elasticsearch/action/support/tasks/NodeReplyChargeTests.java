/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.tasks;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class NodeReplyChargeTests extends ESTestCase {

    public void testCancellationBetweenAdmissionAndPublishingRollsTheChargeBack() throws Exception {
        final CountDownLatch admitted = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        final LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofKb(1)) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
                super.addEstimateBytesAndMaybeBreak(bytes, label);
                admitted.countDown();
                safeAwait(resume);
            }
        };
        final TransportTasksAction.NodeReplyCharge charge = new TransportTasksAction.NodeReplyCharge(breaker);
        final Thread charging = new Thread(() -> charge.charge(100));
        charging.start();
        safeAwait(admitted);
        assertThat(breaker.getUsed(), equalTo(100L));

        charge.close();
        resume.countDown();
        charging.join();

        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testRetainedChargeSurvivesTheOwnerAndIsReleasedWithTheLastHolder() {
        final LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofKb(1));
        final TransportTasksAction.NodeReplyCharge charge = new TransportTasksAction.NodeReplyCharge(breaker);
        charge.charge(100);
        final Releasable retention = charge.retain();

        charge.close();
        assertThat(breaker.getUsed(), equalTo(100L));
        retention.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testChargeAlreadyReleasedCannotBeRetained() {
        final LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofKb(1));
        final TransportTasksAction.NodeReplyCharge charge = new TransportTasksAction.NodeReplyCharge(breaker);
        charge.charge(100);

        charge.close();

        assertThat(charge.retain(), nullValue());
        assertThat(breaker.getUsed(), equalTo(0L));
        charge.charge(100);
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
