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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SuspendableRefContainerTests extends ESTestCase {

    public void testBasicAcquire() throws InterruptedException {
        SuspendableRefContainer refContainer = new SuspendableRefContainer();
        assertThat(refContainer.activeRefs(), equalTo(0));

        Releasable lock1 = randomLockingMethod(refContainer);
        assertThat(refContainer.activeRefs(), equalTo(1));
        Releasable lock2 = randomLockingMethod(refContainer);
        assertThat(refContainer.activeRefs(), equalTo(2));
        lock1.close();
        assertThat(refContainer.activeRefs(), equalTo(1));
        lock1.close(); // check idempotence
        assertThat(refContainer.activeRefs(), equalTo(1));
        lock2.close();
        assertThat(refContainer.activeRefs(), equalTo(0));
    }

    public void testAcquisitionBlockingBlocksNewAcquisitions() throws InterruptedException {
        SuspendableRefContainer refContainer = new SuspendableRefContainer();
        assertThat(refContainer.activeRefs(), equalTo(0));

        try (Releasable block = refContainer.blockAcquisition()) {
            assertThat(refContainer.activeRefs(), equalTo(0));
            assertThat(refContainer.tryAcquire(), nullValue());
            assertThat(refContainer.activeRefs(), equalTo(0));
        }
        try (Releasable lock = refContainer.tryAcquire()) {
            assertThat(refContainer.activeRefs(), equalTo(1));
        }

        // same with blocking acquire
        AtomicBoolean acquired = new AtomicBoolean();
        Thread t = new Thread(() -> {
            try (Releasable lock = randomBoolean() ? refContainer.acquire() : refContainer.acquireUninterruptibly()) {
                acquired.set(true);
                assertThat(refContainer.activeRefs(), equalTo(1));
            } catch (InterruptedException e) {
                fail("Interrupted");
            }
        });
        try (Releasable block = refContainer.blockAcquisition()) {
            assertThat(refContainer.activeRefs(), equalTo(0));
            t.start();
            // check that blocking acquire really blocks
            assertThat(acquired.get(), equalTo(false));
            assertThat(refContainer.activeRefs(), equalTo(0));
        }
        t.join();
        assertThat(acquired.get(), equalTo(true));
        assertThat(refContainer.activeRefs(), equalTo(0));
    }

    public void testAcquisitionBlockingWaitsOnExistingAcquisitions() throws InterruptedException {
        SuspendableRefContainer refContainer = new SuspendableRefContainer();

        AtomicBoolean acquired = new AtomicBoolean();
        Thread t = new Thread(() -> {
            try (Releasable block = refContainer.blockAcquisition()) {
                acquired.set(true);
                assertThat(refContainer.activeRefs(), equalTo(0));
            }
        });
        try (Releasable lock = randomLockingMethod(refContainer)) {
            assertThat(refContainer.activeRefs(), equalTo(1));
            t.start();
            assertThat(acquired.get(), equalTo(false));
            assertThat(refContainer.activeRefs(), equalTo(1));
        }
        t.join();
        assertThat(acquired.get(), equalTo(true));
        assertThat(refContainer.activeRefs(), equalTo(0));
    }

    private Releasable randomLockingMethod(SuspendableRefContainer refContainer) throws InterruptedException {
        switch (randomInt(2)) {
            case 0: return refContainer.tryAcquire();
            case 1: return refContainer.acquire();
            case 2: return refContainer.acquireUninterruptibly();
        }
        throw new IllegalArgumentException("randomLockingMethod inconsistent");
    }
}
