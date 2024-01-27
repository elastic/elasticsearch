/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.mockito.stubbing.Answer;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RotatableSecretTests extends ESTestCase {

    private final SecureString secret1 = new SecureString(randomAlphaOfLength(10));
    private final SecureString secret2 = new SecureString(randomAlphaOfLength(10));
    private final SecureString secret3 = new SecureString(randomAlphaOfLength(10));

    public void testBasicRotation() throws Exception {
        // initial state
        RotatableSecret rotatableSecret = new RotatableSecret(secret1);
        assertTrue(rotatableSecret.matches(secret1));
        assertFalse(rotatableSecret.matches(secret2));
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertTrue(rotatableSecret.isSet());
        assertEquals(secret1, rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());
        assertEquals(Instant.EPOCH, rotatableSecret.getSecrets().priorValidTill());

        // normal rotation
        TimeValue expiresIn = TimeValue.timeValueDays(1);
        rotatableSecret.rotate(secret2, expiresIn);
        assertTrue(rotatableSecret.matches(secret1));
        assertTrue(rotatableSecret.matches(secret2));
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertTrue(rotatableSecret.isSet());
        assertEquals(secret2, rotatableSecret.getSecrets().current());
        assertEquals(secret1, rotatableSecret.getSecrets().prior());
        assertTrue(rotatableSecret.getSecrets().priorValidTill().isAfter(Instant.now()));
        assertTrue(
            rotatableSecret.getSecrets().priorValidTill().isBefore(Instant.now().plusMillis(TimeValue.timeValueDays(2).getMillis()))
        );

        // attempt to rotate same value does nothing
        rotatableSecret.rotate(secret2, TimeValue.timeValueDays(99)); // ignores the new expiry since you can't rotate the same secret
        assertTrue(rotatableSecret.matches(secret1));
        assertTrue(rotatableSecret.matches(secret2));
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertTrue(rotatableSecret.isSet());
        assertEquals(secret2, rotatableSecret.getSecrets().current());
        assertEquals(secret1, rotatableSecret.getSecrets().prior());
        assertTrue(rotatableSecret.getSecrets().priorValidTill().isAfter(Instant.now()));
        assertTrue(
            rotatableSecret.getSecrets().priorValidTill().isBefore(Instant.now().plusMillis(TimeValue.timeValueDays(2).getMillis()))
        );

        // rotate with expiry
        rotatableSecret.rotate(secret3, TimeValue.timeValueMillis(1));
        Thread.sleep(2); // ensure secret2 is expired
        assertTrue(rotatableSecret.matches(secret3));
        assertFalse(rotatableSecret.matches(secret1));
        assertFalse(rotatableSecret.matches(secret2));
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertTrue(rotatableSecret.isSet());
        assertEquals(secret3, rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());
        assertTrue(rotatableSecret.getSecrets().priorValidTill().isBefore(Instant.now()));

        // unset current and prior
        rotatableSecret.rotate(null, TimeValue.ZERO);
        assertFalse(rotatableSecret.matches(secret3));
        assertFalse(rotatableSecret.matches(secret1));
        assertFalse(rotatableSecret.matches(secret2));
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertFalse(rotatableSecret.isSet());
        assertNull(rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());
        assertTrue(rotatableSecret.getSecrets().priorValidTill().isBefore(Instant.now()));
    }

    public void testConcurrentReadWhileLocked() throws Exception {
        // initial state
        RotatableSecret rotatableSecret = new RotatableSecret(secret1);
        assertTrue(rotatableSecret.matches(secret1));
        assertFalse(rotatableSecret.matches(secret2));
        assertEquals(secret1, rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());

        boolean expired = randomBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        TimeValue mockGracePeriod = mock(TimeValue.class);  // use a mock to force a long rotation to exercise the concurrency
        when(mockGracePeriod.getMillis()).then((Answer<Long>) invocation -> {
            latch.await();
            return expired ? 0L : Long.MAX_VALUE;
        });

        // start writer thread
        Thread t1 = new Thread(() -> rotatableSecret.rotate(secret2, mockGracePeriod));
        t1.start();
        assertBusy(() -> assertEquals(Thread.State.WAITING, t1.getState())); // waiting on countdown latch, holds write lock
        assertTrue(rotatableSecret.isWriteLocked());

        // start reader threads
        int readers = randomIntBetween(1, 16);
        Set<Thread> readerThreads = new HashSet<>(readers);
        for (int i = 0; i < readers; i++) {
            Thread t = new Thread(() -> {
                if (randomBoolean()) { // either matches or isSet can block
                    if (expired) {
                        assertFalse(rotatableSecret.matches(secret1));
                    } else {
                        assertTrue(rotatableSecret.matches(secret1));
                    }
                    assertTrue(rotatableSecret.matches(secret2));
                } else {
                    assertTrue(rotatableSecret.isSet());
                }
            });
            readerThreads.add(t);
            t.start();
        }
        for (Thread t : readerThreads) {
            assertBusy(() -> assertEquals(Thread.State.WAITING, t.getState())); // waiting on write lock from thread 1 to be released
        }
        assertTrue(rotatableSecret.isWriteLocked());
        latch.countDown(); // let thread1 finish, which also unblocks the reader threads
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t1.getState())); // done with work
        for (Thread t : readerThreads) {
            assertBusy(() -> assertEquals(Thread.State.TERMINATED, t.getState())); // done with work
            t.join();
        }
        t1.join();
        assertFalse(rotatableSecret.isWriteLocked());
    }

    public void testConcurrentRotations() throws Exception {
        // initial state
        RotatableSecret rotatableSecret = new RotatableSecret(secret1);
        assertTrue(rotatableSecret.matches(secret1));
        assertFalse(rotatableSecret.matches(secret2));
        assertEquals(secret1, rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());

        // start first rotation
        AtomicBoolean latch1 = new AtomicBoolean(false); // using boolean as latch to differentiate the kinds of waiting
        TimeValue mockGracePeriod1 = mock(TimeValue.class);  // use a mock to force a long rotation to exercise the concurrency
        when(mockGracePeriod1.getMillis()).then((Answer<Long>) invocation -> {
            while (latch1.get() == false) {
                Thread.sleep(10); // thread in TIMED_WAITING
            }
            return Long.MAX_VALUE;
        });
        Thread t1 = new Thread(() -> rotatableSecret.rotate(secret2, mockGracePeriod1));
        t1.start();
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, t1.getState())); // waiting on latch, holds write lock

        // start second rotation
        AtomicBoolean latch2 = new AtomicBoolean(false);
        TimeValue mockGracePeriod2 = mock(TimeValue.class);  // use a mock to force a long rotation to exercise the concurrency
        when(mockGracePeriod2.getMillis()).then((Answer<Long>) invocation -> {
            while (latch2.get() == false) {
                Thread.sleep(10); // thread in TIMED_WAITING
            }
            return Long.MAX_VALUE;
        });
        Thread t2 = new Thread(() -> rotatableSecret.rotate(secret3, mockGracePeriod2));
        t2.start();
        assertBusy(() -> assertEquals(Thread.State.WAITING, t2.getState())); // waiting on write lock from thread 1

        // start third rotation
        AtomicBoolean latch3 = new AtomicBoolean(false);
        TimeValue mockGracePeriod3 = mock(TimeValue.class);  // use a mock to force a long rotation to exercise the concurrency
        when(mockGracePeriod3.getMillis()).then((Answer<Long>) invocation -> {
            while (latch3.get() == false) {
                Thread.sleep(10); // thread in TIMED_WAITING
            }
            return Long.MAX_VALUE;
        });
        Thread t3 = new Thread(() -> rotatableSecret.rotate(null, mockGracePeriod3));
        t3.start();
        assertBusy(() -> assertEquals(Thread.State.WAITING, t3.getState())); // waiting on write lock from thread 1

        // initial state
        assertEquals(rotatableSecret.getSecrets().current(), secret1);
        assertNull(rotatableSecret.getSecrets().prior());
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, t1.getState())); // waiting on latch
        assertBusy(() -> assertEquals(Thread.State.WAITING, t2.getState())); // waiting on lock
        assertBusy(() -> assertEquals(Thread.State.WAITING, t3.getState())); // waiting on lock

        latch1.set(true); // let first rotation succeed
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t1.getState())); // work done
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, t2.getState())); // waiting on latch
        assertBusy(() -> assertEquals(Thread.State.WAITING, t3.getState()));  // waiting lock
        assertEquals(rotatableSecret.getSecrets().current(), secret2);
        assertEquals(rotatableSecret.getSecrets().prior(), secret1);

        latch2.set(true); // let second rotation succeed
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t1.getState())); // work done
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t2.getState())); // work done
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, t3.getState())); // waiting on latch
        assertEquals(rotatableSecret.getSecrets().current(), secret3);
        assertEquals(rotatableSecret.getSecrets().prior(), secret2);

        latch3.set(true); // let third rotation succeed
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t1.getState())); // work done
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t2.getState())); // work done
        assertBusy(() -> assertEquals(Thread.State.TERMINATED, t3.getState())); // work done
        assertEquals(rotatableSecret.getSecrets().current(), null);
        assertEquals(rotatableSecret.getSecrets().prior(), secret3);

        t1.join();
        t2.join();
        t3.join();
    }

    public void testUnsetThenRotate() {
        // it is not set on startup
        RotatableSecret rotatableSecret = new RotatableSecret(null);
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertFalse(rotatableSecret.isSet());
        assertNull(rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());
        assertEquals(Instant.EPOCH, rotatableSecret.getSecrets().priorValidTill());

        // normal rotation for when it was not set on startup
        TimeValue expiresIn = TimeValue.timeValueDays(1);
        rotatableSecret.rotate(secret1, expiresIn);
        assertTrue(rotatableSecret.matches(secret1));
        assertFalse(rotatableSecret.matches(new SecureString(randomAlphaOfLength(10))));
        assertTrue(rotatableSecret.isSet());
        assertEquals(secret1, rotatableSecret.getSecrets().current());
        assertNull(rotatableSecret.getSecrets().prior());
        assertTrue(rotatableSecret.getSecrets().priorValidTill().isAfter(Instant.now()));
        assertTrue(
            rotatableSecret.getSecrets().priorValidTill().isBefore(Instant.now().plusMillis(TimeValue.timeValueDays(2).getMillis()))
        );
    }
}
