/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class WatchLockServiceTests extends ElasticsearchTestCase {

    @Test
    public void testLocking_notStarted() {
        WatchLockService lockService = new WatchLockService();
        try {
            lockService.acquire("_name");
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("not started"));
        }
    }

    @Test
    public void testLocking() {
        WatchLockService lockService = new WatchLockService();
        lockService.start();
        WatchLockService.Lock lock = lockService.acquire("_name");
        assertThat(lockService.getWatchLocks().hasLockedKeys(), is(true));
        lock.release();
        assertThat(lockService.getWatchLocks().hasLockedKeys(), is(false));
        lockService.stop();
    }

    @Test
    public void testLocking_alreadyHeld() {
        WatchLockService lockService = new WatchLockService();
        lockService.start();
        WatchLockService.Lock lock1 = lockService.acquire("_name");
        try {
            lockService.acquire("_name");
            fail("exception expected");
        } catch (ElasticsearchIllegalStateException e) {
            assertThat(e.getMessage(), containsString("Lock already acquired"));
        }
        lock1.release();
        lockService.stop();
    }

}
