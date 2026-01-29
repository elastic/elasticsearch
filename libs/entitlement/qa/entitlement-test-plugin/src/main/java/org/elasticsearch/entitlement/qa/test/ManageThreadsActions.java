/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.currentThread;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "testing entitlements")
@SuppressWarnings({ "unused" /* called via reflaction */, "removal" })
class ManageThreadsActions {
    private ManageThreadsActions() {}

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_Thread$start() throws InterruptedException {
        AtomicBoolean threadRan = new AtomicBoolean(false);
        Thread thread = new Thread(() -> threadRan.set(true), "test");
        thread.start();
        thread.join();
        assert threadRan.get();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_Thread$setDaemon() {
        new Thread().setDaemon(true);
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_ThreadGroup$setDaemon() {
        currentThread().getThreadGroup().setDaemon(currentThread().getThreadGroup().isDaemon());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_util_concurrent_ForkJoinPool$setParallelism() {
        ForkJoinPool.commonPool().setParallelism(ForkJoinPool.commonPool().getParallelism());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_Thread$setName() {
        currentThread().setName(currentThread().getName());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_Thread$setPriority() {
        currentThread().setPriority(currentThread().getPriority());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_Thread$setUncaughtExceptionHandler() {
        currentThread().setUncaughtExceptionHandler(currentThread().getUncaughtExceptionHandler());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void java_lang_ThreadGroup$setMaxPriority() {
        currentThread().getThreadGroup().setMaxPriority(currentThread().getThreadGroup().getMaxPriority());
    }

}
