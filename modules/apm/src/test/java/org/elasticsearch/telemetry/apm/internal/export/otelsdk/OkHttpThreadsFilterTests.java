/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import org.elasticsearch.test.ESTestCase;

public class OkHttpThreadsFilterTests extends ESTestCase {

    private final OkHttpThreadsFilter filter = new OkHttpThreadsFilter();

    public void testRejectsOkHttpTaskRunner() {
        assertTrue(filter.reject(threadNamed("OkHttp TaskRunner")));
    }

    public void testRejectsOkioWatchdog() {
        assertTrue(filter.reject(threadNamed("Okio Watchdog")));
    }

    public void testDoesNotRejectOkHttpClientThreads() {
        // Per-request OkHttp dispatcher threads share the "OkHttp" prefix but are real
        // threads that should be detectable as leaks.
        assertFalse(filter.reject(threadNamed("OkHttp https://example.com/...")));
        assertFalse(filter.reject(threadNamed("OkHttp Dispatcher")));
    }

    public void testDoesNotRejectUnrelatedThreads() {
        assertFalse(filter.reject(threadNamed("main")));
        assertFalse(filter.reject(threadNamed("elasticsearch[node-1][search][T#1]")));
    }

    private static Thread threadNamed(String name) {
        Thread t = new Thread();
        t.setName(name);
        return t;
    }
}
