/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export.otelsdk;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Excludes OkHttp and Okio global daemon threads from the thread-leak checker.
 * {@code OkHttp TaskRunner} and {@code Okio Watchdog} are JVM-wide singletons that persist
 * after all OkHttp clients are closed; they are not a true resource leak. Matching by exact
 * name (rather than prefix) avoids masking leaked per-request OkHttp client threads.
 */
public class OkHttpThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        String name = t.getName();
        return name.equals("OkHttp TaskRunner") || name.equals("Okio Watchdog");
    }
}
