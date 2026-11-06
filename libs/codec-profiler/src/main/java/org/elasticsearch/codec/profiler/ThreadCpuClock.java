/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import org.elasticsearch.core.Nullable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.function.LongSupplier;

/**
 * The platform's clock for the CPU time of the current thread, resolved once. Support is checked here and never
 * again; a reading can still come back negative later, which {@link Timer} treats as the clock failing.
 */
final class ThreadCpuClock {

    @Nullable
    private static final LongSupplier PLATFORM = resolve();

    private ThreadCpuClock() {}

    /** The platform clock, or null when the platform cannot measure the current thread's CPU time. */
    @Nullable
    static LongSupplier platform() {
        return PLATFORM;
    }

    @Nullable
    private static LongSupplier resolve() {
        try {
            final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            if (threads.isCurrentThreadCpuTimeSupported() == false || threads.isThreadCpuTimeEnabled() == false) {
                return null;
            }
            return threads::getCurrentThreadCpuTime;
        } catch (UnsupportedOperationException | SecurityException e) {
            return null;
        }
    }
}
