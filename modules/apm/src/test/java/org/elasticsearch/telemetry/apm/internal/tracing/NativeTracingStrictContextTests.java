/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

/** Runs the native contracts with the SDK's opt-in scope diagnostic, separately from normal entitlement coverage. */
@ESTestCase.WithoutEntitlements // StrictContextStorage's diagnostic watchdog triggered manage_threads; production does not enable this
                                // diagnostic.
@ThreadLeakFilters(filters = NativeTracingStrictContextTests.WatchdogThreadFilter.class)
public class NativeTracingStrictContextTests extends NativeTracingTests {
    /** Strict storage must be selected at JVM startup; ordinary runs retain the normal entitlement checks. */
    @BeforeClass
    public static void requireStrictContextChecking() {
        assumeTrue("enable the SDK diagnostic explicitly", Boolean.getBoolean("io.opentelemetry.context.enableStrictContext"));
    }

    /** The strict checker owns one JVM-lifetime daemon, independent of any node or SDK lifecycle. */
    public static class WatchdogThreadFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            return thread.getName().equals("weak-ref-cleaner-strictcontextstorage");
        }
    }
}
