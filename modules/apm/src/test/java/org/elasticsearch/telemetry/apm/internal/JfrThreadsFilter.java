/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * OpenTelemetry's runtime-telemetry-java17 uses JFR (Java Flight Recorder)
 * which spawns a "JFR Periodic Tasks" thread that persists after test completion.
 * Filter this thread out since we can't clean it up.
 */
public class JfrThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("JFR ");
    }
}
