/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.testcontainers;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * test container spawns extra threads, which causes our thread leak
 * detection to fail. Filter these threads out since we can't clean them up.
 *
 * This also filters the "JNA Cleaner" thread since TestContainers uses JNA
 * internally for Docker communication, and the JNA cleaner is a static
 * JVM-level thread that cleans up native memory references.
 */
public class TestContainersThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("testcontainers-")
            || t.getName().startsWith("ducttape")
            || t.getName().startsWith("ForkJoinPool.commonPool-worker-")
            || t.getName().equals("JNA Cleaner");
    }
}
