/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * Filter out threads controlled by gradle that may be created during unit tests.
 *
 * Currently this includes pooled threads for Exec as well as file system event watcher threads.
 */
public class GradleThreadsFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith("Exec process")
            || t.getName().startsWith("File watcher consumer")
            || t.getName().startsWith("Memory manager");

    }
}
