/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The Netty object cleaner thread is not closeable and it does not terminate in a timely manner. This means that thread leak control in
 * tests will fail test suites when the object cleaner thread has not terminated. Since there is not a reliable way to terminate this
 * thread we instead filter it out of thread leak control.
 */
public class ObjectCleanerThreadThreadFilter implements ThreadFilter {

    @Override
    public boolean reject(final Thread t) {
        // TODO: replace with constant from Netty when https://github.com/netty/netty/pull/8014 is integrated
        return "ObjectCleanerThread".equals(t.getName());
    }

}
