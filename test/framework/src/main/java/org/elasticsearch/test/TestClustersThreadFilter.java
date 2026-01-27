/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 *  When using shared test clusters we launch processes that persist across test suites. This filter is used to ignore those in that case.
 */
public class TestClustersThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().endsWith("-log-forwarder") || t.getName().contains("node-executor");
    }
}
