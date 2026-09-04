/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;

public class FakeTimeThreadPool extends TestThreadPool {

    private long currentTimeInMillis;
    private final long absoluteTimeOffset;

    public FakeTimeThreadPool() {
        this("test", randomLong());
    }

    @SuppressWarnings("this-escape")
    public FakeTimeThreadPool(String name, long absoluteTimeOffset, ExecutorBuilder<?>... customBuilders) {
        super(name, customBuilders);
        this.absoluteTimeOffset = absoluteTimeOffset;
        stopCachedTimeThread();
        setRandomTime();
    }

    @Override
    public long relativeTimeInMillis() {
        return currentTimeInMillis;
    }

    @Override
    public long absoluteTimeInMillis() {
        return currentTimeInMillis + absoluteTimeOffset;
    }

    public void setCurrentTimeInMillis(long currentTimeInMillis) {
        this.currentTimeInMillis = currentTimeInMillis;
    }

    public void setRandomTime() {
        // absolute time needs to be nonnegative
        currentTimeInMillis = randomNonNegativeLong() - absoluteTimeOffset;
    }
}
