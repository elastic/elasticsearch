/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TestEsExecutors {

    /**
     * Other than EsExecutors.daemonThreadFactory this doesn't follow the general naming pattern of ES threads:
     * {@code elasticsearch[<nodeName>][<executorName>][T#<threadNumber>]}.
     * This is sometimes desirable to easily distinguish test threads from ES threads.
     */
    public static ThreadFactory testOnlyDaemonThreadFactory(final String name) {
        assert name != null && name.isEmpty() == false;
        final AtomicInteger threadNumber = new AtomicInteger(1);
        final ThreadGroup group = Thread.currentThread().getThreadGroup();
        return runnable -> {
            Thread t = new Thread(group, runnable, name + "[T#" + threadNumber.getAndIncrement() + "]");
            t.setDaemon(true);
            return t;
        };
    }
}
