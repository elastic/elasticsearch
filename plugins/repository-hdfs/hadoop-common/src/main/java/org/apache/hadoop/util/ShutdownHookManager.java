/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.hadoop.util;

import java.util.concurrent.TimeUnit;

/**
 * A replacement for the ShutdownHookManager from hadoop.
 *
 * This class does not actually add a shutdown hook. Hadoop's shutdown hook
 * manager does not fail gracefully when it lacks security manager permissions
 * to add shutdown hooks. This implements the same api as the hadoop class, but
 * with no-ops.
 */
public class ShutdownHookManager {
    private static final ShutdownHookManager MGR = new ShutdownHookManager();

    public static ShutdownHookManager get() {
        return MGR;
    }

    private ShutdownHookManager() {}

    public void addShutdownHook(Runnable shutdownHook, int priority) {}

    public void addShutdownHook(Runnable shutdownHook, int priority, long timeout, TimeUnit unit) {}

    public boolean removeShutdownHook(Runnable shutdownHook) {
        return false;
    }

    public boolean hasShutdownHook(Runnable shutdownHook) {
        return false;
    }

    public boolean isShutdownInProgress() {
        return false;
    }

    public void clearShutdownHooks() {}
}
