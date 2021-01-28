/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
