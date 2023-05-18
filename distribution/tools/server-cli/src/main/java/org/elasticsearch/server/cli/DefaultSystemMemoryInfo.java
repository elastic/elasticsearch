/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import com.sun.management.OperatingSystemMXBean;

import org.elasticsearch.core.SuppressForbidden;

import java.lang.management.ManagementFactory;
import java.util.function.LongSupplier;

/**
 * A {@link SystemMemoryInfo} which delegates to {@link OperatingSystemMXBean}.
 */
public final class DefaultSystemMemoryInfo implements SystemMemoryInfo {
    private final LongSupplier totalMemory;

    public DefaultSystemMemoryInfo() {
        this.totalMemory = getOSBeanMemoryGetter();
    }

    @SuppressForbidden(reason = "Using com.sun internals is the only way to query total system memory")
    private static LongSupplier getOSBeanMemoryGetter() {
        OperatingSystemMXBean bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return bean::getTotalMemorySize;
    }

    @Override
    public long availableSystemMemory() {
        return totalMemory.getAsLong();
    }
}
