/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import com.sun.management.OperatingSystemMXBean;

import org.elasticsearch.tools.java_version_checker.JavaVersion;
import org.elasticsearch.tools.java_version_checker.SuppressForbidden;

import java.lang.management.ManagementFactory;

/**
 * A {@link SystemMemoryInfo} which delegates to {@link OperatingSystemMXBean}.
 *
 * <p>Prior to JDK 14 {@link OperatingSystemMXBean} did not take into consideration container memory limits when reporting total system
 * memory. Therefore attempts to use this implementation on earlier JDKs will result in an {@link SystemMemoryInfoException}.
 */
@SuppressForbidden(reason = "Using com.sun internals is the only way to query total system memory")
public final class DefaultSystemMemoryInfo implements SystemMemoryInfo {
    private final OperatingSystemMXBean operatingSystemMXBean;

    public DefaultSystemMemoryInfo() {
        this.operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }

    @Override
    @SuppressWarnings("deprecation")
    public long availableSystemMemory() throws SystemMemoryInfoException {
        if (JavaVersion.majorVersion(JavaVersion.CURRENT) < 14) {
            throw new SystemMemoryInfoException("The minimum required Java version is 14 to use " + this.getClass().getName());
        }

        return operatingSystemMXBean.getTotalPhysicalMemorySize();
    }
}
