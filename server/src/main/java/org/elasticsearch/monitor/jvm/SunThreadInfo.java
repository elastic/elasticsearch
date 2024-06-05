/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

public class SunThreadInfo {

    private static final ThreadMXBean threadMXBean;
    private static final Method getThreadAllocatedBytes;
    private static final Method isThreadAllocatedMemorySupported;
    private static final Method isThreadAllocatedMemoryEnabled;

    private static final Logger logger = LogManager.getLogger(SunThreadInfo.class);
    public static final SunThreadInfo INSTANCE = new SunThreadInfo();

    static {
        threadMXBean = ManagementFactory.getThreadMXBean();
        getThreadAllocatedBytes = getMethod("getThreadAllocatedBytes", long.class);
        isThreadAllocatedMemorySupported = getMethod("isThreadAllocatedMemorySupported");
        isThreadAllocatedMemoryEnabled = getMethod("isThreadAllocatedMemoryEnabled");
    }

    public boolean isThreadAllocatedMemorySupported() {
        if (isThreadAllocatedMemorySupported == null) {
            return false;
        }

        try {
            return (boolean) isThreadAllocatedMemorySupported.invoke(threadMXBean);
        } catch (Exception e) {
            logger.warn("exception while invoke isThreadAllocatedMemorySupported", e);
            return false;
        }
    }

    public boolean isThreadAllocatedMemoryEnabled() {
        if (isThreadAllocatedMemoryEnabled == null) {
            return false;
        }

        try {
            return (boolean) isThreadAllocatedMemoryEnabled.invoke(threadMXBean);
        } catch (Exception e) {
            logger.warn("exception while invoke isThreadAllocatedMemoryEnabled", e);
            return false;
        }
    }

    public long getThreadAllocatedBytes(long id) {
        if (getThreadAllocatedBytes == null) {
            return 0;
        }

        if (isThreadAllocatedMemorySupported() == false || isThreadAllocatedMemoryEnabled() == false) {
            return 0;
        }

        if (id <= 0) {
            return 0;
        }

        try {
            long bytes = (long) getThreadAllocatedBytes.invoke(threadMXBean, id);
            assert bytes >= 0 : "OS reported a negative thread allocated size [" + bytes + "], thread id [" + id + "].";
            return Math.max(0, bytes);
        } catch (Exception e) {
            logger.warn("exception retrieving thread allocated memory", e);
            return 0;
        }
    }

    private static Method getMethod(String methodName, Class<?>... parameterTypes) {
        try {
            Method method = Class.forName("com.sun.management.ThreadMXBean").getMethod(methodName, parameterTypes);
            return method;
        } catch (Exception e) {
            // not available
            return null;
        }
    }
}
