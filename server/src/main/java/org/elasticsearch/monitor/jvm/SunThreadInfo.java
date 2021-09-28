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

import java.lang.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;

public class SunThreadInfo {

    private static final ThreadMXBean threadMXBean;
    private static final Method getThreadAllocatedBytes;
    private static final Method isThreadAllocatedMemorySupported;
    private static final Method isThreadAllocatedMemoryEnabled;

    private static final Logger logger = LogManager.getLogger(SunThreadInfo.class);

    static {
        threadMXBean = ManagementFactory.getThreadMXBean();
        getThreadAllocatedBytes = getMethod("getThreadAllocatedBytes", long.class);
        isThreadAllocatedMemorySupported = getMethod("isThreadAllocatedMemorySupported");
        isThreadAllocatedMemoryEnabled = getMethod("isThreadAllocatedMemoryEnabled");
    }

    public static boolean isThreadAllocatedMemorySupported() {
        if (isThreadAllocatedMemorySupported == null) {
            logger.warn("isThreadAllocatedMemorySupported is not available");
            return false;
        }

        try {
            return (boolean) isThreadAllocatedMemorySupported.invoke(threadMXBean);
        } catch (Exception e) {
            logger.warn("exception while invoke isThreadAllocatedMemorySupported", e);
            return false;
        }
    }

    public static boolean isThreadAllocatedMemoryEnabled() {
        if (isThreadAllocatedMemoryEnabled == null) {
            logger.warn("isThreadAllocatedMemoryEnabled is not available");
            return false;
        }

        try {
            return (boolean) isThreadAllocatedMemoryEnabled.invoke(threadMXBean);
        } catch (Exception e) {
            logger.warn("exception while invoke isThreadAllocatedMemoryEnabled", e);
            return false;
        }
    }

    public static long getThreadAllocatedBytes(long id) {
        if (getThreadAllocatedBytes == null) {
            logger.warn("getThreadAllocatedBytes is not available");
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
            if (bytes < 0) {
                logger.debug("OS reported a negative thread allocated size [{}]", bytes);
                return 0;
            }
            return bytes;
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
