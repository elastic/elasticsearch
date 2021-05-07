/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;

public class SunThreadInfo {

    private static final ThreadMXBean threadMXBean;

    static {
        threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    }
    public static long getThreadAllocatedBytes(long id){
        if (id <= 0){
            return 0;
        }

        long bytes = 0;
        try {
            bytes=  threadMXBean.getThreadAllocatedBytes(id);
        }catch (UnsupportedOperationException ignored){
            // ignore if getThreadAllocatedBytes is unsupported
        }
        return bytes;
    }
}

