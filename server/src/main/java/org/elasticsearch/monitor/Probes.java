/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor;

import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

public class Probes {
    public static short getLoadAndScaleToPercent(Method method, OperatingSystemMXBean osMxBean) {
        if (method != null) {
            try {
                double load = (double) method.invoke(osMxBean);
                if (load >= 0) {
                    return (short) (load * 100);
                }
            } catch (Exception e) {
                return -1;
            }
        }
        return -1;
    }
}
