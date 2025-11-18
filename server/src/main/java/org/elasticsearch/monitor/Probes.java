/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

public class Probes {
    private static final Logger logger = LogManager.getLogger(Probes.class);

    public static short getLoadAndScaleToPercent(Method method, OperatingSystemMXBean osMxBean) {
        logger.debug("Starting probe of method {} on osMxBean {}", method, osMxBean);
        if (method != null) {
            try {
                double load = (double) method.invoke(osMxBean);
                if (load >= 0) {
                    return (short) (load * 100);
                }
            } catch (Exception e) {
                logger.debug(() -> "failed to invoke method [" + method + "] on osMxBean [" + osMxBean + "]", e);
                return -1;
            }
        }
        logger.debug("Method is null. Returning default value.");
        return -1;
    }
}
