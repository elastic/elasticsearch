/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.lang.management.ManagementFactory;

class JvmPid {

    private static final long PID;

    static long getPid() {
        return PID;
    }

    static {
        PID = initializePid();
    }

    private static long initializePid() {
        final String name = ManagementFactory.getRuntimeMXBean().getName();
        try {
            return Long.parseLong(name.split("@")[0]);
        } catch (final NumberFormatException e) {
            LogManager.getLogger(JvmPid.class).debug(new ParameterizedMessage("failed parsing PID from [{}]", name), e);
            return -1;
        }
    }

}
