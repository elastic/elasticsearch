/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;

/**
 * This class performs the actual work with JNA and library bindings to call native methods. It should only be used after
 * we are sure that the JNA classes are available to the JVM
 */
class JNANatives {

    /** no instantiation */
    private JNANatives() {}

    private static final Logger logger = LogManager.getLogger(JNANatives.class);

    // Set to true, in case native system call filter install was successful
    static boolean LOCAL_SYSTEM_CALL_FILTER = false;
    // Set to true, in case policy can be applied to all threads of the process (even existing ones)
    // otherwise they are only inherited for new threads (ES app threads)
    static boolean LOCAL_SYSTEM_CALL_FILTER_ALL = false;

    static void tryInstallSystemCallFilter(Path tmpFile) {
        try {
            int ret = SystemCallFilter.init(tmpFile);
            LOCAL_SYSTEM_CALL_FILTER = true;
            if (ret == 1) {
                LOCAL_SYSTEM_CALL_FILTER_ALL = true;
            }
        } catch (Exception e) {
            // this is likely to happen unless the kernel is newish, its a best effort at the moment
            // so we log stacktrace at debug for now...
            if (logger.isDebugEnabled()) {
                logger.debug("unable to install syscall filter", e);
            }
            logger.warn("unable to install syscall filter: ", e);
        }
    }

}
