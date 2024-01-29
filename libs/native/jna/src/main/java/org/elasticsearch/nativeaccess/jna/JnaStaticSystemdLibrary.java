/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Native;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class JnaStaticSystemdLibrary {
    private static final Logger logger = LogManager.getLogger(JnaStaticSystemdLibrary.class);

    public static final boolean loaded;

    static {
        boolean success = false;
        try {
            Native.register("libsystemd.so.0");
            success = true;
        } catch (UnsatisfiedLinkError e) {
            logger.warn("unable to link C library. native methods (mlockall) will be disabled.", e);
        }
        loaded = success;
    }

    static native int sd_notify(int unset_environment, String state);
}
