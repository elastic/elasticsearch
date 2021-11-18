/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.systemd;

import com.sun.jna.Native;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Provides access to the native method sd_notify from libsystemd.
 */
class Libsystemd {

    static {
        AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            Native.register(Libsystemd.class, "libsystemd.so.0");
            return null;
        });
    }

    /**
     * Notify systemd of state changes.
     *
     * @param unset_environment if non-zero, the NOTIFY_SOCKET environment variable will be unset before returning and further calls to
     *                          sd_notify will fail
     * @param state             a new-line separated list of variable assignments; some assignments are understood directly by systemd
     * @return a negative error code on failure, and positive if status was successfully sent
     */
    static native int sd_notify(int unset_environment, String state);

}
