/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

import java.util.Locale;

public class Systemd {
    private static final Logger logger = LogManager.getLogger(Systemd.class);

    private final SystemdLibrary lib;

    Systemd(SystemdLibrary lib) {
        this.lib = lib;
    }

    /**
     * Notify systemd that the process is ready.
     *
     * @throws RuntimeException on failure to notify systemd
     */
    public void notify_ready() {
        notify("READY=1", false);
    }

    public void notify_extend_timeout(long seconds) {
        notify("EXTEND_TIMEOUT_USEC=" + (seconds * 1000000), true);
    }

    public void notify_stopping() {
        notify("STOPPING=1", true);
    }

    private void notify(String state, boolean warnOnError) {
        int rc = lib.sd_notify(0, state);
        logger.trace("sd_notify({}, {}) returned [{}]", 0, state, rc);
        if (rc < 0) {
            String message = String.format(Locale.ROOT, "sd_notify(%d, %s) returned error [%d]", 0, state, rc);
            if (warnOnError) {
                logger.warn(message);
            } else {
                throw new RuntimeException(message);
            }
        }
    }
}
