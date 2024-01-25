/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.nativeaccess.lib.CLibrary;

class LinuxNativeAccess extends PosixNativeAccess {
    LinuxNativeAccess(CLibrary libc) {
        super(libc, 8, -1L);
    }

    @Override
    protected void logMemoryLimitInstructions() {
        // give specific instructions for the linux case to make it easy
        String user = System.getProperty("user.name");
        logger.warn("""
                        These can be adjusted by modifying /etc/security/limits.conf, for example:
                        \t# allow user '{}' mlockall
                        \t{} soft memlock unlimited
                        \t{} hard memlock unlimited""", user, user, user);
        logger.warn("If you are logged in interactively, you will have to re-login for the new limits to take effect.");
    }
}
