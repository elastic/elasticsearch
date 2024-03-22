/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;

import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

class JnaSystemdLibrary implements SystemdLibrary {
    private interface NativeFunctions extends Library {
        int sd_notify(int unset_environment, String state);
    }

    private final NativeFunctions functions;

    JnaSystemdLibrary() {
        this.functions = Native.load("libsystemd.so.0", NativeFunctions.class);
    }

    @Override
    public int sd_notify(int unset_environment, String state) {
        return functions.sd_notify(unset_environment, state);
    }
}
