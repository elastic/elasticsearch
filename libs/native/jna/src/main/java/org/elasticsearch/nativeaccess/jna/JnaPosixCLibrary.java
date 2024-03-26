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

import org.elasticsearch.nativeaccess.lib.PosixCLibrary;

class JnaPosixCLibrary implements PosixCLibrary {

    private interface NativeFunctions extends Library {
        int geteuid();
    }

    private final NativeFunctions functions;

    JnaPosixCLibrary() {
        this.functions = Native.load("c", NativeFunctions.class);
    }

    @Override
    public int geteuid() {
        return functions.geteuid();
    }
}
