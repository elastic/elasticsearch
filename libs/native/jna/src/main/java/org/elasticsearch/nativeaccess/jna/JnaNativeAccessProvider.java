/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.NativeAccessProvider;

public class JnaNativeAccessProvider extends NativeAccessProvider {
    // marker to determine if the JNA class files are available to the JVM
    static final boolean JNA_AVAILABLE;

    static {
        boolean v = false;
        try {
            // load one of the main JNA classes to see if the classes are available. this does not ensure that all native
            // libraries are available, only the ones necessary by JNA to function
            Class.forName("com.sun.jna.Native");
            v = true;
        } catch (ClassNotFoundException e) {
            //logger.warn("JNA not found. native methods will be disabled.", e);
        } catch (UnsatisfiedLinkError e) {
            //logger.warn("unable to load JNA native support library, native methods will be disabled.", e);
        }
        JNA_AVAILABLE = v;
    }

    @Override
    protected NativeAccess loadLinuxNativeAccess() {
        if (JNA_AVAILABLE) {
            return new JnaLinuxNativeAccess();
        }
        return new NoopNativeAccess();
    }

    @Override
    protected NativeAccess loadMacOSNativeAccess() {
        if (JNA_AVAILABLE) {
            return new JnaMacOSNativeAccess();
        }
        return new NoopNativeAccess();
    }

    @Override
    protected NativeAccess loadWindowsNativeAccess() {
        if (JNA_AVAILABLE) {
            return new JnaWindowsNativeAccess();
        }
        return new NoopNativeAccess();
    }
}
