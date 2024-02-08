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
import org.elasticsearch.nativeaccess.lib.NativeLibraryProvider;

class NativeAccessHolder {

    protected static final Logger logger = LogManager.getLogger(NativeAccess.class);

    static final NativeAccess INSTANCE;

    static {
        var libProvider = NativeLibraryProvider.instance();
        var os = System.getProperty("os.name");

        AbstractNativeAccess inst = null;
        try {
            if (os.startsWith("Linux")) {
                inst = new LinuxNativeAccess(libProvider);
            } else if (os.startsWith("Mac OS")) {
                inst = new MacNativeAccess(libProvider);
            } else if (os.startsWith("Windows")) {
                inst = new WindowsNativeAccess(libProvider);
            } else {
                logger.warn("Unsupported OS [" + os + "]. Native methods will be disabled.");
            }
        } catch (LinkageError e) {
            logger.warn("Unable to load native provider. Native methods will be disabled.", e);
        }
        if (inst == null) {
            inst = new NoopNativeAccess();
        } else {
            logger.info("Using [" + libProvider.getName() + "] native provider and native methods for [" + inst.getName() + "]");
        }
        INSTANCE = inst;
    }
}
