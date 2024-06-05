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
import org.elasticsearch.common.ReferenceDocs;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Locale;

/**
 * The Natives class is a wrapper class that checks if the classes necessary for calling native methods are available on
 * startup. If they are not available, this class will avoid calling code that loads these classes.
 */
final class Natives {
    /** no instantiation */
    private Natives() {}

    private static final Logger logger = LogManager.getLogger(Natives.class);

    // marker to determine if the JNA class files are available to the JVM
    static final boolean JNA_AVAILABLE;

    static {
        boolean v = false;
        try {
            // load one of the main JNA classes to see if the classes are available. this does not ensure that all native
            // libraries are available, only the ones necessary by JNA to function
            MethodHandles.publicLookup().ensureInitialized(com.sun.jna.Native.class);
            v = true;
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (UnsatisfiedLinkError e) {
            logger.warn(
                String.format(
                    Locale.ROOT,
                    "unable to load JNA native support library, native methods will be disabled. See %s",
                    ReferenceDocs.EXECUTABLE_JNA_TMPDIR
                ),
                e
            );
        }
        JNA_AVAILABLE = v;
    }

    static void tryInstallSystemCallFilter(Path tmpFile) {
        if (JNA_AVAILABLE == false) {
            logger.warn("cannot install system call filter because JNA is not available");
            return;
        }
        JNANatives.tryInstallSystemCallFilter(tmpFile);
    }

    static boolean isSystemCallFilterInstalled() {
        if (JNA_AVAILABLE == false) {
            return false;
        }
        return JNANatives.LOCAL_SYSTEM_CALL_FILTER;
    }

}
