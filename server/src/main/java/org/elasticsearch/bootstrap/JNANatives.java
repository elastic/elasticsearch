/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.nio.file.Path;

import static org.elasticsearch.bootstrap.JNAKernel32Library.SizeT;

/**
 * This class performs the actual work with JNA and library bindings to call native methods. It should only be used after
 * we are sure that the JNA classes are available to the JVM
 */
class JNANatives {

    /** no instantiation */
    private JNANatives() {}

    private static final Logger logger = LogManager.getLogger(JNANatives.class);

    // Set to true, in case native mlockall call was successful
    static boolean LOCAL_MLOCKALL = false;
    // Set to true, in case native system call filter install was successful
    static boolean LOCAL_SYSTEM_CALL_FILTER = false;
    // Set to true, in case policy can be applied to all threads of the process (even existing ones)
    // otherwise they are only inherited for new threads (ES app threads)
    static boolean LOCAL_SYSTEM_CALL_FILTER_ALL = false;
    // set to the maximum number of threads that can be created for
    // the user ID that owns the running Elasticsearch process
    static long MAX_NUMBER_OF_THREADS = -1;

    static long MAX_SIZE_VIRTUAL_MEMORY = Long.MIN_VALUE;

    static long MAX_FILE_SIZE = Long.MIN_VALUE;

    static String rlimitToString(long value) {
        assert Constants.LINUX || Constants.MAC_OS_X;
        if (value == JNACLibrary.RLIM_INFINITY) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name (or the original path if getting the short path name fails for any reason)
     */
    static String getShortPathName(String path) {
        assert Constants.WINDOWS;
        try {
            final WString longPath = new WString("\\\\?\\" + path);
            // first we get the length of the buffer needed
            final int length = JNAKernel32Library.getInstance().GetShortPathNameW(longPath, null, 0);
            if (length == 0) {
                logger.warn("failed to get short path name: {}", Native.getLastError());
                return path;
            }
            final char[] shortPath = new char[length];
            // knowing the length of the buffer, now we get the short name
            if (JNAKernel32Library.getInstance().GetShortPathNameW(longPath, shortPath, length) > 0) {
                return Native.toString(shortPath);
            } else {
                logger.warn("failed to get short path name: {}", Native.getLastError());
                return path;
            }
        } catch (final UnsatisfiedLinkError e) {
            return path;
        }
    }

    static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        // The console Ctrl handler is necessary on Windows platforms only.
        if (Constants.WINDOWS) {
            try {
                boolean result = JNAKernel32Library.getInstance().addConsoleCtrlHandler(handler);
                if (result) {
                    logger.debug("console ctrl handler correctly set");
                } else {
                    logger.warn("unknown error {} when adding console ctrl handler", Native.getLastError());
                }
            } catch (UnsatisfiedLinkError e) {
                // this will have already been logged by Kernel32Library, no need to repeat it
            }
        }
    }

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
