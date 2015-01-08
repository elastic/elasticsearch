/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.jna;

import com.sun.jna.Native;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.jna.Kernel32Library.ConsoleCtrlHandler;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Locale;

/**
 *
 */
public class Natives {

    private static ESLogger logger = Loggers.getLogger(Natives.class);
    // Set to true, in case native mlockall call was successful
    public static boolean LOCAL_MLOCKALL = false;

    public static void tryMlockall() {
        int errno = Integer.MIN_VALUE;
        try {
            int result = CLibrary.mlockall(CLibrary.MCL_CURRENT);
            if (result != 0) {
                errno = Native.getLastError();
            } else {
                LOCAL_MLOCKALL = true;
            }
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by CLibrary, no need to repeat it
            return;
        }

        if (errno != Integer.MIN_VALUE) {
            if (errno == CLibrary.ENOMEM && System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux")) {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out."
                        + " Increase RLIMIT_MEMLOCK (ulimit).");
            } else if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("mac")) {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error " + errno);
            }
        }
    }

    public static void addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        // The console Ctrl handler is necessary on Windows platforms only.
        if (Constants.WINDOWS) {
            try {
                boolean result = Kernel32Library.getInstance().addConsoleCtrlHandler(handler);
                if (result) {
                    logger.debug("console ctrl handler correctly set");
                } else {
                    logger.warn("unknown error " + Native.getLastError() + " when adding console ctrl handler:");
                }
            } catch (NoClassDefFoundError e) {
                logger.warn("JNA not found: native methods and handlers will be disabled.");
            } catch (UnsatisfiedLinkError e) {
                // this will have already been logged by Kernel32Library, no need to repeat it
            }
        }
    }

}
