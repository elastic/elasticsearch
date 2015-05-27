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

package org.elasticsearch.bootstrap;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Locale;

import static org.elasticsearch.bootstrap.JNAKernel32Library.SizeT;

/**
 * This class performs the actual work with JNA and library bindings to call native methods. It should only be used after
 * we are sure that the JNA classes are available to the JVM
 */
class JNANatives {

    private static final ESLogger logger = Loggers.getLogger(JNANatives.class);

    // Set to true, in case native mlockall call was successful
    public static boolean LOCAL_MLOCKALL = false;

    static void tryMlockall() {
        int errno = Integer.MIN_VALUE;
        try {
            int result = JNACLibrary.mlockall(JNACLibrary.MCL_CURRENT);
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
            if (errno == JNACLibrary.ENOMEM && System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux")) {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out."
                        + " Increase RLIMIT_MEMLOCK (ulimit).");
            } else if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("mac")) {
                // OS X allows mlockall to be called, but always returns an error
                logger.warn("Unknown mlockall error " + errno);
            }
        }
    }

    static void tryVirtualLock() {
        JNAKernel32Library kernel = JNAKernel32Library.getInstance();
        Pointer process = null;
        try {
            process = kernel.GetCurrentProcess();
            // By default, Windows limits the number of pages that can be locked.
            // Thus, we need to first increase the working set size of the JVM by
            // the amount of memory we wish to lock, plus a small overhead (1MB).
            SizeT size = new SizeT(JvmInfo.jvmInfo().getMem().getHeapInit().getBytes() + (1024 * 1024));
            if (!kernel.SetProcessWorkingSetSize(process, size, size)) {
                logger.warn("Unable to lock JVM memory. Failed to set working set size. Error code " + Native.getLastError());
            } else {
                JNAKernel32Library.MemoryBasicInformation memInfo = new JNAKernel32Library.MemoryBasicInformation();
                long address = 0;
                while (kernel.VirtualQueryEx(process, new Pointer(address), memInfo, memInfo.size()) != 0) {
                    boolean lockable = memInfo.State.longValue() == JNAKernel32Library.MEM_COMMIT
                            && (memInfo.Protect.longValue() & JNAKernel32Library.PAGE_NOACCESS) != JNAKernel32Library.PAGE_NOACCESS
                            && (memInfo.Protect.longValue() & JNAKernel32Library.PAGE_GUARD) != JNAKernel32Library.PAGE_GUARD;
                    if (lockable) {
                        kernel.VirtualLock(memInfo.BaseAddress, new SizeT(memInfo.RegionSize.longValue()));
                    }
                    // Move to the next region
                    address += memInfo.RegionSize.longValue();
                }
                LOCAL_MLOCKALL = true;
            }
        } catch (UnsatisfiedLinkError e) {
            // this will have already been logged by Kernel32Library, no need to repeat it
        } finally {
            if (process != null) {
                kernel.CloseHandle(process);
            }
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
                    logger.warn("unknown error " + Native.getLastError() + " when adding console ctrl handler:");
                }
            } catch (UnsatisfiedLinkError e) {
                // this will have already been logged by Kernel32Library, no need to repeat it
            }
        }
    }

}
