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

public abstract class NativeAccess {
    protected static final Logger logger = LogManager.getLogger(NativeAccess.class);

    private static class Holder {
        private static final NativeAccess INSTANCE;
        static {
            var libraryProvider = NativeLibraryProvider.getInstance();
            logger.info("Using native provider: " + libraryProvider.getClass().getSimpleName());
            var os = System.getProperty("os.name");
            NativeAccess inst = null;
            try {
                if (os.startsWith("Linux")) {
                    inst = new LinuxNativeAccess(libraryProvider);
                } else if (os.startsWith("Mac OS")) {
                    inst = new MacNativeAccess(libraryProvider);
                } else if (os.startsWith("Windows")) {
                    inst = new WindowsNativeAccess();
                } else {
                    logger.warn("Unsupported OS " + os + ". Native methods will be disabled.");
                }
            } catch (LinkageError e) {
                logger.warn("Unable to load native provider. Native methods will be disabled.", e);
            }
            if (inst == null) {
                inst = new NoopNativeAccess();
            }
            INSTANCE = inst;
        }
    }
    public static NativeAccess instance() {
        return Holder.INSTANCE;
    }

    public abstract boolean definitelyRunningAsRoot();

    public abstract void tryLockMemory();
    public abstract boolean isMemoryLocked();

    /*public abstract void tryInstallSystemCallFilter(Path tmpFile);
    public abstract boolean isSystemCallFilterInstalled();*/

    public void trySetMaxNumberOfThreads() {}

    public long getMaxNumberOfThreads() {
        return -1;
    }

    public void trySetMaxVirtualMemorySize() {}

    public long getMaxVirtualMemorySize() {
        return Long.MIN_VALUE;
    }

    public void trySetMaxFileSize() {}

    public long getMaxFileSize() {
        return Long.MIN_VALUE;
    }
    /*



    String getShortPathName(String path);
    void addConsoleCtrlHandler(ConsoleCtrlHandler handler);



    int preallocate(int fd, long offset, long length);
*/
    /**
     * Windows callback for console events
     */
    interface ConsoleCtrlHandler {

        int CTRL_CLOSE_EVENT = 2;

        /**
         * Handles the Ctrl event.
         *
         * @param code the code corresponding to the Ctrl sent.
         * @return true if the handler processed the event, false otherwise. If false, the next handler will be called.
         */
        boolean handle(int code);
    }
}
