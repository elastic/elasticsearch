/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

public interface NativeAccess {
    static NativeAccess instance() {
        return NativeAccessHolder.INSTANCE;
    }

    boolean definitelyRunningAsRoot();

    void tryLockMemory();
    boolean isMemoryLocked();

    /*public abstract void tryInstallSystemCallFilter(Path tmpFile);
    public abstract boolean isSystemCallFilterInstalled();*/

    void trySetMaxNumberOfThreads();

    long getMaxNumberOfThreads();

    void trySetMaxVirtualMemorySize();

    long getMaxVirtualMemorySize();

    void trySetMaxFileSize();

    long getMaxFileSize();
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
