/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import java.nio.file.Path;

public interface NativeAccess {
    static NativeAccess instance() {
        return NativeAccessProvider.getNativeAccess();
    }

    String getDummyString();
    /*
    void tryLockMemory();
    boolean isMemoryLocked();

    boolean definitelyRunningAsRoot();

    String getShortPathName(String path);
    void addConsoleCtrlHandler(ConsoleCtrlHandler handler);

    void tryInstallSystemCallFilter(Path tmpFile);
    boolean isSystemCallFilterInstalled();

    void trySetMaxNumberOfThreads();
    void trySetMaxSizeVirtualMemory();
    void trySetMaxFileSize();

    int preallocate(int fd, long offset, long length);
    int getLastError();
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
