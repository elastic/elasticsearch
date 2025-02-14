/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.nativeaccess.lib.Kernel32Library;

/**
 * Native functions specific to the Windows operating system.
 */
public class WindowsFunctions {
    private static final Logger logger = LogManager.getLogger(Systemd.class);

    private final Kernel32Library kernel;

    WindowsFunctions(Kernel32Library kernel) {
        this.kernel = kernel;
    }

    /**
     * Retrieves the short path form of the specified path.
     *
     * @param path the path
     * @return the short path name, or the original path name if unsupported or unavailable
     */
    public String getShortPathName(String path) {
        String longPath = "\\\\?\\" + path;
        // first we get the length of the buffer needed
        final int length = kernel.GetShortPathNameW(longPath, null, 0);
        if (length == 0) {
            logger.warn("failed to get short path name: {}", kernel.GetLastError());
            return path;
        }
        final char[] shortPath = new char[length];
        // knowing the length of the buffer, now we get the short name
        if (kernel.GetShortPathNameW(longPath, shortPath, length) > 0) {
            assert shortPath[length - 1] == '\0';
            return new String(shortPath, 0, length - 1);
        } else {
            logger.warn("failed to get short path name: {}", kernel.GetLastError());
            return path;
        }
    }

    /**
     * Adds a Console Ctrl Handler for Windows. On non-windows this is a noop.
     *
     * @return true if the handler is correctly set
     */
    public boolean addConsoleCtrlHandler(ConsoleCtrlHandler handler) {
        return kernel.SetConsoleCtrlHandler(dwCtrlType -> {
            if (logger.isDebugEnabled()) {
                logger.debug("console control handler received event [{}]", dwCtrlType);
            }
            return handler.handle(dwCtrlType);
        }, true);
    }

    /**
     * Windows callback for console events
     *
     * @see <a href="http://msdn.microsoft.com/en-us/library/windows/desktop/ms683242%28v=vs.85%29.aspx">HandlerRoutine docs</a>
     */
    public interface ConsoleCtrlHandler {

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
