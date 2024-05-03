/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.FileUtils.slurp;

public class Platforms {
    public static final String OS_NAME = System.getProperty("os.name");
    public static final boolean LINUX = OS_NAME.startsWith("Linux");
    public static final boolean WINDOWS = OS_NAME.startsWith("Windows");
    public static final boolean DARWIN = OS_NAME.startsWith("Mac OS X");
    public static final PlatformAction NO_ACTION = () -> {};

    public static String getOsRelease() {
        if (LINUX) {
            return slurp(Paths.get("/etc/os-release"));
        } else {
            throw new RuntimeException("os-release is only supported on linux");
        }
    }

    public static boolean isDPKG() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().runIgnoreExitCode("which dpkg").isSuccess();
    }

    public static boolean isRPM() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().runIgnoreExitCode("which rpm").isSuccess();
    }

    public static boolean isSystemd() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().runIgnoreExitCode("which systemctl").isSuccess();
    }

    public static boolean isDocker() {
        return new Shell().runIgnoreExitCode("which docker").isSuccess();
    }

    public static void onWindows(PlatformAction action) throws Exception {
        if (WINDOWS) {
            action.run();
        }
    }

    public static void onLinux(PlatformAction action) throws Exception {
        if (LINUX) {
            action.run();
        }
    }

    public static void onRPM(PlatformAction action) throws Exception {
        if (isRPM()) {
            action.run();
        }
    }

    public static void onDPKG(PlatformAction action) throws Exception {
        if (isDPKG()) {
            action.run();
        }
    }

    /**
     * Essentially a Runnable, but we make the distinction so it's more clear that these are synchronous
     */
    @FunctionalInterface
    public interface PlatformAction {
        void run() throws Exception;
    }
}
