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

    public static boolean isSysVInit() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().runIgnoreExitCode("which service").isSuccess();
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
