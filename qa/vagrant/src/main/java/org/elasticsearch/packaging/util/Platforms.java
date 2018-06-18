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

public class Platforms {
    public static final String OS_NAME = System.getProperty("os.name");
    public static final boolean LINUX = OS_NAME.startsWith("Linux");
    public static final boolean WINDOWS = OS_NAME.startsWith("Windows");

    public static boolean isDPKG() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().bashIgnoreExitCode("which dpkg").isSuccess();
    }

    public static boolean isAptGet() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().bashIgnoreExitCode("which apt-get").isSuccess();
    }

    public static boolean isRPM() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().bashIgnoreExitCode("which rpm").isSuccess();
    }

    public static boolean isYUM() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().bashIgnoreExitCode("which yum").isSuccess();
    }

    public static boolean isSystemd() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().bashIgnoreExitCode("which systemctl").isSuccess();
    }

    public static boolean isSysVInit() {
        if (WINDOWS) {
            return false;
        }
        return new Shell().bashIgnoreExitCode("which service").isSuccess();
    }
}
