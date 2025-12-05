/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.util;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.Version;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.nio.file.Files;
import java.nio.file.Path;

public final class OsUtils {

    private static final Logger LOGGER = Logging.getLogger(OsUtils.class);

    private OsUtils() {}

    /**
      * OpenJDK 17 that we ship with for older ES distributions is incompatible with Ubuntu 24.04 and newer due to
      * a change in newer kernel versions that causes JVM crashes.
      * <p>
      * See <a href="https://github.com/oracle/graal/issues/4831">https://github.com/oracle/graal/issues/4831</a> that exposes a similar issue with GraalVM.
      * <p>
      * It can be reproduced using Jshell on Ubuntu 24.04+ with:
      * <pre>
      * jshell&gt; java.lang.management.ManagementFactory.getOperatingSystemMXBean()
      * |  Exception java.lang.NullPointerException: Cannot invoke "jdk.internal.platform.CgroupInfo.getMountPoint()" because "anyController" is null
      * </pre>
      * <p>
      * This method returns true if the given version of the JDK is known to be incompatible
      */
    public static boolean jdkIsIncompatibleWithOS(Version version) {
        return version.onOrAfter("7.17.0") && version.onOrBefore("8.10.4") && isUbuntu2404OrLater();
    }

    private static boolean isUbuntu2404OrLater() {
        try {
            if (OS.current() != OS.LINUX) {
                return false;
            }

            // try reading kernel info from System properties first to make this better testable
            String osKernelString = System.getProperty("os.version");
            Version kernelVersion = Version.fromString(osKernelString, Version.Mode.RELAXED);
            if (kernelVersion.onOrAfter("6.14.0")) {
                return true;
            }

            // Read /etc/os-release file to get distribution info
            Path osRelease = Path.of("/etc/os-release");
            if (Files.exists(osRelease) == false) {
                return false;
            }

            String content = Files.readString(osRelease);
            boolean isUbuntu = content.contains("ID=ubuntu");

            if (isUbuntu == false) {
                return false;
            }

            // Extract version
            String versionLine = content.lines().filter(line -> line.startsWith("VERSION_ID=")).findFirst().orElse("");

            if (versionLine.isEmpty()) {
                return false;
            }

            String version = versionLine.substring("VERSION_ID=".length()).replace("\"", "");
            String[] parts = version.split("\\.");

            if (parts.length >= 2) {
                int major = Integer.parseInt(parts[0]);
                int minor = Integer.parseInt(parts[1]);
                return major > 24 || (major == 24 && minor >= 4);
            }

            return false;
        } catch (Exception e) {
            LOGGER.debug("Failed to detect Ubuntu version", e);
            return false;
        }
    }

}
