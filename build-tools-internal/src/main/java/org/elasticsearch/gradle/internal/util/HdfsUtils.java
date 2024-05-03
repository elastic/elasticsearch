/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util;

import org.elasticsearch.gradle.OS;
import org.gradle.api.Project;
import org.gradle.api.logging.Logging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HdfsUtils {

    public static boolean isHdfsFixtureSupported(Project project) {
        String projectPath = project.getProjectDir().getPath();
        if (isLegalHdfsPath(projectPath) == false) {
            Logging.getLogger(HdfsUtils.class).warn("hdfs Fixture unsupported since there are spaces in the path: '" + projectPath + "'");
            return false;
        }
        return (OS.current() != OS.WINDOWS) ? true : isHadoopWindowsInstallationAvailable();
    }

    private static boolean isHadoopWindowsInstallationAvailable() {
        // hdfs fixture will not start without hadoop native libraries on windows
        String nativePath = System.getenv("HADOOP_HOME");
        if (nativePath != null) {
            Path path = Paths.get(nativePath);
            if (Files.isDirectory(path)
                && Files.exists(path.resolve("bin").resolve("winutils.exe"))
                && Files.exists(path.resolve("bin").resolve("hadoop.dll"))
                && Files.exists(path.resolve("bin").resolve("hdfs.dll"))) {
                return true;
            } else {
                throw new IllegalStateException(
                    "HADOOP_HOME: " + path + " is invalid, does not contain hadoop native libraries in \\$HADOOP_HOME\\bin"
                );
            }
        }
        Logging.getLogger(HdfsUtils.class).warn("hdfs Fixture unsupported, please set HADOOP_HOME and put HADOOP_HOME\\bin in PATH");

        return false;
    }

    public static boolean isLegalHdfsPath(String path) {
        return path.contains(" ") == false;

    }
}
