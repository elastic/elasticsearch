/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.runner;

import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnectionException;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around the Gradle Tooling API that executes Gradle tasks in a
 * child build and exits with the build's result code. This provides a
 * programmatic entry point for CI tooling that needs to monitor task and
 * test status beyond what the CLI offers.
 */
public class GradleRunner {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println(
                "Usage: java -jar gradle-runner.jar [--project-dir <dir>] [--gradle-home <dir>] <task> [task...] [-- <gradle-args>]"
            );
            System.exit(1);
        }

        File projectDir = null;
        File gradleHome = null;
        List<String> tasks = new ArrayList<>();
        List<String> gradleArgs = new ArrayList<>();

        boolean parsingGradleArgs = false;
        for (int i = 0; i < args.length; i++) {
            if (parsingGradleArgs) {
                gradleArgs.add(args[i]);
                continue;
            }
            switch (args[i]) {
                case "--project-dir" -> {
                    if (++i >= args.length) {
                        System.err.println("--project-dir requires a value");
                        System.exit(1);
                    }
                    projectDir = new File(args[i]);
                }
                case "--gradle-home" -> {
                    if (++i >= args.length) {
                        System.err.println("--gradle-home requires a value");
                        System.exit(1);
                    }
                    gradleHome = new File(args[i]);
                }
                case "--" -> parsingGradleArgs = true;
                default -> tasks.add(args[i]);
            }
        }

        if (tasks.isEmpty()) {
            System.err.println("No tasks specified");
            System.exit(1);
        }

        if (projectDir == null) {
            projectDir = new File(System.getProperty("user.dir"));
        }

        GradleConnector connector = GradleConnector.newConnector().forProjectDirectory(projectDir);
        if (gradleHome != null) {
            connector.useInstallation(gradleHome);
        }

        int exitCode = 0;
        try (ProjectConnection connection = connector.connect()) {
            BuildLauncher launcher = connection.newBuild()
                .forTasks(tasks.toArray(String[]::new))
                .setStandardOutput(System.out)
                .setStandardError(System.err);

            if (gradleArgs.isEmpty() == false) {
                launcher.withArguments(gradleArgs);
            }

            launcher.run();
        } catch (GradleConnectionException e) {
            exitCode = 1;
        } catch (Exception e) {
            System.err.println("Gradle runner failed: " + e.getMessage());
            e.printStackTrace(System.err);
            exitCode = 2;
        }

        System.exit(exitCode);
    }
}
