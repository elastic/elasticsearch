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
import org.gradle.tooling.CancellationTokenSource;
import org.gradle.tooling.GradleConnectionException;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.gradle.tooling.events.OperationType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper around the Gradle Tooling API that executes Gradle tasks in a child build,
 * monitors task and test status, handles GCP Spot VM preemption gracefully, and exits
 * with the appropriate exit code.
 *
 * <p>When {@code GCP_PREEMPTION_WATCHDOG=true} is set, the runner polls the GCP metadata
 * server for preemption signals. On preemption, it cancels the build via the Tooling API's
 * cancellation token, force-kills descendant processes, writes a status report, and exits
 * with a configurable preemption exit code (default 47).
 */
public class GradleRunner {

    private static final int EXIT_BUILD_FAILURE = 1;
    private static final int EXIT_RUNNER_ERROR = 2;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java -jar gradle-runner.jar [--project-dir <dir>] [--gradle-home <dir>] -- <gradle-args>");
            System.exit(1);
        }

        File projectDir = null;
        File gradleHome = null;
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
                default -> gradleArgs.add(args[i]);
            }
        }

        if (gradleArgs.isEmpty()) {
            System.err.println("No gradle arguments specified");
            System.exit(1);
        }

        if (projectDir == null) {
            projectDir = new File(System.getProperty("user.dir"));
        } else {
            projectDir = projectDir.getAbsoluteFile();
        }

        GcpPreemptionWatchdog.start();

        CancellationTokenSource tokenSource = GradleConnector.newCancellationTokenSource();
        BuildCanceller canceller = new BuildCanceller(tokenSource, projectDir);
        canceller.install();

        TaskTracker tracker = new TaskTracker(canceller);

        GradleConnector connector = GradleConnector.newConnector().forProjectDirectory(projectDir);
        if (gradleHome != null) {
            connector.useInstallation(gradleHome);
        } else {
            connector.useBuildDistribution();
        }

        int exitCode = 0;
        try (ProjectConnection connection = connector.connect()) {
            BuildLauncher launcher = connection.newBuild()
                .withArguments(gradleArgs)
                .setStandardOutput(System.out)
                .setStandardError(System.err)
                .withCancellationToken(tokenSource.token())
                .addProgressListener(tracker, OperationType.TASK, OperationType.TEST);

            launcher.run();
        } catch (GradleConnectionException e) {
            exitCode = EXIT_BUILD_FAILURE;
        } catch (Exception e) {
            System.err.println("Gradle runner failed: " + e.getMessage());
            e.printStackTrace(System.err);
            exitCode = EXIT_RUNNER_ERROR;
        }

        // Kill any remaining worker processes after the build has finished and the daemon
        // has had a chance to collect results and upload the build scan.
        if (canceller.isCancelled()) {
            // BuildCanceller.killRemainingWorkers(); // TODO
        }

        writeStatusReport(tracker, projectDir);

        if (GcpPreemptionWatchdog.isPreempted()) {
            int preemptionExitCode = getPreemptionExitCode();
            boolean hadRealFailures = canceller.isCancelled() && tracker.hadFailuresBeforePreemption();
            System.out.println(
                "[gcp-preemption-watchdog] build was preempted"
                    + (hadRealFailures ? " (had failures before preemption)" : "")
                    + "; exiting with code "
                    + preemptionExitCode
            );
            System.exit(preemptionExitCode);
        }

        System.exit(exitCode);
    }

    private static void writeStatusReport(TaskTracker tracker, File projectDir) {
        try {
            File reportFile = new File(projectDir, "build/task-status.json");
            StatusReport report = tracker.buildReport();
            report.writeTo(reportFile);
        } catch (Exception e) {
            System.err.println("Failed to write task status report: " + e.getMessage());
        }
    }

    private static int getPreemptionExitCode() {
        String envCode = System.getenv("GCP_PREEMPTION_EXIT_CODE");
        if (envCode != null) {
            try {
                return Integer.parseInt(envCode);
            } catch (NumberFormatException e) {
                // fall through to default
            }
        }
        return 47;
    }
}
