/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.Version;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.gradle.internal.util.JavaUtil.getJavaHome;

/**
 * By registering bwc tasks via this extension we can support declaring custom bwc tasks from the build script
 * without relying on groovy closures and sharing common logic for tasks created by the BwcSetup plugin already.
 */
public class BwcSetupExtension {

    private static final String MINIMUM_COMPILER_VERSION_PATH = "src/main/resources/minimumCompilerVersion";
    private static final Version BUILD_TOOL_MINIMUM_VERSION = Version.fromString("7.14.0");
    private final Project project;
    private final Provider<BwcVersions.UnreleasedVersionInfo> unreleasedVersionInfo;
    private final Provider<InternalDistributionBwcSetupPlugin.BwcTaskThrottle> bwcTaskThrottleProvider;

    private Provider<File> checkoutDir;

    public BwcSetupExtension(
        Project project,
        Provider<BwcVersions.UnreleasedVersionInfo> unreleasedVersionInfo,
        Provider<InternalDistributionBwcSetupPlugin.BwcTaskThrottle> bwcTaskThrottleProvider,
        Provider<File> checkoutDir
    ) {
        this.project = project;
        this.unreleasedVersionInfo = unreleasedVersionInfo;
        this.bwcTaskThrottleProvider = bwcTaskThrottleProvider;
        this.checkoutDir = checkoutDir;
    }

    TaskProvider<LoggedExec> bwcTask(String name, Action<LoggedExec> configuration) {
        return createRunBwcGradleTask(project, name, configuration);
    }

    private TaskProvider<LoggedExec> createRunBwcGradleTask(Project project, String name, Action<LoggedExec> configAction) {
        return project.getTasks().register(name, LoggedExec.class, loggedExec -> {
            loggedExec.dependsOn("checkoutBwcBranch");
            loggedExec.usesService(bwcTaskThrottleProvider);
            loggedExec.setSpoolOutput(true);
            loggedExec.getWorkingDir().set(checkoutDir.get());
            // Execution time so that the checkouts are available
            loggedExec.getEnvironment().put("JAVA_HOME", project.provider(() -> {
                String compilerVersionInfoPath = minimumCompilerVersionPath(unreleasedVersionInfo.get().version());
                String minimumCompilerVersion = readFromFile(new File(checkoutDir.get(), compilerVersionInfoPath));
                return getJavaHome(Integer.parseInt(minimumCompilerVersion));
            }));

            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                loggedExec.getExecutable().set("cmd");
                loggedExec.getArgs().addAll("/C", "call", new File(checkoutDir.get(), "gradlew").toString());
            } else {
                loggedExec.getExecutable().set(new File(checkoutDir.get(), "gradlew").toString());
            }

            loggedExec.getArgs().addAll("-g", project.getGradle().getGradleUserHomeDir().toString());
            if (project.getGradle().getStartParameter().isOffline()) {
                loggedExec.getArgs().add("--offline");
            }
            // TODO resolve
            String buildCacheUrl = System.getProperty("org.elasticsearch.build.cache.url");
            if (buildCacheUrl != null) {
                loggedExec.getArgs().add("-Dorg.elasticsearch.build.cache.url=" + buildCacheUrl);
            }

            loggedExec.getArgs().add("-Dbuild.snapshot=true");
            loggedExec.getArgs().add("-Dscan.tag.NESTED");
            final LogLevel logLevel = project.getGradle().getStartParameter().getLogLevel();
            List<LogLevel> nonDefaultLogLevels = Arrays.asList(LogLevel.QUIET, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG);
            if (nonDefaultLogLevels.contains(logLevel)) {
                loggedExec.getArgs().add("--" + logLevel.name().toLowerCase(Locale.ENGLISH));
            }
            final String showStacktraceName = project.getGradle().getStartParameter().getShowStacktrace().name();
            assert Arrays.asList("INTERNAL_EXCEPTIONS", "ALWAYS", "ALWAYS_FULL").contains(showStacktraceName);
            if (showStacktraceName.equals("ALWAYS")) {
                loggedExec.getArgs().add("--stacktrace");
            } else if (showStacktraceName.equals("ALWAYS_FULL")) {
                loggedExec.getArgs().add("--full-stacktrace");
            }
            if (project.getGradle().getStartParameter().isParallelProjectExecutionEnabled()) {
                loggedExec.getArgs().add("--parallel");
            }
            loggedExec.getOutputIndenting().set(unreleasedVersionInfo.get().version().toString());
            configAction.execute(loggedExec);
        });
    }

    private String minimumCompilerVersionPath(Version bwcVersion) {
        return (bwcVersion.onOrAfter(BUILD_TOOL_MINIMUM_VERSION))
            ? "build-tools-internal/" + MINIMUM_COMPILER_VERSION_PATH
            : "buildSrc/" + MINIMUM_COMPILER_VERSION_PATH;
    }

    private static String readFromFile(File file) {
        try {
            return FileUtils.readFileToString(file).trim();
        } catch (IOException ioException) {
            throw new GradleException("Cannot read java properties file.", ioException);
        }
    }
}
