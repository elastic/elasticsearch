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
import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.gradle.util.JavaUtil.getJavaHome;

/**
 * By registering bwc tasks via this extension we can support declaring custom bwc tasks from the build script
 * without relying on groovy closures and sharing common logic for tasks created by the BwcSetup plugin already.
 * */
public class BwcSetupExtension {

    private final Project project;

    private final Provider<BwcVersions.UnreleasedVersionInfo> unreleasedVersionInfo;
    private Provider<File> checkoutDir;

    public BwcSetupExtension(
        Project project,
        Provider<BwcVersions.UnreleasedVersionInfo> unreleasedVersionInfo,
        Provider<File> checkoutDir
    ) {
        this.project = project;
        this.unreleasedVersionInfo = unreleasedVersionInfo;
        this.checkoutDir = checkoutDir;
    }

    TaskProvider<LoggedExec> bwcTask(String name, Action<LoggedExec> configuration) {
        return createRunBwcGradleTask(project, name, configuration);
    }

    private TaskProvider<LoggedExec> createRunBwcGradleTask(Project project, String name, Action<LoggedExec> configAction) {
        return project.getTasks().register(name, LoggedExec.class, loggedExec -> {
            // TODO revisit
            loggedExec.dependsOn("checkoutBwcBranch");
            loggedExec.setSpoolOutput(true);
            loggedExec.setWorkingDir(checkoutDir.get());
            loggedExec.doFirst(t -> {
                // Execution time so that the checkouts are available
                String javaVersionsString = readFromFile(new File(checkoutDir.get(), ".ci/java-versions.properties"));
                loggedExec.environment(
                    "JAVA_HOME",
                    getJavaHome(
                        Integer.parseInt(
                            javaVersionsString.lines()
                                .filter(l -> l.trim().startsWith("ES_BUILD_JAVA="))
                                .map(l -> l.replace("ES_BUILD_JAVA=java", "").trim())
                                .map(l -> l.replace("ES_BUILD_JAVA=openjdk", "").trim())
                                .collect(Collectors.joining("!!"))
                        )
                    )
                );
                loggedExec.environment(
                    "RUNTIME_JAVA_HOME",
                    getJavaHome(
                        Integer.parseInt(
                            javaVersionsString.lines()
                                .filter(l -> l.trim().startsWith("ES_RUNTIME_JAVA="))
                                .map(l -> l.replace("ES_RUNTIME_JAVA=java", "").trim())
                                .map(l -> l.replace("ES_RUNTIME_JAVA=openjdk", "").trim())
                                .collect(Collectors.joining("!!"))
                        )
                    )
                );
            });

            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                loggedExec.executable("cmd");
                loggedExec.args("/C", "call", new File(checkoutDir.get(), "gradlew").toString());
            } else {
                loggedExec.executable(new File(checkoutDir.get(), "gradlew").toString());
            }
            if (project.getGradle().getStartParameter().isOffline()) {
                loggedExec.args("--offline");
            }
            // TODO resolve
            String buildCacheUrl = System.getProperty("org.elasticsearch.build.cache.url");
            if (buildCacheUrl != null) {
                loggedExec.args("-Dorg.elasticsearch.build.cache.url=" + buildCacheUrl);
            }

            loggedExec.args("-Dbuild.snapshot=true");
            loggedExec.args("-Dscan.tag.NESTED");
            final LogLevel logLevel = project.getGradle().getStartParameter().getLogLevel();
            List<LogLevel> nonDefaultLogLevels = Arrays.asList(LogLevel.QUIET, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG);
            if (nonDefaultLogLevels.contains(logLevel)) {
                loggedExec.args("--" + logLevel.name().toLowerCase(Locale.ENGLISH));
            }
            final String showStacktraceName = project.getGradle().getStartParameter().getShowStacktrace().name();
            assert Arrays.asList("INTERNAL_EXCEPTIONS", "ALWAYS", "ALWAYS_FULL").contains(showStacktraceName);
            if (showStacktraceName.equals("ALWAYS")) {
                loggedExec.args("--stacktrace");
            } else if (showStacktraceName.equals("ALWAYS_FULL")) {
                loggedExec.args("--full-stacktrace");
            }
            if (project.getGradle().getStartParameter().isParallelProjectExecutionEnabled()) {
                loggedExec.args("--parallel");
            }
            loggedExec.setStandardOutput(new IndentingOutputStream(System.out, unreleasedVersionInfo.get().version));
            loggedExec.setErrorOutput(new IndentingOutputStream(System.err, unreleasedVersionInfo.get().version));
            configAction.execute(loggedExec);
        });
    }

    private static class IndentingOutputStream extends OutputStream {

        public final byte[] indent;
        private final OutputStream delegate;

        IndentingOutputStream(OutputStream delegate, Object version) {
            this.delegate = delegate;
            indent = (" [" + version + "] ").getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void write(int b) throws IOException {
            int[] arr = { b };
            write(arr, 0, 1);
        }

        public void write(int[] bytes, int offset, int length) throws IOException {
            for (int i = 0; i < bytes.length; i++) {
                delegate.write(bytes[i]);
                if (bytes[i] == '\n') {
                    delegate.write(indent);
                }
            }
        }
    }

    private static String readFromFile(File file) {
        try {
            return FileUtils.readFileToString(file).trim();
        } catch (IOException ioException) {
            throw new GradleException("Cannot read java properties file.", ioException);
        }
    }
}
