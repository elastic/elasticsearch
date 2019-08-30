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

package org.elasticsearch.gradle;

import org.elasticsearch.gradle.tool.ClasspathUtils;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.internal.jvm.Jvm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReaperService {

    private static final String REAPER_CLASS = "org/elasticsearch/gradle/reaper/Reaper.class";
    private static final Pattern REAPER_JAR_PATH_PATTERN = Pattern.compile("file:(.*)!/" + REAPER_CLASS);
    private Logger logger;
    private Path buildDir;
    private Path inputDir;
    private Path logFile;
    private volatile Process reaperProcess;

    public ReaperService(Logger logger, Path buildDir, Path inputDir) {
        this.logger = logger;
        this.buildDir = buildDir;
        this.inputDir = inputDir;
        this.logFile = inputDir.resolve("reaper.log");
    }

    /**
     * Register a pid that will be killed by the reaper.
     */
    public void registerPid(String serviceId, long pid) {
        String[] killPidCommand = OS.<String[]>conditional()
            .onWindows(() -> new String[]{"Taskill", "/F", "/PID", String.valueOf(pid)})
            .onUnix(() -> new String[]{"kill", "-9", String.valueOf(pid)})
            .supply();
        registerCommand(serviceId, killPidCommand);
    }

    /**
     * Register a system command that will be run by the reaper.
     */
    public void registerCommand(String serviceId, String... command) {
        ensureReaperStarted();

        try {
            Files.writeString(getCmdFile(serviceId), String.join(" ", command));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getCmdFile(String serviceId) {
        return inputDir.resolve(
            serviceId.replaceAll("[^a-zA-Z0-9]","-") + ".cmd"
        );
    }

    public void unregister(String serviceId) {
        try {
            Files.deleteIfExists(getCmdFile(serviceId));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void shutdown() {
        if (reaperProcess != null) {
            ensureReaperAlive();
            try {
                reaperProcess.getOutputStream().close();
                logger.info("Waiting for reaper to exit normally");
                if (reaperProcess.waitFor() != 0) {
                    throw new GradleException("Reaper process failed. Check log at "
                        + inputDir.resolve("error.log") + " for details");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    private synchronized void ensureReaperStarted() {
        if (reaperProcess == null) {
            try {
                Path jarPath = locateReaperJar();

                // ensure the input directory exists
                Files.createDirectories(inputDir);

                // start the reaper
                ProcessBuilder builder = new ProcessBuilder(
                    Jvm.current().getJavaExecutable().toString(), // same jvm as gradle
                    "-Xms4m", "-Xmx16m", // no need for a big heap, just need to read some files and execute
                    "-jar", jarPath.toString(),
                    inputDir.toString());
                logger.info("Launching reaper: " + String.join(" ", builder.command()));
                // be explicit for stdin, we use closing of the pipe to signal shutdown to the reaper
                builder.redirectInput(ProcessBuilder.Redirect.PIPE);
                builder.redirectOutput(logFile.toFile());
                builder.redirectError(logFile.toFile());
                reaperProcess = builder.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            ensureReaperAlive();
        }
    }

    private Path locateReaperJar() {
        if (ClasspathUtils.isElasticsearchProject()) {
            // when running inside the Elasticsearch build just pull find the jar in the runtime classpath
            URL main = this.getClass().getClassLoader().getResource(REAPER_CLASS);
            String mainPath = main.getFile();
            Matcher matcher = REAPER_JAR_PATH_PATTERN.matcher(mainPath);

            if (matcher.matches()) {
                String path = matcher.group(1);
                return Path.of(
                    OS.<String>conditional()
                        .onWindows(() -> path.substring(1))
                        .onUnix(() -> path)
                        .supply()
                );
            } else {
                throw new RuntimeException("Unable to locate " + REAPER_CLASS + " on build classpath.");
            }
        } else {
            // copy the reaper jar
            Path jarPath = buildDir.resolve("reaper").resolve("reaper.jar");
            try {
                Files.createDirectories(jarPath.getParent());
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to create reaper JAR output directory " + jarPath.getParent(), e);
            }

            try (
                OutputStream out = Files.newOutputStream(jarPath);
                InputStream jarInput = this.getClass().getResourceAsStream("/META-INF/reaper.jar");
            ) {
                logger.info("Copying reaper.jar...");
                jarInput.transferTo(out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return jarPath;
        }
    }

    private void ensureReaperAlive() {
        if (reaperProcess.isAlive() == false) {
            throw new IllegalStateException("Reaper process died unexpectedly! Check the log at " + logFile.toString());
        }
    }
}
