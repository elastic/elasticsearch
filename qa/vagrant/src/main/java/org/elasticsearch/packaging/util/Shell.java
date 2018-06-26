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

import org.elasticsearch.common.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * Wrapper to run shell commands and collect their outputs in a less verbose way
 */
public class Shell {

    final Map<String, String> env;
    final Path workingDirectory;

    public Shell() {
        this(emptyMap(), null);
    }

    public Shell(Map<String, String> env) {
        this(env, null);
    }

    public Shell(Path workingDirectory) {
        this(emptyMap(), workingDirectory);
    }

    public Shell(Map<String, String> env, Path workingDirectory) {
        this.env = new HashMap<>(env);
        this.workingDirectory = workingDirectory;
    }

    /**
     * Runs a script in a bash shell, throwing an exception if its exit code is nonzero
     */
    public Result bash(String script) {
        return run(bashCommand(script));
    }

    /**
     * Runs a script in a bash shell
     */
    public Result bashIgnoreExitCode(String script) {
        return runIgnoreExitCode(bashCommand(script));
    }

    private static String[] bashCommand(String script) {
        return Stream.concat(Stream.of("bash", "-c"), Stream.of(script)).toArray(String[]::new);
    }

    /**
     * Runs a script in a powershell shell, throwing an exception if its exit code is nonzero
     */
    public Result powershell(String script) {
        return run(powershellCommand(script));
    }

    /**
     * Runs a script in a powershell shell
     */
    public Result powershellIgnoreExitCode(String script) {
        return runIgnoreExitCode(powershellCommand(script));
    }

    private static String[] powershellCommand(String script) {
        return Stream.concat(Stream.of("powershell.exe", "-Command"), Stream.of(script)).toArray(String[]::new);
    }

    /**
     * Runs an executable file, passing all elements of {@code command} after the first as arguments. Throws an exception if the process'
     * exit code is nonzero
     */
    private Result run(String[] command) {
        Result result = runIgnoreExitCode(command);
        if (result.isSuccess() == false) {
            throw new RuntimeException("Command was not successful: [" + String.join(" ", command) + "] result: " + result.toString());
        }
        return result;
    }

    /**
     * Runs an executable file, passing all elements of {@code command} after the first as arguments
     */
    private Result runIgnoreExitCode(String[] command) {
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(command);

        if (workingDirectory != null) {
            setWorkingDirectory(builder, workingDirectory);
        }

        if (env != null && env.isEmpty() == false) {
            for (Map.Entry<String, String> entry : env.entrySet()) {
                builder.environment().put(entry.getKey(), entry.getValue());
            }
        }

        try {

            Process process = builder.start();

            StringBuilder stdout = new StringBuilder();
            StringBuilder stderr = new StringBuilder();

            Thread stdoutThread = new Thread(new StreamCollector(process.getInputStream(), stdout));
            Thread stderrThread = new Thread(new StreamCollector(process.getErrorStream(), stderr));

            stdoutThread.start();
            stderrThread.start();

            stdoutThread.join();
            stderrThread.join();

            int exitCode = process.waitFor();

            return new Result(exitCode, stdout.toString(), stderr.toString());

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressForbidden(reason = "ProcessBuilder expects java.io.File")
    private static void setWorkingDirectory(ProcessBuilder builder, Path path) {
        builder.directory(path.toFile());
    }

    public String toString() {
        return new StringBuilder()
            .append("<")
            .append(this.getClass().getName())
            .append(" ")
            .append("env = [")
            .append(env)
            .append("]")
            .append("workingDirectory = [")
            .append(workingDirectory)
            .append("]")
            .append(">")
            .toString();
    }

    public static class Result {
        public final int exitCode;
        public final String stdout;
        public final String stderr;

        public Result(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public boolean isSuccess() {
            return exitCode == 0;
        }

        public String toString() {
            return new StringBuilder()
                .append("<")
                .append(this.getClass().getName())
                .append(" ")
                .append("exitCode = [")
                .append(exitCode)
                .append("]")
                .append(" ")
                .append("stdout = [")
                .append(stdout)
                .append("]")
                .append(" ")
                .append("stderr = [")
                .append(stderr)
                .append("]")
                .append(">")
                .toString();
        }
    }

    private static class StreamCollector implements Runnable {
        private final InputStream input;
        private final Appendable appendable;

        StreamCollector(InputStream input, Appendable appendable) {
            this.input = Objects.requireNonNull(input);
            this.appendable = Objects.requireNonNull(appendable);
        }

        public void run() {
            try {

                BufferedReader reader = new BufferedReader(reader(input));
                String line;

                while ((line = reader.readLine()) != null) {
                    appendable.append(line);
                    appendable.append("\n");
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressForbidden(reason = "the system's default character set is a best guess of what subprocesses will use")
        private static InputStreamReader reader(InputStream inputStream) {
            return new InputStreamReader(inputStream);
        }
    }
}
