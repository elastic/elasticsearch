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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Wrapper to run shell commands and collect their outputs in a less verbose way
 */
public class Shell {

    protected final Logger logger =  LogManager.getLogger(getClass());

    final Map<String, String> env;
    Path workingDirectory;

    public Shell() {
        this.env = new HashMap<>();
        this.workingDirectory = null;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public void setWorkingDirectory(Path workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    /**
     * Run the provided string as a shell script. On Linux the {@code bash -c [script]} syntax will be used, and on Windows
     * the {@code powershell.exe -Command [script]} syntax will be used. Throws an exception if the exit code of the script is nonzero
     */
    public Result run(String script) {
        return runScript(getScriptCommand(script));
    }

    /**
     * Same as {@link #run(String)}, but does not throw an exception if the exit code of the script is nonzero
     */
    public Result runIgnoreExitCode(String script) {
        return runScriptIgnoreExitCode(getScriptCommand(script));
    }

    public void chown(Path path) throws Exception {
        Platforms.onLinux(() -> run("chown -R elasticsearch:elasticsearch " + path));
        Platforms.onWindows(() -> run(
            "$account = New-Object System.Security.Principal.NTAccount '" + System.getenv("username")  + "'; " +
                "$tempConf = Get-ChildItem '" + path + "' -Recurse; " +
                "$tempConf += Get-Item '" + path + "'; " +
                "$tempConf | ForEach-Object { " +
                "$acl = Get-Acl $_.FullName; " +
                "$acl.SetOwner($account); " +
                "Set-Acl $_.FullName $acl " +
                "}"
        ));
    }

    public Result run( String command, Object... args) {
        String formattedCommand = String.format(Locale.ROOT, command, args);
        return run(formattedCommand);
    }
    private String[] getScriptCommand(String script) {
        if (Platforms.WINDOWS) {
            return powershellCommand(script);
        } else {
            return bashCommand(script);
        }
    }

    private static String[] bashCommand(String script) {
        return Stream.concat(Stream.of("bash", "-c"), Stream.of(script)).toArray(String[]::new);
    }

    private static String[] powershellCommand(String script) {
        return Stream.concat(Stream.of("powershell.exe", "-Command"), Stream.of(script)).toArray(String[]::new);
    }

    private Result runScript(String[] command) {
        Result result = runScriptIgnoreExitCode(command);
        if (result.isSuccess() == false) {
            throw new RuntimeException("Command was not successful: [" + String.join(" ", command) + "]\n   result: " + result.toString());
        }
        return result;
    }

    private Result runScriptIgnoreExitCode(String[] command) {
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

            StringBuffer stdout = new StringBuffer();
            StringBuffer stderr = new StringBuffer();

            Thread stdoutThread = new Thread(new StreamCollector(process.getInputStream(), stdout));
            Thread stderrThread = new Thread(new StreamCollector(process.getErrorStream(), stderr));

            stdoutThread.start();
            stderrThread.start();

            if (process.waitFor(10, TimeUnit.MINUTES) == false) {
                // Try to wait for thread
                tryToWaitForThread(stdoutThread);
                tryToWaitForThread(stderrThread);

                Result result = new Result(-1, stdout.toString(), stderr.toString());
                throw new IllegalStateException(
                    "Timed out running shell command: " + command + "\n" +
                    "Result:\n" + result
                );
            }
            int exitCode = process.exitValue();

            // Try to wait for thread
            tryToWaitForThread(stdoutThread);
            tryToWaitForThread(stderrThread);

            Result result = new Result(exitCode, stdout.toString(), stderr.toString());
            logger.info("Ran: {}\nr{}\n", Arrays.toString(command), result);
            return result;

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void tryToWaitForThread(Thread stdoutThread) throws InterruptedException {
        logger.info("Waiting for appender thread to exit");
        stdoutThread.join(TimeUnit.SECONDS.toMillis(3));
        if (stdoutThread.isAlive()) {
            logger.info("Appender thread still alive, interrupting it");
            stdoutThread.interrupt();
            stdoutThread.join();
        } else {
            logger.info("Appender thread stopped");
        }
    }

    @SuppressForbidden(reason = "ProcessBuilder expects java.io.File")
    private static void setWorkingDirectory(ProcessBuilder builder, Path path) {
        builder.directory(path.toFile());
    }

    public String toString() {
        return new StringBuilder()
            .append(" ")
            .append("env = [")
            .append(env)
            .append("]")
            .append("workingDirectory = [")
            .append(workingDirectory)
            .append("]")
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
                .append("exitCode = [")
                .append(exitCode)
                .append("]")
                .append("stdout = [")
                .append(stdout.trim())
                .append("]")
                .append("stderr = [")
                .append(stderr.trim())
                .append("]")
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

                do {
                    while (reader.ready() == false) {
                        Thread.sleep(10);
                    }
                    line = reader.readLine();
                    if (line != null) {
                        appendable.append(line);
                        appendable.append("\n");
                    }
                } while (line != null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // It's expected that the control thread will interrupt, just exit
                return;
            }
        }

        @SuppressForbidden(reason = "the system's default character set is a best guess of what subprocesses will use")
        private static InputStreamReader reader(InputStream inputStream) {
            return new InputStreamReader(inputStream);
        }
    }
}
