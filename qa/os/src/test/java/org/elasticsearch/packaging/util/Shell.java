/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wrapper to run shell commands and collect their outputs in a less verbose way
 */
public class Shell {

    public static final Result NO_OP = new Shell.Result(0, "", "");
    protected final Logger logger = LogManager.getLogger(getClass());

    protected final Map<String, String> env = new HashMap<>();
    String umask;
    Path workingDirectory;

    public Shell() {
        this.workingDirectory = null;
    }

    /**
     * Reset the shell to its newly created state.
     */
    public void reset() {
        env.clear();
        workingDirectory = null;
        umask = null;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public void setWorkingDirectory(Path workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public void setUmask(String umask) {
        this.umask = umask;
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
        Platforms.onWindows(
            () -> run(
                String.format(
                    Locale.ROOT,
                    "$account = New-Object System.Security.Principal.NTAccount '%s'; "
                        + "$pathInfo = Get-Item '%s'; "
                        + "$toChown = @(); "
                        + "if ($pathInfo.PSIsContainer) { "
                        + "  $toChown += Get-ChildItem '%s' -Recurse; "
                        + "}"
                        + "$toChown += $pathInfo; "
                        + "$toChown | ForEach-Object { "
                        + "  $acl = Get-Acl $_.FullName; "
                        + "  $acl.SetOwner($account); "
                        + "  Set-Acl $_.FullName $acl "
                        + "}",
                    System.getenv("username"),
                    path,
                    path
                )
            )
        );
    }

    public void extractZip(Path zipPath, Path destinationDir) throws Exception {
        Platforms.onLinux(() -> run("unzip \"" + zipPath + "\" -d \"" + destinationDir + "\""));
        Platforms.onWindows(() -> run("Expand-Archive -Path \"" + zipPath + "\" -DestinationPath \"" + destinationDir + "\""));
    }

    public Result run(String command, Object... args) {
        String formattedCommand = String.format(Locale.ROOT, command, args);
        return run(formattedCommand);
    }

    protected String[] getScriptCommand(String script) {
        if (Platforms.WINDOWS) {
            return powershellCommand(script);
        } else {
            return bashCommand(script);
        }
    }

    private String[] bashCommand(String script) {
        List<String> command = new ArrayList<>();
        command.add("bash");
        command.add("-c");
        if (umask == null) {
            command.add(script);
        } else {
            command.add(String.format(Locale.ROOT, "umask %s && %s", umask, script));
        }
        return command.toArray(new String[0]);
    }

    private static String[] powershellCommand(String script) {
        return new String[] { "powershell.exe", "-Command", script };
    }

    private Result runScript(String[] command) {
        logger.warn("Running command with env: " + env);
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
        builder.environment().keySet().remove("ES_JAVA_HOME"); // start with a fresh environment
        builder.environment().keySet().remove("JAVA_HOME");
        for (Map.Entry<String, String> entry : env.entrySet()) {
            builder.environment().put(entry.getKey(), entry.getValue());
        }
        final Path stdOut;
        final Path stdErr;
        try {
            Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
            Files.createDirectories(tmpDir);
            stdOut = Files.createTempFile(tmpDir, getClass().getName(), ".out");
            stdErr = Files.createTempFile(tmpDir, getClass().getName(), ".err");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        redirectOutAndErr(builder, stdOut, stdErr);

        try {
            Process process = builder.start();
            if (process.waitFor(10, TimeUnit.MINUTES) == false) {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
                Result result = new Result(-1, readFileIfExists(stdOut), readFileIfExists(stdErr));
                throw new IllegalStateException(
                    "Timed out running shell command: " + Arrays.toString(command) + "\n" + "Result:\n" + result
                );
            }

            Result result = new Result(process.exitValue(), readFileIfExists(stdOut), readFileIfExists(stdErr));
            logger.info("Ran: {} {}", Arrays.toString(command), result);
            return result;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            try {
                FileUtils.deleteIfExists(stdOut);
                FileUtils.deleteIfExists(stdErr);
            } catch (UncheckedIOException e) {
                logger.info("Cleanup of output files failed", e);
            }
        }
    }

    private String readFileIfExists(Path path) throws IOException {
        if (Files.exists(path)) {
            long size = Files.size(path);
            if (size > 100 * 1024) {
                return "<<Too large to read: " + size + " bytes>>";
            }
            try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
                return lines.collect(Collectors.joining("\n"));
            }
        } else {
            return "";
        }
    }

    @SuppressForbidden(reason = "ProcessBuilder expects java.io.File")
    private void redirectOutAndErr(ProcessBuilder builder, Path stdOut, Path stdErr) {
        builder.redirectOutput(stdOut.toFile());
        builder.redirectError(stdErr.toFile());
    }

    @SuppressForbidden(reason = "ProcessBuilder expects java.io.File")
    private static void setWorkingDirectory(ProcessBuilder builder, Path path) {
        builder.directory(path.toFile());
    }

    public String toString() {
        return String.format(Locale.ROOT, " env = [%s] workingDirectory = [%s]", env, workingDirectory);
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
            return String.format(Locale.ROOT, "exitCode = [%d] stdout = [%s] stderr = [%s]", exitCode, stdout.trim(), stderr.trim());
        }
    }

}
