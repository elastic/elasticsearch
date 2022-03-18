/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util.docker;

import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.packaging.util.Shell;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Extends {@link Shell} so that executed commands happen in the currently running Docker container.
 */
public class DockerShell extends Shell {
    @Override
    protected String[] getScriptCommand(String script) {
        assert Docker.containerId != null;

        List<String> cmd = new ArrayList<>();
        cmd.add("docker");
        cmd.add("exec");
        cmd.add("--tty");

        env.forEach((key, value) -> cmd.add("--env " + key + "=\"" + value + "\""));

        cmd.add(Docker.containerId);
        cmd.add(script);

        return super.getScriptCommand(String.join(" ", cmd));
    }

    /**
     * Overrides {@link Shell#run(String)} to attempt to collect Docker container
     * logs when a command fails to execute successfully.
     *
     * @param script the command to run
     * @return the command's output
     */
    @Override
    public Result run(String script) {
        try {
            return super.run(script);
        } catch (ShellException e) {
            try {
                final Result dockerLogs = Docker.getContainerLogs();
                logger.error(
                    "Command [{}] failed.\n\nContainer stdout: [{}]\n\nContainer stderr: [{}]",
                    script,
                    dockerLogs.stdout(),
                    dockerLogs.stderr()
                );
            } catch (ShellException shellException) {
                logger.error(
                    "Command [{}] failed.\n\nTried to dump container logs but that failed too: [{}]",
                    script,
                    shellException.getMessage()
                );
            }
            throw e;
        }
    }

    /**
     * Execute a command inside the Docker container, but without invoking a local shell. The caller
     * is entirely responsible for correctly escaping command arguments, or for invoking a shell
     * inside the container if required.
     * @param args the command and arguments to execute inside the container
     * @return the result of executing the command
     */
    public static Shell.Result executeCommand(String... args) {
        assert Docker.containerId != null;

        final String[] prefix = new String[] { "docker", "exec", "--tty", Docker.containerId };
        final String[] command = ArrayUtils.concat(prefix, args);
        final ProcessBuilder pb = new ProcessBuilder(command);

        final Process p;
        final int exitCode;
        final String stdout;
        final String stderr;
        try {
            p = pb.start();
            exitCode = p.waitFor();
            stdout = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            stderr = new String(p.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new Shell.Result(exitCode, stdout, stderr);
    }
}
