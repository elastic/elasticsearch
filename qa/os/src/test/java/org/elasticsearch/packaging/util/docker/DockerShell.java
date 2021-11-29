/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util.docker;

import org.elasticsearch.packaging.util.Shell;

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
                    dockerLogs.stdout,
                    dockerLogs.stderr
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
}
