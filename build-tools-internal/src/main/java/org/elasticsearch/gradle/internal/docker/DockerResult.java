/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import java.util.Objects;

/**
 * This class models the result of running a command. It captures the exit code, standard output and standard error and allows
 * applying String filter for stdout as this is intended to create configuration cache compatible output which
 * aims to be agnostic.
 */
public class DockerResult {

    private int exitCode;
    private String stdout;
    private String stderr;

    public DockerResult(int exitCode, String stdout, String stderr) {
        this.exitCode = exitCode;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    public int getExitCode() {
        return exitCode;
    }

    public String getStdout() {
        return stdout;
    }

    public String getStderr() {
        return stderr;
    }

    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    public void setStdout(String stdout) {
        this.stdout = stdout;
    }

    public void setStderr(String stderr) {
        this.stderr = stderr;
    }

    public boolean isSuccess() {
        return exitCode == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DockerResult that = (DockerResult) o;
        return exitCode == that.exitCode && Objects.equals(stdout, that.stdout) && Objects.equals(stderr, that.stderr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exitCode, stdout, stderr);
    }
}
