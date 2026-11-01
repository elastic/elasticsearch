/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.GradleException;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * The command line tools used to inspect deb and rpm packages, which are only available on some build hosts.
 */
final class PackageTools {

    /**
     * The directories searched for the package tools unless configured otherwise.
     */
    static final List<String> DEFAULT_SEARCH_PATH = List.of("/bin", "/usr/bin", "/usr/local/bin");

    private PackageTools() {}

    /**
     * The name of the executable that can read packages of the given type.
     */
    static String executableName(PackageMetadata.Type type) {
        return switch (type) {
            case DEB -> "dpkg-deb";
            case RPM -> "rpm";
        };
    }

    /**
     * Runs a package tool and returns its standard output, failing with the full output if the tool fails.
     */
    static String run(ExecOperations execOperations, List<String> commandLine) {
        ByteArrayOutputStream standardOutput = new ByteArrayOutputStream();
        ByteArrayOutputStream errorOutput = new ByteArrayOutputStream();
        ExecResult result = execOperations.exec(spec -> {
            spec.commandLine(commandLine);
            spec.setStandardOutput(standardOutput);
            spec.setErrorOutput(errorOutput);
            spec.setIgnoreExitValue(true);
        });
        String output = standardOutput.toString(StandardCharsets.UTF_8);
        if (result.getExitValue() != 0) {
            throw new GradleException(
                "Command "
                    + commandLine
                    + " failed with exit code ["
                    + result.getExitValue()
                    + "]\nstdout:\n"
                    + output
                    + "\nstderr:\n"
                    + errorOutput.toString(StandardCharsets.UTF_8)
            );
        }
        return output;
    }
}
