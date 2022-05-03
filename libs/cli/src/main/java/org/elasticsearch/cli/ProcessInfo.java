/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

import org.elasticsearch.core.SuppressForbidden;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * A holder for information about a CLI process.
 *
 * @param sysprops System properties passed to the JVM
 * @param envVars Environment variables of the process
 * @param workingDir The working directory of the process
 */
public record ProcessInfo(Map<String, String> sysprops, Map<String, String> envVars, Path workingDir) {

    /**
     * Return process info for the current JVM.
     */
    @SuppressForbidden(reason = "get system process information")
    public static ProcessInfo fromSystem() {
        Properties props = System.getProperties();
        var sysprops = props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
        var envVars = Map.copyOf(System.getenv());
        var workingDir = Paths.get("").toAbsolutePath();
        return new ProcessInfo(sysprops, envVars, workingDir);
    }
}
