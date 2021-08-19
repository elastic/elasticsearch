/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.gradle.process.ExecOperations;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GitWrapper {

    private final ExecOperations execOperations;

    public GitWrapper(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    public Map<String, String> listRemotes() {
        return runCommand("git", "remote", "-v").lines()
            .filter(l -> l.contains("(fetch)"))
            .map(line -> line.split("\\s+"))
            .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
    }

    String runCommand(String... args) {
        final ByteArrayOutputStream stdout = new ByteArrayOutputStream();

        execOperations.exec(spec -> {
            // The redundant cast is to silence a compiler warning.
            spec.setCommandLine((Object[]) args);
            spec.setStandardOutput(stdout);
        });

        return stdout.toString(StandardCharsets.UTF_8);
    }

    public void updateRemote(String remote) {
        runCommand("git", "fetch", remote);
    }

    public void updateTags(String remote) {
        runCommand("git", "fetch", "--tags", remote);
    }

    public Stream<QualifiedVersion> listVersions(String pattern) {
        return runCommand("git", "tag", "-l", pattern).lines().map(QualifiedVersion::of);
    }

    public Stream<String> listFiles(String ref, String path) {
        return runCommand("git", "ls-tree", "--name-only", "-r", ref, path).lines();
    }
}
