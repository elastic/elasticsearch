/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.gradle.api.GradleException;
import org.gradle.process.ExecOperations;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class wraps certain {@code git} operations. This is partly for convenience, and partly so that these
 * operations can be easily mocked in testing.
 */
public class GitWrapper {

    private final ExecOperations execOperations;

    public GitWrapper(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    /**
     * @return a mapping from remote names to remote URLs.
     */
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

    /**
     * Updates the git repository from the specified remote
     * @param remote the remote to use to update
     */
    public void updateRemote(String remote) {
        runCommand("git", "fetch", Objects.requireNonNull(remote));
    }

    /**
     * Updates the git repository's tags from the specified remote
     * @param remote the remote to use to update
     */
    public void updateTags(String remote) {
        runCommand("git", "fetch", "--tags", Objects.requireNonNull(remote));
    }

    /**
     * Fetch all tags matching the specified pattern, returning them as {@link QualifiedVersion} instances.
     * @param pattern the tag pattern to match
     * @return matching versions
     */
    public Stream<QualifiedVersion> listVersions(String pattern) {
        return runCommand("git", "tag", "-l", pattern).lines().map(QualifiedVersion::of);
    }

    /**
     * Returns all files at the specified {@param path} for the state of the git repository at {@param ref}.
     *
     * @param ref the ref to use
     * @param path the path to list
     * @return A stream of file names. No path information is included.
     */
    public Stream<String> listFiles(String ref, String path) {
        return runCommand("git", "ls-tree", "--name-only", "-r", ref, path).lines();
    }

    public String getUpstream() {
        String upstream = listRemotes().entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains("elastic/elasticsearch"))
            .findFirst()
            .map(Map.Entry::getKey)
            .orElseThrow(() -> new GradleException("Couldn't find a git remote for [elastic/elasticsearch]"));
        return upstream;
    }
}
