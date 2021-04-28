/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.fs;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class AvailableIndexFoldersBenchmark {

    private NodeEnvironment.NodePath nodePath;
    private NodeEnvironment nodeEnv;
    private Set<String> excludedDirs;

    @Setup
    public void setup() throws IOException {
        Path path = Files.createTempDirectory("test");
        nodePath = new NodeEnvironment.NodePath(path);

        LogConfigurator.setNodeName("test");
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), path)
            .put(Environment.PATH_DATA_SETTING.getKey(), path.resolve("data"))
            .build();
        nodeEnv = new NodeEnvironment(settings, new Environment(settings, null));

        Files.createDirectories(nodePath.indicesPath);
        excludedDirs = new HashSet<>();
        int numIndices = 5000;
        for (int i = 0; i < numIndices; i++) {
            String dirName = "dir" + i;
            Files.createDirectory(nodePath.indicesPath.resolve(dirName));
            excludedDirs.add(dirName);
        }
        if (nodeEnv.availableIndexFoldersForPath(nodePath).size() != numIndices) {
            throw new IllegalStateException("bad size");
        }
        if (nodeEnv.availableIndexFoldersForPath(nodePath, excludedDirs::contains).size() != 0) {
            throw new IllegalStateException("bad size");
        }
    }

    @Benchmark
    public Set<String> availableIndexFolderNaive() throws IOException {
        return nodeEnv.availableIndexFoldersForPath(nodePath);
    }

    @Benchmark
    public Set<String> availableIndexFolderOptimized() throws IOException {
        return nodeEnv.availableIndexFoldersForPath(nodePath, excludedDirs::contains);
    }

}
