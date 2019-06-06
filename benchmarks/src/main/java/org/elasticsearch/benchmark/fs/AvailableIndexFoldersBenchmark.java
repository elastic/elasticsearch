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
        String[] paths = new String[] {path.toString()};
        nodePath = new NodeEnvironment.NodePath(path);

        LogConfigurator.setNodeName("test");
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), path)
            .putList(Environment.PATH_DATA_SETTING.getKey(), paths).build();
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
