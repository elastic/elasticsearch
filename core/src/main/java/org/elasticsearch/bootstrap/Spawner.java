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

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.env.Environment;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Spawns native plugin controller processes if present.  Will only work prior to a system call filter being installed.
 */
final class Spawner implements Closeable {

    private static final String PROGRAM_NAME = Constants.WINDOWS ? "controller.exe" : "controller";
    private static final String PLATFORM_NAME = makePlatformName(Constants.OS_NAME, Constants.OS_ARCH);
    private static final String TMP_ENVVAR = "TMPDIR";

    /**
     * References to the processes that have been spawned, so that we can destroy them.
     */
    private final List<Process> processes = new ArrayList<>();

    @Override
    public void close() throws IOException {
        try {
            IOUtils.close(() -> processes.stream().map(s -> (Closeable)s::destroy).iterator());
        } finally {
            processes.clear();
        }
    }

    /**
     * For each plugin, attempt to spawn the controller daemon.  Silently ignore any plugins
     * that don't include a controller for the correct platform.
     */
    void spawnNativePluginControllers(Environment environment) throws IOException {
        if (Files.exists(environment.pluginsFile())) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
                for (Path plugin : stream) {
                    Path spawnPath = makeSpawnPath(plugin);
                    if (Files.isRegularFile(spawnPath)) {
                        spawnNativePluginController(spawnPath, environment.tmpFile());
                    }
                }
            }
        }
    }

    /**
     * Attempt to spawn the controller daemon for a given plugin.  The spawned process
     * will remain connected to this JVM via its stdin, stdout and stderr, but the
     * references to these streams are not available to code outside this package.
     */
    private void spawnNativePluginController(Path spawnPath, Path tmpPath) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(spawnPath.toString());

        // The only environment variable passes on the path to the temporary directory
        pb.environment().clear();
        pb.environment().put(TMP_ENVVAR, tmpPath.toString());

        // The output stream of the Process object corresponds to the daemon's stdin
        processes.add(pb.start());
    }

    List<Process> getProcesses() {
        return Collections.unmodifiableList(processes);
    }

    /**
     * Make the full path to the program to be spawned.
     */
    static Path makeSpawnPath(Path plugin) {
        return plugin.resolve("platform").resolve(PLATFORM_NAME).resolve("bin").resolve(PROGRAM_NAME);
    }

    /**
     * Make the platform name in the format used in Kibana downloads, for example:
     * - darwin-x86_64
     * - linux-x86-64
     * - windows-x86_64
     * For *nix platforms this is more-or-less `uname -s`-`uname -m` converted to lower case.
     * However, for consistency between different operating systems on the same architecture
     * "amd64" is replaced with "x86_64" and "i386" with "x86".
     * For Windows it's "windows-" followed by either "x86" or "x86_64".
     */
    static String makePlatformName(String osName, String osArch) {
        String os = osName.toLowerCase(Locale.ROOT);
        if (os.startsWith("windows")) {
            os = "windows";
        } else if (os.equals("mac os x")) {
            os = "darwin";
        }
        String cpu = osArch.toLowerCase(Locale.ROOT);
        if (cpu.equals("amd64")) {
            cpu = "x86_64";
        } else if (cpu.equals("i386")) {
            cpu = "x86";
        }
        return os + "-" + cpu;
    }
}
