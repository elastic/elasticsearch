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
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Spawns native plugin controller processes if present.  Will only work prior to seccomp being set up.
 */
final class Spawner {

    private static final String PROGRAM_NAME = Constants.WINDOWS ? "controller.exe" : "controller";
    private static final String PLATFORM_NAME = makePlatformName(Constants.OS_NAME, Constants.OS_ARCH);
    private static final String TMP_ENVVAR = "TMPDIR";

    /**
     * On Windows we have to retain a reference to the OutputStream that corresponds to each
     * process's stdin, otherwise the pipe will be closed and the spawned process will receive
     * an EOF on its stdin.  This isn't necessary on *nix but doesn't do any harm.
     */
    private final List<OutputStream> stdinReferences;

    Spawner() {
        stdinReferences = new ArrayList<>();
    }

    /**
     * For each plugin, attempt to spawn the controller daemon.
     */
    void spawnNativePluginControllers(Environment environment) throws IOException {
        if (Files.exists(environment.pluginsFile())) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(environment.pluginsFile())) {
                for (Path plugin : stream) {
                    spawnNativePluginController(environment, plugin);
                }
            }
        }
    }

    /**
     * Attempt to spawn the controller daemon for a given plugin.  Silently ignore any plugins
     * that don't include a version of the program for the correct platform.  The spawned
     * process will remain connected to this JVM via its stdin, stdout and stderr, but the
     * references to these streams are not available to code outside this class.
     */
    private void spawnNativePluginController(Environment environment, Path plugin) throws IOException {
        Path spawnPath = makeSpawnPath(plugin);
        if (Files.exists(spawnPath) == false) {
            return;
        }

        ProcessBuilder pb = new ProcessBuilder(spawnPath.toString());

        // The only environment variable passes on the path to the temporary directory
        pb.environment().clear();
        pb.environment().put(TMP_ENVVAR, environment.tmpFile().toString());

        // The output stream of the Process object corresponds to the daemon's stdin
        stdinReferences.add(pb.start().getOutputStream());
    }

    List<OutputStream> getStdinReferences() {
        return stdinReferences;
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
