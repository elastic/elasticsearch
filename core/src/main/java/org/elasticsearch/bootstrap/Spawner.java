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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.plugins.Plugins;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spawns native plugin controller processes if present. Will only work prior to a system call
 * filter being installed.
 */
final class Spawner implements Closeable {

    /*
     * References to the processes that have been spawned, so that we can destroy them.
     */
    private final List<Process> processes = new ArrayList<>();
    private AtomicBoolean spawned = new AtomicBoolean();

    @Override
    public void close() throws IOException {
        IOUtils.close(() -> processes.stream().map(s -> (Closeable) s::destroy).iterator());
    }

    /**
     * Spawns the native controllers for each plugin
     *
     * @param environment the node environment
     * @throws IOException if an I/O error occurs reading the plugins or spawning a native process
     */
    void spawnNativePluginControllers(final Environment environment) throws IOException {
        if (!spawned.compareAndSet(false, true)) {
            throw new IllegalStateException("native controllers already spawned");
        }
        final Path pluginsFile = environment.pluginsFile();
        if (!Files.exists(pluginsFile)) {
            throw new IllegalStateException("plugins directory [" + pluginsFile + "] not found");
        }
        /*
         * For each plugin, attempt to spawn the controller daemon. Silently ignore any plugin that
         * don't include a controller for the correct platform.
         */
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsFile)) {
            for (final Path plugin : stream) {
                final PluginInfo info = PluginInfo.readFromProperties(plugin);
                final Path spawnPath = Plugins.nativeControllerPath(plugin);
                if (!Files.isRegularFile(spawnPath)) {
                    continue;
                }
                if (!info.hasNativeController()) {
                    final String message = String.format(
                            Locale.ROOT,
                            "plugin [%s] does not have permission to fork native controller",
                            plugin.getFileName());
                    throw new IllegalArgumentException(message);
                }
                final Process process =
                        spawnNativePluginController(spawnPath, environment.tmpFile());
                processes.add(process);
            }
        }
    }

    /**
     * Attempt to spawn the controller daemon for a given plugin. The spawned process will remain
     * connected to this JVM via its stdin, stdout, and stderr streams, but the references to these
     * streams are not available to code outside this package.
     */
    private Process spawnNativePluginController(
            final Path spawnPath,
            final Path tmpPath) throws IOException {
        final ProcessBuilder pb = new ProcessBuilder(spawnPath.toString());

        // the only environment variable passes on the path to the temporary directory
        pb.environment().clear();
        pb.environment().put("TMPDIR", tmpPath.toString());

        // the output stream of the process object corresponds to the daemon's stdin
        return pb.start();
    }

    /**
     * The collection of processes representing spawned native controllers.
     *
     * @return the processes
     */
    List<Process> getProcesses() {
        return Collections.unmodifiableList(processes);
    }

}
