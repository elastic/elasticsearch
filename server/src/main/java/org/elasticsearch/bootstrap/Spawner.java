/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.plugins.PluginsUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spawns native module controller processes if present. Will only work prior to a system call filter being installed.
 */
final class Spawner implements Closeable {

    /*
     * References to the processes that have been spawned, so that we can destroy them.
     */
    private final List<Process> processes = new ArrayList<>();
    private final List<Thread> pumpThreads = new ArrayList<>();
    private AtomicBoolean spawned = new AtomicBoolean();

    @Override
    public void close() throws IOException {
        List<Closeable> closeables = new ArrayList<>();
        closeables.addAll(processes.stream().map(s -> (Closeable) s::destroy).toList());
        closeables.addAll(pumpThreads.stream().map(t -> (Closeable) () -> {
            try {
                t.join(); // wait for thread to complete now that the spawned process is destroyed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // best effort, ignore
            }
        }).toList());
        IOUtils.close(closeables);
    }

    /**
     * Spawns the native controllers for each module.
     *
     * @param environment The node environment
     * @throws IOException if an I/O error occurs reading the module or spawning a native process
     */
    void spawnNativeControllers(final Environment environment) throws IOException {
        if (spawned.compareAndSet(false, true) == false) {
            throw new IllegalStateException("native controllers already spawned");
        }
        if (Files.exists(environment.modulesDir()) == false) {
            throw new IllegalStateException("modules directory [" + environment.modulesDir() + "] not found");
        }
        /*
         * For each module, attempt to spawn the controller daemon. Silently ignore any module that doesn't include a controller for the
         * correct platform.
         */
        List<Path> paths = PluginsUtils.findPluginDirs(environment.modulesDir());
        for (final Path modules : paths) {
            final PluginDescriptor info = PluginDescriptor.readFromProperties(modules);
            final Path spawnPath = Platforms.nativeControllerPath(modules);
            if (Files.isRegularFile(spawnPath) == false) {
                continue;
            }
            if (info.hasNativeController() == false) {
                final String message = String.format(
                    Locale.ROOT,
                    "module [%s] does not have permission to fork native controller",
                    modules.getFileName()
                );
                throw new IllegalArgumentException(message);
            }
            final Process process = spawnNativeController(spawnPath, environment.tmpDir());
            // The process _shouldn't_ write any output via its stdout or stderr, but if it does then
            // it will block if nothing is reading that output. To avoid this we can pipe the
            // outputs and create pump threads to write any messages there to the ES log.
            startPumpThread(info.getName(), "stdout", process.getInputStream());
            startPumpThread(info.getName(), "stderr", process.getErrorStream());
            processes.add(process);
        }
    }

    private void startPumpThread(String componentName, String streamName, InputStream stream) {
        String loggerName = componentName + "-controller-" + streamName;
        final Logger logger = LogManager.getLogger(loggerName);
        Thread t = new Thread(() -> {
            try (var br = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // since we do not expect native controllers to ever write to stdout/stderr, we always log at warn level
                    logger.warn(line);
                }
            } catch (IOException e) {
                logger.error("error while reading " + streamName, e);
            }
        }, loggerName + "-pump");
        t.start();
        pumpThreads.add(t);
    }

    /**
     * Attempt to spawn the controller daemon for a given module. The spawned process will remain connected to this JVM via its stdin,
     * stdout, and stderr streams, but the references to these streams are not available to code outside this package.
     */
    private static Process spawnNativeController(final Path spawnPath, final Path tmpPath) throws IOException {
        final String command;
        if (Constants.WINDOWS) {
            /*
             * We have to get the short path name or starting the process could fail due to max path limitations. The underlying issue here
             * is that starting the process on Windows ultimately involves the use of CreateProcessW. CreateProcessW has a limitation that
             * if its first argument (the application name) is null, then its second argument (the command line for the process to start) is
             * restricted in length to 260 characters (cf. https://msdn.microsoft.com/en-us/library/windows/desktop/ms682425.aspx). Since
             * this is exactly how the JDK starts the process on Windows (cf.
             * http://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/windows/native/java/lang/ProcessImpl_md.c#l319), this
             * limitation is in force. As such, we use the short name to avoid any such problems.
             */
            command = NativeAccess.instance().getWindowsFunctions().getShortPathName(spawnPath.toString());
        } else {
            command = spawnPath.toString();
        }
        final ProcessBuilder pb = new ProcessBuilder(command);

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
