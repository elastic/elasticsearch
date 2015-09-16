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
package org.elasticsearch.common;

import java.nio.charset.StandardCharsets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Process ID file abstraction that writes the current pid into a file and optionally
 * removes it on system exit.
 */
public final class PidFile {

    private final long pid;
    private final Path path;
    private final boolean deleteOnExit;

    private PidFile(Path path, boolean deleteOnExit, long pid) throws IOException {
        this.path = path;
        this.deleteOnExit = deleteOnExit;
        this.pid = pid;
    }

    /**
     * Creates a new PidFile and writes the current process ID into the provided path
     *
     * @param path the path to the pid file. The file is newly created or truncated if it already exists
     * @param deleteOnExit if <code>true</code> the pid file is deleted with best effort on system exit
     * @throws IOException if an IOException occurs
     */
    public static PidFile create(Path path, boolean deleteOnExit) throws IOException {
        return create(path, deleteOnExit, JvmInfo.jvmInfo().pid());
    }

    static PidFile create(Path path, boolean deleteOnExit, long pid) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            if (Files.exists(parent) && Files.isDirectory(parent) == false) {
                throw new IllegalArgumentException(parent + " exists but is not a directory");
            }
            if (Files.exists(parent) == false) {
                // only do this if it doesn't exists we get a better exception further down
                // if there are security issues etc. this also doesn't work if the parent exists
                // and is a soft-link like on many linux systems /var/run can be a link and that should
                // not prevent us from writing the PID
                Files.createDirectories(parent);
            }
        }
        if (Files.exists(path) && Files.isRegularFile(path) == false) {
            throw new IllegalArgumentException(path + " exists but is not a regular file");
        }

        try(OutputStream stream = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            stream.write(Long.toString(pid).getBytes(StandardCharsets.UTF_8));
        }

        if (deleteOnExit) {
            addShutdownHook(path);
        }
        return new PidFile(path, deleteOnExit, pid);
    }


    /**
     * Returns the current process id
     */
    public long getPid() {
        return pid;
    }

    /**
     * Returns the process id file path
     */
    public Path getPath() {
        return path;
    }

    /**
     * Returns <code>true</code> iff the process id file is deleted on system exit. Otherwise <code>false</code>.
     */
    public boolean isDeleteOnExit() {
        return deleteOnExit;
    }

    private static void addShutdownHook(final Path path) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new ElasticsearchException("Failed to delete pid file " + path, e);
                }
            }
        });
    }
}
