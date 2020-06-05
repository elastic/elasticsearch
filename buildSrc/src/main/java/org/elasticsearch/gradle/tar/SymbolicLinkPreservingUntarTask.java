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

package org.elasticsearch.gradle.tar;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.function.Function;

/**
 * A custom task that explodes a tar archive that preserves symbolic links.
 *
 * This task is necessary because the built-in task {@link org.gradle.api.internal.file.archive.TarFileTree} does not preserve symbolic
 * links.
 */
public class SymbolicLinkPreservingUntarTask extends DefaultTask {

    private final RegularFileProperty tarFile;

    @InputFile
    public RegularFileProperty getTarFile() {
        return tarFile;
    }

    private final DirectoryProperty extractPath;

    @OutputDirectory
    public DirectoryProperty getExtractPath() {
        return extractPath;
    }

    private Function<String, Path> transform;

    @Internal
    public Function<String, Path> getTransform() {
        return transform;
    }

    /**
     * A transform to apply to the tar entry, to derive the relative path from the entry name. If the return value is null, the entry is
     * dropped from the exploded tar archive.
     *
     * @param transform the transform
     */
    public void setTransform(Function<String, Path> transform) {
        this.transform = transform;
    }

    @Inject
    public SymbolicLinkPreservingUntarTask(final ObjectFactory objectFactory) {
        this.tarFile = objectFactory.fileProperty();
        this.extractPath = objectFactory.directoryProperty();
        this.transform = name -> Paths.get(name);
    }

    @TaskAction
    final void execute() {
        // ensure the target extraction path is empty
        getProject().delete(extractPath);
        try (
            TarArchiveInputStream tar = new TarArchiveInputStream(
                new GzipCompressorInputStream(new FileInputStream(tarFile.getAsFile().get()))
            )
        ) {
            final Path destinationPath = extractPath.get().getAsFile().toPath();
            TarArchiveEntry entry = tar.getNextTarEntry();
            while (entry != null) {
                final Path relativePath = transform.apply(entry.getName());
                if (relativePath == null) {
                    entry = tar.getNextTarEntry();
                    continue;
                }

                final Path destination = destinationPath.resolve(relativePath);
                final Path parent = destination.getParent();
                if (Files.exists(parent) == false) {
                    Files.createDirectories(parent);
                }
                if (entry.isDirectory()) {
                    Files.createDirectory(destination);
                } else if (entry.isSymbolicLink()) {
                    Files.createSymbolicLink(destination, Paths.get(entry.getLinkName()));
                } else {
                    // copy the file from the archive using a small buffer to avoid heaping
                    Files.createFile(destination);
                    try (FileOutputStream fos = new FileOutputStream(destination.toFile())) {
                        tar.transferTo(fos);
                    }
                }
                if (entry.isSymbolicLink() == false) {
                    // check if the underlying file system supports POSIX permissions
                    final PosixFileAttributeView view = Files.getFileAttributeView(destination, PosixFileAttributeView.class);
                    if (view != null) {
                        final Set<PosixFilePermission> permissions = PosixFilePermissions.fromString(
                            permissions((entry.getMode() >> 6) & 07) + permissions((entry.getMode() >> 3) & 07) + permissions(
                                (entry.getMode() >> 0) & 07
                            )
                        );
                        Files.setPosixFilePermissions(destination, permissions);
                    }
                }
                entry = tar.getNextTarEntry();
            }
        } catch (final IOException e) {
            throw new GradleException("unable to extract tar [" + tarFile.getAsFile().get().toPath() + "]", e);
        }
    }

    private String permissions(final int permissions) {
        if (permissions < 0 || permissions > 7) {
            throw new IllegalArgumentException("permissions [" + permissions + "] out of range");
        }
        final StringBuilder sb = new StringBuilder(3);
        if ((permissions & 4) == 4) {
            sb.append('r');
        } else {
            sb.append('-');
        }
        if ((permissions & 2) == 2) {
            sb.append('w');
        } else {
            sb.append('-');
        }
        if ((permissions & 1) == 1) {
            sb.append('x');
        } else {
            sb.append('-');
        }
        return sb.toString();
    }

}
