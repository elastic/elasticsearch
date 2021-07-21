/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.transform;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.gradle.api.artifacts.transform.TransformOutputs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static org.elasticsearch.gradle.util.PermissionUtils.chmod;

public abstract class SymbolicLinkPreservingUntarTransform implements UnpackTransform {

    private static final Path CURRENT_DIR_PATH = Paths.get(".");

    public void unpack(File tarFile, File targetDir, TransformOutputs outputs, boolean asFiletreeOutput) throws IOException {
        Function<String, Path> pathModifier = pathResolver();
        TarArchiveInputStream tar = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(tarFile)));
        final Path destinationPath = targetDir.toPath();
        TarArchiveEntry entry = tar.getNextTarEntry();
        while (entry != null) {
            final Path relativePath = pathModifier.apply(entry.getName());
            if (relativePath == null || relativePath.getFileName().equals(CURRENT_DIR_PATH)) {
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
                if (asFiletreeOutput) {
                    outputs.file(destination.toFile());
                }
            }
            if (entry.isSymbolicLink() == false) {
                // check if the underlying file system supports POSIX permissions
                chmod(destination, entry.getMode());
            }
            entry = tar.getNextTarEntry();
        }
    }
}
