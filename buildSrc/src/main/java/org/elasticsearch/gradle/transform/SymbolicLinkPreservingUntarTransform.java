/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.transform;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

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

    public void unpack(File tarFile, File targetDir) throws IOException {
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
            }
            if (entry.isSymbolicLink() == false) {
                // check if the underlying file system supports POSIX permissions
                chmod(destination, entry.getMode());
            }
            entry = tar.getNextTarEntry();
        }

    }
}
