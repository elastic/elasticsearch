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

import org.apache.commons.io.IOUtils;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logging;
import org.gradle.internal.nativeintegration.filesystem.jdk7.PosixFilePermissionConverter;
import shadow.org.apache.tools.zip.ZipEntry;
import shadow.org.apache.tools.zip.ZipFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.Enumeration;

public abstract class UnzipTransform implements UnpackTransform {

    public void unpack(File zipFile, File targetDir) throws IOException {
        Logging.getLogger(UnzipTransform.class)
            .info("Unpacking " + zipFile.getName() + " using " + UnzipTransform.class.getSimpleName() + ".");

        ZipFile zip = new ZipFile(zipFile);
        try {
            Enumeration<ZipEntry> entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                String child = maybeTrim(zipEntry);
                if (child == null) {
                    continue;
                }
                File outFile = new File(targetDir, child);
                if (zipEntry.isDirectory()) {
                    outFile.mkdirs();
                    chmod(outFile, zipEntry.getUnixMode());
                    continue;
                }
                try (FileOutputStream outputStream = new FileOutputStream(outFile)) {
                    IOUtils.copyLarge(zip.getInputStream(zipEntry), outputStream);
                }
                chmod(outFile, zipEntry.getUnixMode());
            }
        } finally {
            zip.close();
        }
    }

    private void chmod(File f, int mode) {
        try {
            PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(f.toPath(), PosixFileAttributeView.class);
            fileAttributeView.setPermissions(PosixFilePermissionConverter.convertToPermissionsSet(0777 & mode));
        } catch (IOException ioException) {
            throw new GradleException("Cannot set file permissions", ioException);
        }
    }

    protected String maybeTrim(ZipEntry entry) {
        return entry.getName();
    }

}
