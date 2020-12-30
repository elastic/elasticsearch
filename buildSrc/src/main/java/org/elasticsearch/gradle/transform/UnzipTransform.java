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
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.function.Function;

import static org.elasticsearch.gradle.util.PermissionUtils.chmod;

public abstract class UnzipTransform implements UnpackTransform {

    public void unpack(File zipFile, File targetDir) throws IOException {
        Logging.getLogger(UnzipTransform.class)
            .info("Unpacking " + zipFile.getName() + " using " + UnzipTransform.class.getSimpleName() + ".");
        Function<String, Path> pathModifier = pathResolver();
        ZipFile zip = new ZipFile(zipFile);
        try {
            Enumeration<ZipEntry> entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                Path child = pathModifier.apply(zipEntry.getName());
                if (child == null) {
                    continue;
                }
                Path outputPath = targetDir.toPath().resolve(child);
                if (zipEntry.isDirectory()) {
                    outputPath.toFile().mkdirs();
                    chmod(outputPath, zipEntry.getUnixMode());
                    continue;
                }
                try (FileOutputStream outputStream = new FileOutputStream(outputPath.toFile())) {
                    IOUtils.copyLarge(zip.getInputStream(zipEntry), outputStream);
                }
                chmod(outputPath, zipEntry.getUnixMode());
            }
        } finally {
            zip.close();
        }
    }

}
