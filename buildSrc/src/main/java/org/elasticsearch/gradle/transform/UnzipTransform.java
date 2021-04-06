/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.transform;

import org.apache.commons.io.IOUtils;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipFile;
import org.gradle.api.artifacts.transform.TransformOutputs;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.function.Function;

import static org.elasticsearch.gradle.util.PermissionUtils.chmod;

public abstract class UnzipTransform implements UnpackTransform {

    public void unpack(File zipFile, File targetDir, TransformOutputs outputs, boolean asFiletreeOutput) throws IOException {
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
                if (asFiletreeOutput) {
                    outputs.file(outputPath.toFile());
                }
            }
        } finally {
            zip.close();
        }
    }

}
