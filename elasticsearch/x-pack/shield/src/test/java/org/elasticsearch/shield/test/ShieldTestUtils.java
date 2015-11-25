/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.test;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class ShieldTestUtils {

    public static Path createFolder(Path parent, String name) {
        Path createdFolder = parent.resolve(name);
        //the directory might exist e.g. if the global cluster gets restarted, then we recreate the directory as well
        if (Files.exists(createdFolder)) {
            try {
                FileSystemUtils.deleteSubDirectories(createdFolder);
            } catch (IOException e) {
                throw new RuntimeException("could not delete existing temporary folder: " + createdFolder.toAbsolutePath(), e);
            }
        } else {
            try {
                Files.createDirectory(createdFolder);
            } catch (IOException e) {
                throw new RuntimeException("could not create temporary folder: " + createdFolder.toAbsolutePath());
            }
        }
        return createdFolder;
    }

    public static String writeFile(Path folder, String name, byte[] content) {
        Path file = folder.resolve(name);
        try (OutputStream os = Files.newOutputStream(file)) {
            Streams.copy(content, os);
        } catch (IOException e) {
            throw new ElasticsearchException("error writing file in test", e);
        }
        return file.toAbsolutePath().toString();
    }

    public static String writeFile(Path folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(StandardCharsets.UTF_8));
    }
}
