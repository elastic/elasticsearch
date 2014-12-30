/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.test;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Streams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ShieldTestUtils {

    public static String writeFile(File folder, String name, byte[] content) {
        Path file = folder.toPath().resolve(name);
        try {
            Streams.copy(content, file.toFile());
        } catch (IOException e) {
            throw new ElasticsearchException("Error writing file in test", e);
        }
        return file.toFile().getAbsolutePath();
    }

    public static String writeFile(File folder, String name, String content) {
        return writeFile(folder, name, content.getBytes(Charsets.UTF_8));
    }
}
