/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class ResourceUtils {

    public static Path copyResourceToFile(Class<?> clazz, Path targetFolder, String resourcePath) {
        try {
            ClassLoader classLoader = clazz.getClassLoader();
            URL resourceUrl = classLoader.getResource(resourcePath);
            if (resourceUrl == null) {
                throw new RuntimeException("Failed to load " + resourcePath + " from classpath");
            }
            InputStream inputStream = resourceUrl.openStream();
            File outputFile = new File(targetFolder.toFile(), resourcePath.substring(resourcePath.lastIndexOf('/')));
            Files.copy(inputStream, outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return outputFile.toPath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load ca.jks from classpath", e);
        }
    }
}
