/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class TestUtils {

    public static void setupJarJdkClasspath(File projectRoot) {
        try {
            URL originLocation = TestUtils.class.getClassLoader()
                .loadClass("org.elasticsearch.jdk.JdkJarHellCheck")
                .getProtectionDomain()
                .getCodeSource()
                .getLocation();
            File targetFile = new File(
                projectRoot,
                "sample_jars/build/testrepo/org/elasticsearch/elasticsearch-core/current/elasticsearch-core-current.jar"
            );
            targetFile.getParentFile().mkdirs();
            Path originalPath = Paths.get(originLocation.toURI());
            Files.copy(originalPath, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (ClassNotFoundException | URISyntaxException | IOException e) {
            e.printStackTrace();
            fail("Cannot setup jdk jar hell classpath");
        }
    }

    public static String normalizeString(String input, File projectRootDir) {
        try {
            String normalizedPathPrefix = projectRootDir.getCanonicalPath().replaceAll("\\\\", "/");
            System.out.println("normalizedPathPrefix = " + normalizedPathPrefix);
            return input.lines()
                    .map(it -> it.replaceAll("\\\\", "/"))
                    .map(it -> it.replaceAll(normalizedPathPrefix, "."))
                    .map(it -> it.replaceAll("Gradle Test Executor \\d", "Gradle Test Executor 1"))
                    .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
