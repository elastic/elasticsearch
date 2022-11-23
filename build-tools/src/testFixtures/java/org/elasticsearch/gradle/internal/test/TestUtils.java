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
import java.util.stream.Collectors;

public class TestUtils {

    public static String normalizeString(String input, File projectRootDir) {
        try {
            String cannonicalNormalizedPathPrefix = projectRootDir.getCanonicalPath().replace("\\", "/");
            String normalizedPathPrefix = projectRootDir.getAbsolutePath().replace("\\", "/");
            return input.lines()
                .filter(it -> it.startsWith("Picked up JAVA_TOOL_OPTIONS") == false)
                .map(it -> it.replace("\\", "/"))
                .map(it -> it.replaceAll("\\d+\\.\\d\\ds", "0.00s"))
                .map(it -> it.replaceAll(cannonicalNormalizedPathPrefix, "."))
                .map(it -> it.replaceAll(normalizedPathPrefix, "."))
                .map(it -> it.replaceAll("Gradle Test Executor \\d", "Gradle Test Executor 1"))
                .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
