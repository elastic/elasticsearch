/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ResourceUtils {

    static byte[] loadVersionedResourceUTF8(Class<?> clazz, String name, int version, String versionProperty) {
        return loadVersionedResourceUTF8(clazz, name, version, versionProperty, Map.of());
    }

    static byte[] loadVersionedResourceUTF8(
        Class<?> clazz,
        String name,
        int version,
        String versionProperty,
        Map<String, String> variables
    ) {
        try {
            String content = loadResource(clazz, name);
            content = TemplateUtils.replaceVariables(content, String.valueOf(version), versionProperty, variables);
            return content.getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String loadResource(Class<?> clazz, String name) throws IOException {
        try (InputStream is = clazz.getResourceAsStream(name)) {
            if (is == null) {
                throw new IOException("Resource [" + name + "] not found in classpath.");
            }
            return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

}
