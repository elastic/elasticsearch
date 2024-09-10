/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.xpack.core.template.resources.TemplateResources;

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
            String content = loadResourceWithFallback(clazz, name);
            content = TemplateUtils.replaceVariables(content, String.valueOf(version), versionProperty, variables);
            return content.getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * loadResourceWithFallback loads template resources from TemplateResource class. It expects
     * the loader to be a named Java module and the template resources to contain required templates
     * with a folder named with the module name. If the specified resource name is not found in
     * template resource then the plugin's resources is used as a fallback.
     *
     * @param clazz the runtime class of the source plugin
     * @param name the relative path of the resource with leading `/`
     * @return the loaded resource as string
     * @throws IOException
     */
    static String loadResourceWithFallback(Class<?> clazz, String name) throws IOException {
        InputStream is = null;
        if (clazz.getModule().isNamed()) {
            is = TemplateResources.class.getResourceAsStream("/" + clazz.getModule().getName() + name);
        }
        if (is == null) {
            is = clazz.getResourceAsStream(name);
        }
        if (is == null) {
            throw new IOException("Resource [" + name + "] not found in classpath.");
        }
        return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    }

}
