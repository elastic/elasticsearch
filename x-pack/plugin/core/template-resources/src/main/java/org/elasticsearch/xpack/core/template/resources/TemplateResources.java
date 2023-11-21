/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template.resources;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * TemplateResources is a bridge to xpack template resource files. This class should only be used within x-pack core.
 */
public class TemplateResources {

    private TemplateResources() {}

    /**
     * Returns a template resource for the given resource path.
     * @throws IOException if the resource name is not found
     */
    public static String load(String name) throws IOException {
        try (InputStream is = TemplateResources.class.getResourceAsStream(name)) {
            if (is == null) {
                throw new IOException("Template [" + name + "] not found in x-pack template resources.");
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
