/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import groovy.text.SimpleTemplateEngine;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

/**
 * Methods for working with Groovy templates.
 */
public class TemplateUtils {

    /**
     * Applies {@code bindings} to {@code template}, then removes all carriage returns from
     * the result.
     *
     * @param template a Groovy template
     * @param bindings parameters for the template
     * @return the rendered template
     */
    public static String render(String template, Map<String, Object> bindings) throws IOException {
        final StringWriter writer = new StringWriter();

        try {
            final SimpleTemplateEngine engine = new SimpleTemplateEngine();
            engine.createTemplate(template).make(bindings).writeTo(writer);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return writer.toString().replace("\\r", "");
    }

}
