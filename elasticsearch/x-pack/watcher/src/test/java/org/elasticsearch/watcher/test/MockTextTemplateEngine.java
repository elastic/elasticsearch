/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test;

import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.util.Map;

public class MockTextTemplateEngine implements TextTemplateEngine {
    @Override
    public String render(TextTemplate template, Map<String, Object> model) {
        if (template == null ) {
            return null;
        }

        return template.getTemplate();
    }
}
