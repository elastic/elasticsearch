/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test;

import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.util.Map;

public class MockTextTemplateEngine extends TextTemplateEngine {
    public MockTextTemplateEngine() {
        super(null);
    }

    @Override
    public String render(TextTemplate textTemplate, Map<String, Object> model) {
        if (textTemplate == null ) {
            return null;
        }

        return textTemplate.getTemplate();
    }
}
