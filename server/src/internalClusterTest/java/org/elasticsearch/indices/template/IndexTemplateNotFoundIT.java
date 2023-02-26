/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.template;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.get.TemplateNotFoundException;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;

public class IndexTemplateNotFoundIT extends ESSingleNodeTestCase {

    public void testTemplateNotFound() {
        getNotFoundTemplateWithException("template_not_found", "template_not_found");

        createTemplate("template_1");
        createTemplate("template_2");

        assertEquals("template_1",
            getTemplate("template_1").getIndexTemplates().get(0).getName());

        assertEquals(2, getTemplate("template_*").getIndexTemplates().size());

        getNotFoundTemplateWithException("template_not_found", "template_1", "template_2", "template_not_found");
    }

    private void getNotFoundTemplateWithException(String notFoundTemplate, String... name) {
        // expect exception throws
        expectThrows(TemplateNotFoundException.class,
            () -> getTemplate(name));

        // expect exception message correct
        try {
            getTemplate(name);
        } catch (TemplateNotFoundException e) {
            assertEquals(notFoundTemplate, e.getTemplate());
            assertEquals("no such template [" + notFoundTemplate +"]", e.getMessage());
        }
    }

    private void createTemplate(String template) {
        client().admin().indices().preparePutTemplate(template)
            .setPatterns(Collections.singletonList("te*"))
            .setMapping("type", "type=keyword", "field", "type=text").get();
    }

    private GetIndexTemplatesResponse getTemplate(String... name) {
        return client().admin().indices().prepareGetTemplates(name).get();
    }
}
