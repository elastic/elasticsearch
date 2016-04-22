/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.template.TemplateUtilsTests.assertTemplate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

public class MarvelTemplateUtilsTests extends ESTestCase {

    public void testLoadTemplate() throws IOException {
        String source = MarvelTemplateUtils.loadTemplate("test");

        assertThat(source, notNullValue());
        assertThat(source.length(), greaterThan(0));
        assertTemplate(source, equalTo("{\n" +
                "  \"template\": \".monitoring-data-" + MarvelTemplateUtils.TEMPLATE_VERSION + "\",\n" +
                "  \"mappings\": {\n" +
                "    \"type_1\": {\n" +
                "      \"_meta\": {\n" +
                "        \"template.version\": \"" + MarvelTemplateUtils.TEMPLATE_VERSION + "\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}"));
    }
}
