/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

//import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.MARVEL_VERSION_FIELD;

public class MarvelTemplateUtilsTests extends ESTestCase {

    public void testLoadTimestampedIndexTemplate() {
        byte[] template = MarvelTemplateUtils.loadTimestampedIndexTemplate();
        assertNotNull(template);
        assertThat(template.length, greaterThan(0));
    }

    public void testLoadDataIndexTemplate() {
        byte[] template = MarvelTemplateUtils.loadDataIndexTemplate();
        assertNotNull(template);
        assertThat(template.length, greaterThan(0));
    }

    public void testLoad() throws IOException {
        String resource = randomFrom(MarvelTemplateUtils.INDEX_TEMPLATE_FILE, MarvelTemplateUtils.DATA_TEMPLATE_FILE);
        byte[] template = MarvelTemplateUtils.load(resource);
        assertNotNull(template);
        assertThat(template.length, greaterThan(0));
    }

    public void testLoadTemplateVersion() {
        Integer version = MarvelTemplateUtils.loadTemplateVersion();
        assertNotNull(version);
        assertThat(version, greaterThan(0));
        assertThat(version, equalTo(MarvelTemplateUtils.TEMPLATE_VERSION));
    }

    public void testIndexTemplateName() {
        assertThat(MarvelTemplateUtils.indexTemplateName(),
                equalTo(MarvelTemplateUtils.INDEX_TEMPLATE_NAME_PREFIX + MarvelTemplateUtils.TEMPLATE_VERSION));
        int version = randomIntBetween(1, 100);
        assertThat(MarvelTemplateUtils.indexTemplateName(version), equalTo(".marvel-es-" + version));
    }

    public void testDataTemplateName() {
        assertThat(MarvelTemplateUtils.dataTemplateName(),
                equalTo(MarvelTemplateUtils.DATA_TEMPLATE_NAME_PREFIX + MarvelTemplateUtils.TEMPLATE_VERSION));
        int version = randomIntBetween(1, 100);
        assertThat(MarvelTemplateUtils.dataTemplateName(version), equalTo(".marvel-es-data-" + version));
    }
}
