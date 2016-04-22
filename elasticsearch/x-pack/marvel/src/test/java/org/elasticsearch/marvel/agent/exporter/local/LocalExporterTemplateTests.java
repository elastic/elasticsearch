/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.exporter.AbstractExporterTemplateTestCase;

import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class LocalExporterTemplateTests extends AbstractExporterTemplateTestCase {

    @Override
    protected Settings exporterSettings() {
        return Settings.builder().put("type", LocalExporter.TYPE).build();
    }

    @Override
    protected void deleteTemplates() throws Exception {
        waitNoPendingTasksOnAll();
        cluster().wipeAllTemplates(Collections.emptySet());
    }

    @Override
    protected void putTemplate(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().indices().preparePutTemplate(name).setSource(generateTemplateSource(name)).get());
    }

    @Override
    protected void assertTemplateExist(String name) throws Exception {
        waitNoPendingTasksOnAll();
        waitForMarvelTemplate(name);
    }

    @Override
    protected void assertTemplateNotUpdated(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertTemplateExist(name);
    }
}
