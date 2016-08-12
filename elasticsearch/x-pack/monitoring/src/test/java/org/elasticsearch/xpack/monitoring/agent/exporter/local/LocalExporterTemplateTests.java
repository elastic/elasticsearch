/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.agent.exporter.local;

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xpack.monitoring.agent.exporter.AbstractExporterTemplateTestCase;
import org.elasticsearch.xpack.monitoring.agent.exporter.Exporter;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

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
    protected void deletePipeline() throws Exception {
        waitNoPendingTasksOnAll();
        cluster().client().admin().cluster().deletePipeline(new DeletePipelineRequest(Exporter.EXPORT_PIPELINE_NAME));
    }

    @Override
    protected void putTemplate(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().indices().preparePutTemplate(name).setSource(generateTemplateSource(name)).get());
    }

    @Override
    protected void putPipeline(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().cluster().preparePutPipeline(name, Exporter.emptyPipeline(XContentType.JSON).bytes()).get());
    }

    @Override
    protected void assertTemplateExists(String name) throws Exception {
        waitNoPendingTasksOnAll();
        waitForMonitoringTemplate(name);
    }

    @Override
    protected void assertPipelineExists(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertPipelineInstalled(name);
    }

    private void assertPipelineInstalled(String name) throws Exception {
        assertBusy(() -> {
            boolean found = false;
            for (PipelineConfiguration pipeline : client().admin().cluster().prepareGetPipeline(name).get().pipelines()) {
                if (Regex.simpleMatch(name, pipeline.getId())) {
                    found =  true;
                }
            }
            assertTrue("failed to find a pipeline matching [" + name + "]", found);
        }, 60, TimeUnit.SECONDS);
    }

    @Override
    protected void assertTemplateNotUpdated(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertTemplateExists(name);
    }

    @Override
    protected void assertPipelineNotUpdated(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertPipelineExists(name);
    }
}
