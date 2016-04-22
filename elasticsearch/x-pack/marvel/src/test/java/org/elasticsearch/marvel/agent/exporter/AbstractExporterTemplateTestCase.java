/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractExporterTemplateTestCase extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1");

        for (Map.Entry<String, String> setting : exporterSettings().getAsMap().entrySet()) {
            settings.put("xpack.monitoring.agent.exporters._exporter." + setting.getKey(), setting.getValue());
        }
        return settings.build();
    }

    protected abstract Settings exporterSettings();

    protected abstract void deleteTemplates() throws Exception;

    protected abstract void putTemplate(String name) throws Exception;

    protected abstract void assertTemplateExist(String name) throws Exception;

    protected abstract void assertTemplateNotUpdated(String name) throws Exception;

    public void testCreateWhenNoExistingTemplates() throws Exception {
        internalCluster().startNode();

        deleteTemplates();
        doExporting();

        logger.debug("--> templates does not exist: it should have been created in the current version");
        for (String template : monitoringTemplates().keySet()) {
            assertTemplateExist(template);
        }

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    public void testCreateWhenExistingTemplatesAreOld() throws Exception {
        internalCluster().startNode();

        putTemplate(indexTemplateName());
        putTemplate(dataTemplateName());

        doExporting();

        logger.debug("--> existing templates are old");
        assertTemplateExist(dataTemplateName());
        assertTemplateExist(indexTemplateName());

        logger.debug("--> existing templates are old: new templates should be created");
        for (String template : monitoringTemplates().keySet()) {
            assertTemplateExist(template);
        }

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    public void testCreateWhenExistingTemplateAreUpToDate() throws Exception {
        internalCluster().startNode();

        putTemplate(indexTemplateName());
        putTemplate(dataTemplateName());

        doExporting();

        logger.debug("--> existing templates are up to date");
        for (String template : monitoringTemplates().keySet()) {
            assertTemplateExist(template);
        }

        logger.debug("--> existing templates has the same version: they should not be changed");
        assertTemplateNotUpdated(indexTemplateName());
        assertTemplateNotUpdated(dataTemplateName());

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    protected void doExporting() throws Exception {
        Collector collector = internalCluster().getInstance(ClusterStatsCollector.class);
        assertNotNull(collector);

        Exporters exporters = internalCluster().getInstance(Exporters.class);
        assertNotNull(exporters);

        // Wait for exporting bulks to be ready to export
        assertBusy(() -> assertThat(exporters.openBulk(), notNullValue()));
        exporters.export(collector.collect());
    }

    private String dataTemplateName() {
        MockDataIndexNameResolver resolver = new MockDataIndexNameResolver();
        return resolver.templateName();
    }

    private String indexTemplateName() {
        MockTimestampedIndexNameResolver resolver = new MockTimestampedIndexNameResolver(MonitoredSystem.ES, exporterSettings());
        return resolver.templateName();
    }

    private String currentDataIndexName() {
        MockDataIndexNameResolver resolver = new MockDataIndexNameResolver();
        return resolver.index(null);
    }

    private String currentTimestampedIndexName() {
        MonitoringDoc doc = new MonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
        doc.setTimestamp(System.currentTimeMillis());

        MockTimestampedIndexNameResolver resolver = new MockTimestampedIndexNameResolver(MonitoredSystem.ES, exporterSettings());
        return resolver.index(doc);
    }

    /** Generates a basic template **/
    protected static BytesReference generateTemplateSource(String name) throws IOException {
        return jsonBuilder().startObject()
                                .field("template", name)
                                .startObject("settings")
                                    .field("index.number_of_shards", 1)
                                    .field("index.number_of_replicas", 1)
                                .endObject()
                                .startObject("mappings")
                                    .startObject("_default_")
                                        .startObject("_all")
                                            .field("enabled", false)
                                        .endObject()
                                        .field("date_detection", false)
                                        .startObject("properties")
                                            .startObject("cluster_uuid")
                                                .field("type", "keyword")
                                            .endObject()
                                            .startObject("timestamp")
                                                .field("type", "date")
                                                .field("format", "date_time")
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                    .startObject("cluster_info")
                                        .field("enabled", false)
                                    .endObject()
                                    .startObject("cluster_stats")
                                        .startObject("properties")
                                            .startObject("cluster_stats")
                                                .field("type", "object")
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject().bytes();
    }
}
