/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoCollector;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.dataTemplateName;
import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.indexTemplateName;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractExporterTemplateTestCase extends MarvelIntegTestCase {

    private final Integer currentVersion = MarvelTemplateUtils.TEMPLATE_VERSION;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL_SETTING.getKey(), "-1");

        for (Map.Entry<String, String> setting : exporterSettings().getAsMap().entrySet()) {
            settings.put("marvel.agent.exporters._exporter." + setting.getKey(), setting.getValue());
        }
        return settings.build();
    }

    protected abstract Settings exporterSettings();

    protected abstract void deleteTemplates() throws Exception;

    protected abstract void putTemplate(String name, int version) throws Exception;

    protected abstract void assertTemplateExist(String name) throws Exception;

    protected abstract void assertTemplateNotUpdated(String name) throws Exception;

    public void testCreateWhenNoExistingTemplates() throws Exception {
        internalCluster().startNode();

        deleteTemplates();
        doExporting();

        logger.debug("--> templates does not exist: it should have been created in the current version");
        assertTemplateExist(indexTemplateName());
        assertTemplateExist(dataTemplateName());

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    public void testCreateWhenExistingTemplatesAreOld() throws Exception {
        internalCluster().startNode();

        final Integer version = randomIntBetween(0, currentVersion - 1);
        putTemplate(indexTemplateName(version), version);
        putTemplate(dataTemplateName(version), version);

        doExporting();

        logger.debug("--> existing templates are old");
        assertTemplateExist(dataTemplateName(version));
        assertTemplateExist(indexTemplateName(version));

        logger.debug("--> existing templates are old: new templates should be created");
        assertTemplateExist(indexTemplateName());
        assertTemplateExist(dataTemplateName());

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    public void testCreateWhenExistingTemplateAreUpToDate() throws Exception {
        internalCluster().startNode();

        putTemplate(indexTemplateName(currentVersion), currentVersion);
        putTemplate(dataTemplateName(currentVersion), currentVersion);

        doExporting();

        logger.debug("--> existing templates are up to date");
        assertTemplateExist(indexTemplateName());
        assertTemplateExist(dataTemplateName());

        logger.debug("--> existing templates has the same version: they should not be changed");
        assertTemplateNotUpdated(indexTemplateName());
        assertTemplateNotUpdated(dataTemplateName());

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    public void testRandomTemplates() throws Exception {
        internalCluster().startNode();

        int previousIndexTemplateVersion = rarely() ? currentVersion : randomIntBetween(0, currentVersion - 1);
        boolean previousIndexTemplateExist = randomBoolean();
        if (previousIndexTemplateExist) {
            logger.debug("--> creating index template in version [{}]", previousIndexTemplateVersion);
            putTemplate(indexTemplateName(previousIndexTemplateVersion), previousIndexTemplateVersion);
        }

        int previousDataTemplateVersion = rarely() ? currentVersion : randomIntBetween(0, currentVersion - 1);
        boolean previousDataTemplateExist = randomBoolean();
        if (previousDataTemplateExist) {
            logger.debug("--> creating data template in version [{}]", previousDataTemplateVersion);
            putTemplate(dataTemplateName(previousDataTemplateVersion), previousDataTemplateVersion);
        }

        doExporting();

        logger.debug("--> templates should exist in current version");
        assertTemplateExist(indexTemplateName());
        assertTemplateExist(dataTemplateName());

        if (previousIndexTemplateExist) {
            logger.debug("--> index template should exist in version [{}]", previousIndexTemplateVersion);
            assertTemplateExist(indexTemplateName(previousIndexTemplateVersion));
        }

        if (previousDataTemplateExist) {
            logger.debug("--> data template should exist in version [{}]", previousDataTemplateVersion);
            assertTemplateExist(dataTemplateName(previousDataTemplateVersion));
        }

        doExporting();

        logger.debug("--> indices should exist in current version");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    protected void doExporting() throws Exception {
        List<MarvelDoc> docs = new ArrayList<>();
        for (Class<? extends Collector> collectorClass : Arrays.asList(ClusterInfoCollector.class, NodeStatsCollector.class)) {
            Collector collector = internalCluster().getInstance(collectorClass);
            docs.addAll(collector.collect());
        }
        exporter().export(docs);
    }

    private Exporter exporter() {
        Exporters exporters = internalCluster().getInstance(Exporters.class);
        return exporters.iterator().next();
    }

    private String currentDataIndexName() {
        return ".marvel-es-data-" + String.valueOf(currentVersion);
    }

    private String currentTimestampedIndexName() {
        return exporter().indexNameResolver().resolve(System.currentTimeMillis());
    }

    /** Generates a basic template **/
    protected static BytesReference generateTemplateSource(String name, Integer version) throws IOException {
        return jsonBuilder().startObject()
                                .field("template", name)
                                .startObject("settings")
                                    .field("index.number_of_shards", 1)
                                    .field("index.number_of_replicas", 1)
                                    .field(MarvelTemplateUtils.VERSION_FIELD, String.valueOf(version))
                                .endObject()
                .endObject().bytes();
    }
}
