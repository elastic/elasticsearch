/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoCollector;
import org.elasticsearch.marvel.agent.collector.node.NodeStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.VersionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

@ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public abstract class AbstractExporterTemplateTestCase extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "-1");

        for (Map.Entry<String, String> setting : exporterSettings().getAsMap().entrySet()) {
            settings.put("marvel.agent.exporters._exporter." + setting.getKey(), setting.getValue());
        }
        return settings.build();
    }

    protected abstract Settings exporterSettings();

    protected abstract void deleteTemplate() throws Exception;

    protected abstract void putTemplate(String version) throws Exception;

    protected abstract void createMarvelIndex(String index) throws Exception;

    protected abstract void assertTemplateUpdated(Version version) throws Exception;

    protected abstract void assertTemplateNotUpdated(Version version) throws Exception;

    protected abstract void assertMappingsUpdated(String... indices) throws Exception;

    protected abstract void assertMappingsNotUpdated(String... indices) throws Exception;

    protected abstract void assertIndicesNotCreated() throws Exception;

    public void testCreateWhenNoExistingTemplate() throws Exception {
        internalCluster().startNode();

        deleteTemplate();
        doExporting();

        logger.debug("--> template does not exist: it should have been created in the current version");
        assertTemplateUpdated(currentVersion());

        doExporting();

        logger.debug("--> mappings should be up-to-date");
        assertMappingsUpdated(currentIndices());
    }

    public void testUpdateWhenExistingTemplateHasNoVersion() throws Exception {
        internalCluster().startNode();

        putTemplate("");
        doExporting();

        logger.debug("--> existing template does not have a version: it should be updated to the current version");
        assertTemplateUpdated(currentVersion());

        doExporting();

        logger.debug("--> mappings should be up-to-date");
        assertMappingsUpdated(currentIndices());
    }

    public void testUpdateWhenExistingTemplateHasWrongVersion() throws Exception {
        internalCluster().startNode();

        putTemplate(randomAsciiOfLength(5));
        doExporting();

        logger.debug("--> existing template has a wrong version: it should be updated to the current version");
        assertTemplateUpdated(currentVersion());

        doExporting();

        logger.debug("--> mappings should be up-to-date");
        assertMappingsUpdated(currentIndices());
    }

    public void testNoUpdateWhenExistingTemplateIsTooOld() throws Exception {
        internalCluster().startNode();

        putTemplate(VersionUtils.getFirstVersion().number());
        doExporting();

        logger.debug("--> existing template is too old: it should not be updated");
        assertTemplateNotUpdated(VersionUtils.getFirstVersion());

        doExporting();

        logger.debug("--> existing template is too old: no data is exported");
        assertIndicesNotCreated();
    }

    public void testUpdateWhenExistingTemplateIsOld() throws Exception {
        internalCluster().startNode();

        putTemplate(VersionUtils.getPreviousVersion(currentVersion()).number());
        doExporting();

        logger.debug("--> existing template is old but supported: it should be updated to the current version");
        assertTemplateUpdated(currentVersion());

        doExporting();

        logger.debug("--> mappings should be up-to-date");
        assertMappingsUpdated(currentIndices());
    }

    public void testUpdateWhenExistingTemplateIsUpToDate() throws Exception {
        internalCluster().startNode();

        putTemplate(currentVersion().toString());
        doExporting();

        logger.debug("--> existing template has the same version: it should not be updated");
        assertTemplateNotUpdated(currentVersion());

        doExporting();

        logger.debug("--> mappings should not have been updated");
        assertMappingsNotUpdated(currentIndices());
    }

    public void testMappingsUpdate() throws Exception {
        boolean updateMappings = randomBoolean();
        logger.debug("--> update_mappings is {}", updateMappings);
        internalCluster().startNode(Settings.builder().put("marvel.agent.exporters._exporter.update_mappings", updateMappings));

        logger.debug("--> putting a template with a very old version so that it will not be updated");
        putTemplate(VersionUtils.getFirstVersion().toString());

        logger.debug("--> creating marvel data index");
        createMarvelIndex(MarvelSettings.MARVEL_DATA_INDEX_NAME);

        logger.debug("--> creating a cold marvel index");
        createMarvelIndex(coldIndex());

        logger.debug("--> creating an active marvel index");
        createMarvelIndex(hotIndex());

        logger.debug("--> all indices have a old mapping now");
        assertMappingsNotUpdated(coldIndex(), hotIndex(), MarvelSettings.MARVEL_DATA_INDEX_NAME);

        logger.debug("--> updating the template with a previous version, so that it will be updated when exporting documents");
        putTemplate(VersionUtils.getPreviousVersion(currentVersion()).number());
        doExporting();

        logger.debug("--> existing template is old: it should be updated to the current version");
        assertTemplateUpdated(currentVersion());

        logger.debug("--> cold marvel index: mappings should not have been updated");
        assertMappingsNotUpdated(coldIndex());

        if (updateMappings) {
            logger.debug("--> marvel indices: mappings should be up-to-date");
            assertMappingsUpdated(MarvelSettings.MARVEL_DATA_INDEX_NAME, hotIndex());
        } else {
            logger.debug("--> marvel indices: mappings should bnot have been updated");
            assertMappingsNotUpdated(MarvelSettings.MARVEL_DATA_INDEX_NAME, hotIndex());
        }
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

    private Version currentVersion() {
        return MarvelTemplateUtils.loadDefaultTemplateVersion();
    }

    private String[] currentIndices() {
        return new String[]{hotIndex(), MarvelSettings.MARVEL_DATA_INDEX_NAME};
    }

    private String coldIndex() {
        return exporter().indexNameResolver().resolve(new DateTime(2012, 3, 10, 0, 0, DateTimeZone.UTC).getMillis());
    }

    private String hotIndex() {
        return exporter().indexNameResolver().resolve(System.currentTimeMillis());
    }

    /** Generates a template that looks like an old one **/
    protected static BytesReference generateTemplateSource(String version) throws IOException {
        return jsonBuilder().startObject()
                                .field("template", ".marvel-es-*")
                                .startObject("settings")
                                    .field("index.number_of_shards", 1)
                                    .field("index.number_of_replicas", 1)
                                    .field("index.mapper.dynamic", false)
                                    .field(MarvelTemplateUtils.MARVEL_VERSION_FIELD, version)
                                .endObject()
                                .startObject("mappings")
                                    .startObject("_default_")
                                        .startObject("_all")
                                            .field("enabled", false)
                                        .endObject()
                                        .field("date_detection", false)
                                        .startObject("properties")
                                            .startObject("cluster_uuid")
                                                .field("type", "string")
                                                .field("index", "not_analyzed")
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
                                    .startObject("node_stats")
                                        .startObject("properties")
                                            .startObject("node_stats")
                                                .field("type", "object")
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endObject().bytes();
    }
}
