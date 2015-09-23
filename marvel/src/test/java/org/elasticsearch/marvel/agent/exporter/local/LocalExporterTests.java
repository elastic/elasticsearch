/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateCollector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMarvelDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryCollector;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMarvelDoc;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.marvel.agent.exporter.http.HttpExporter.MIN_SUPPORTED_TEMPLATE_VERSION;
import static org.elasticsearch.marvel.agent.exporter.http.HttpExporterUtils.MARVEL_VERSION_FIELD;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class LocalExporterTests extends MarvelIntegTestCase {

    private final static AtomicLong timeStampGenerator = new AtomicLong();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.STARTUP_DELAY, "1h")
                .build();
    }

    @Test
    public void testSimpleExport() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("marvel.agent.exporters._local.type", LocalExporter.TYPE)
                .put("marvel.agent.exporters._local.enabled", true)
                .build());
        ensureGreen();

        Exporter exporter = getExporter("_local");

        logger.debug("--> exporting a single marvel doc");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        assertMarvelDocsCount(1);

        wipeMarvelIndices();

        final List<MarvelDoc> marvelDocs = new ArrayList<>();
        for (int i=0; i < randomIntBetween(2, 50); i++) {
            marvelDocs.add(newRandomMarvelDoc());
        }

        logger.debug("--> exporting {} marvel docs", marvelDocs.size());
        exporter.export(marvelDocs);
        assertMarvelDocsCount(marvelDocs.size());

        SearchResponse response = client().prepareSearch(MarvelSettings.MARVEL_INDICES_PREFIX + "*").get();
        for (SearchHit hit : response.getHits().hits()) {
            Map<String, Object> source = hit.sourceAsMap();
            assertNotNull(source.get("cluster_uuid"));
            assertNotNull(source.get("timestamp"));
        }
    }

    @Test
    public void testTemplateCreation() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("marvel.agent.exporters._local.type", LocalExporter.TYPE)
                .build());
        ensureGreen();

        LocalExporter exporter = getExporter("_local");
        assertTrue(exporter.shouldUpdateTemplate(null, Version.CURRENT));

        assertMarvelTemplateNotExists();

        logger.debug("--> exporting when the marvel template does not exists: template should be created");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        assertMarvelDocsCount(1);
        assertMarvelTemplateExists();

        assertThat(exporter.templateVersion(), equalTo(Version.CURRENT));
    }

    @Test
    public void testTemplateUpdate() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("marvel.agent.exporters._local.type", LocalExporter.TYPE)
                .build());
        ensureGreen();

        LocalExporter exporter = getExporter("_local");
        Version fakeVersion = MIN_SUPPORTED_TEMPLATE_VERSION;
        assertTrue(exporter.shouldUpdateTemplate(fakeVersion, Version.CURRENT));

        logger.debug("--> creating the marvel template with a fake version [{}]", fakeVersion);
        exporter.putTemplate(Settings.builder().put(MARVEL_VERSION_FIELD, fakeVersion.toString()).build());
        assertMarvelTemplateExists();

        assertThat(exporter.templateVersion(), equalTo(fakeVersion));

        logger.debug("--> exporting when the marvel template must be updated: document is exported and the template is updated");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        assertMarvelDocsCount(1);
        assertMarvelTemplateExists();

        assertThat(exporter.templateVersion(), equalTo(Version.CURRENT));
    }

    @Test
    public void testUnsupportedTemplateVersion() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("marvel.agent.exporters._local.type", LocalExporter.TYPE)
                .build());
        ensureGreen();

        LocalExporter exporter = getExporter("_local");
        Version fakeVersion = randomFrom(Version.V_0_18_0, Version.V_1_0_0, Version.V_1_4_0);
        assertFalse(exporter.shouldUpdateTemplate(fakeVersion, Version.CURRENT));

        logger.debug("--> creating the marvel template with a fake version [{}]", fakeVersion);
        exporter.putTemplate(Settings.builder().put(MARVEL_VERSION_FIELD, fakeVersion.toString()).build());
        assertMarvelTemplateExists();

        assertThat(exporter.templateVersion(), equalTo(fakeVersion));

        logger.debug("--> exporting when the marvel template is tool old: no document is exported and the template is not updated");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        assertMarvelDocsCount(0);
        assertMarvelTemplateExists();

        assertThat(exporter.templateVersion(), equalTo(fakeVersion));
    }

    @Test
    public void testIndexTimestampFormat() throws Exception {
        final String timeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");
        final String expectedIndexName = MarvelSettings.MARVEL_INDICES_PREFIX + DateTimeFormat.forPattern(timeFormat).withZoneUTC().print(System.currentTimeMillis());

        internalCluster().startNode(Settings.builder()
                .put("marvel.agent.exporters._local.type", LocalExporter.TYPE)
                .put("marvel.agent.exporters._local." + LocalExporter.INDEX_NAME_TIME_FORMAT_SETTING, timeFormat)
                .build());
        ensureGreen();

        LocalExporter exporter = getExporter("_local");
        assertThat(exporter.indexName(), equalTo(expectedIndexName));

        logger.debug("--> exporting a random marvel document");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        assertMarvelDocsCount(1);

        logger.debug("--> check that the index [{}] has the correct timestamp [{}]", timeFormat, expectedIndexName);
        assertTrue(client().admin().indices().prepareExists(expectedIndexName).get().isExists());

        logger.debug("--> updates the timestamp");
        final String newTimeFormat = randomFrom("dd", "dd.MM.YYYY", "dd.MM");
        final String newExpectedIndexName = MarvelSettings.MARVEL_INDICES_PREFIX + DateTimeFormat.forPattern(timeFormat).withZoneUTC().print(System.currentTimeMillis());

        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
                .put("marvel.agent.exporters._local.index.name.time_format", newTimeFormat)));

        logger.debug("--> exporting a random marvel document");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        assertMarvelDocsCount(1);

        logger.debug("--> check that the index [{}] has the correct timestamp [{}]", newTimeFormat, newExpectedIndexName);
        assertThat(exporter.indexName(), equalTo(newExpectedIndexName));
        assertTrue(client().admin().indices().prepareExists(newExpectedIndexName).get().isExists());
    }


    private <T extends Exporter> T getExporter(String name) {
        Exporter exporter = internalCluster().getInstance(Exporters.class).getExporter(name);
        assertNotNull("exporter [" + name + "] should not be null", exporter);
        return (T) exporter;
    }

    private void assertMarvelDocsCount(long expectedHitCount) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                String index = MarvelSettings.MARVEL_INDICES_PREFIX + "*";
                IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

                assertThat(client().admin().indices().prepareRefresh(index).setIndicesOptions(indicesOptions).get().getFailedShards(), equalTo(0));
                assertHitCount(client().prepareCount(index).setIndicesOptions(indicesOptions).get(), expectedHitCount);
            }
        }, 5, TimeUnit.SECONDS);
    }

    private void wipeMarvelIndices() {
        assertAcked(client().admin().indices().prepareDelete(MarvelSettings.MARVEL_INDICES_PREFIX + "*"));
    }

    private MarvelDoc newRandomMarvelDoc() {
        if (randomBoolean()) {
            return new IndexRecoveryMarvelDoc(internalCluster().getClusterName(),
                    IndexRecoveryCollector.TYPE, timeStampGenerator.incrementAndGet(), new RecoveryResponse());
        } else {
            return new ClusterStateMarvelDoc(internalCluster().getClusterName(),
                    ClusterStateCollector.TYPE, timeStampGenerator.incrementAndGet(), ClusterState.PROTO, ClusterHealthStatus.GREEN);
        }
    }

    private void assertMarvelTemplateExists() {
        assertTrue("marvel template must exists", isTemplateExists(LocalExporter.INDEX_TEMPLATE_NAME));
    }

    private void assertMarvelTemplateNotExists() {
        assertFalse("marvel template must not exists", isTemplateExists(LocalExporter.INDEX_TEMPLATE_NAME));
    }

    private boolean isTemplateExists(String templateName) {
        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates(templateName).get().getIndexTemplates()) {
            if (template.getName().equals(templateName)) {
                return true;
            }
        }
        return false;
    }
}
