/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@AwaitsFix(bugUrl = "https://github.com/elastic/x-pack-elasticsearch/issues/416")
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class LocalExporterTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringInterval();
        wipeMonitoringIndices();
    }

    @TestLogging("org.elasticsearch.xpack.monitoring.exporter.local:TRACE")
    public void testSimpleExport() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .build());
        ensureGreen();

        logger.debug("--> exporting a single monitoring doc");
        export(Collections.singletonList(newRandomMonitoringDoc()));
        awaitMonitoringDocsCount(is(1L));

        deleteMonitoringIndices();

        final List<MonitoringDoc> monitoringDocs = new ArrayList<>();
        for (int i=0; i < randomIntBetween(2, 50); i++) {
            monitoringDocs.add(newRandomMonitoringDoc());
        }

        logger.debug("--> exporting {} monitoring docs", monitoringDocs.size());
        export(monitoringDocs);
        awaitMonitoringDocsCount(is((long) monitoringDocs.size()));

        SearchResponse response = client().prepareSearch(MONITORING_INDICES_PREFIX + "*").get();
        for (SearchHit hit : response.getHits().getHits()) {
            Map<String, Object> source = hit.getSourceAsMap();
            assertNotNull(source.get("cluster_uuid"));
            assertNotNull(source.get("timestamp"));
            assertNotNull(source.get("source_node"));
        }
    }

    public void testTemplateCreation() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .build());
        ensureGreen();

        // start collecting
        updateMonitoringInterval(3L, TimeUnit.SECONDS);

        // lets wait until the monitoring template will be installed
        waitForMonitoringTemplates();
    }

    public void testIndexTimestampFormat() throws Exception {
        String timeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");

        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local." + LocalExporter.INDEX_NAME_TIME_FORMAT_SETTING, timeFormat)
                .build());
        ensureGreen();

        LocalExporter exporter = getLocalExporter("_local");

        // now lets test that the index name resolver works with a doc
        MonitoringDoc doc = newRandomMonitoringDoc();
        String indexName = ".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-"
                + DateTimeFormat.forPattern(timeFormat).withZoneUTC().print(doc.getTimestamp());
        assertThat(exporter.getResolvers().getResolver(doc).index(doc), equalTo(indexName));

        logger.debug("--> exporting a random monitoring document");
        export(Collections.singletonList(doc));
        awaitIndexExists(indexName);

        logger.debug("--> updates the timestamp");
        timeFormat = randomFrom("dd", "dd.MM.YYYY", "dd.MM");
        updateClusterSettings(Settings.builder().put("xpack.monitoring.exporters._local.index.name.time_format", timeFormat));
        exporter = getLocalExporter("_local"); // we need to get it again.. as it was rebuilt
        indexName = ".monitoring-es-" + MonitoringTemplateUtils.TEMPLATE_VERSION + "-"
                + DateTimeFormat.forPattern(timeFormat).withZoneUTC().print(doc.getTimestamp());
        assertThat(exporter.getResolvers().getResolver(doc).index(doc), equalTo(indexName));

        logger.debug("--> exporting the document again (this time with the the new index name time format [{}], expecting index name [{}]",
                timeFormat, indexName);
        export(Collections.singletonList(doc));
        awaitIndexExists(indexName);
    }

    public void testLocalExporterFlush() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.exporters._local.enabled", true)
                .build());
        ensureGreen();

        logger.debug("--> exporting a single monitoring doc");
        export(Collections.singletonList(newRandomMonitoringDoc()));
        awaitMonitoringDocsCount(is(1L));

        logger.debug("--> closing monitoring indices");
        assertAcked(client().admin().indices().prepareClose(MONITORING_INDICES_PREFIX + "*").get());

        try {
            logger.debug("--> exporting a second monitoring doc");
            LocalExporter exporter = getLocalExporter("_local");

            LocalBulk bulk = (LocalBulk) exporter.openBulk();
            bulk.add(Collections.singletonList(newRandomMonitoringDoc()));
            PlainActionFuture<Void> future = new PlainActionFuture<>();
            bulk.close(true, future);
            future.get();
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), containsString("failed to flush export bulk [_local]"));
            assertThat(e.getCause(), instanceOf(ExportException.class));

            ExportException cause = (ExportException) e.getCause();
            assertTrue(cause.hasExportExceptions());
            for (ExportException c : cause) {
                assertThat(c.getMessage(), containsString("IndexClosedException[closed]"));
            }
        }
    }

    private void export(Collection<MonitoringDoc> docs) throws Exception {
        Exporters exporters = internalCluster().getInstance(Exporters.class);
        assertThat(exporters, notNullValue());
        // make sure exporters has been started, otherwise we might miss some of the exporters
        assertBusy(() -> assertEquals(State.STARTED, exporters.lifecycleState()));

        // Wait for exporting bulks to be ready to export
        assertBusy(() -> exporters.forEach(exporter -> assertThat(exporter.openBulk(), notNullValue())));
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        exporters.export(docs, future);
        future.get();
    }

    private LocalExporter getLocalExporter(String name) throws Exception {
        final Exporter exporter = internalCluster().getInstance(Exporters.class).getExporter(name);
        assertThat(exporter, notNullValue());
        assertThat(exporter, instanceOf(LocalExporter.class));
        assertBusy(() -> assertThat(exporter.openBulk(), notNullValue()));
        return (LocalExporter) exporter;
    }

    private MonitoringDoc newRandomMonitoringDoc() {
        String monitoringId = MonitoredSystem.ES.getSystem();
        String monitoringVersion = Version.CURRENT.toString();
        String clusterUUID = internalCluster().getClusterName();
        long timestamp = System.currentTimeMillis();
        DiscoveryNode sourceNode = new DiscoveryNode("id", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

        if (randomBoolean()) {
            return new IndexRecoveryMonitoringDoc(monitoringId, monitoringVersion, clusterUUID,
                    timestamp, sourceNode, new RecoveryResponse());
        } else {
            return new ClusterStateMonitoringDoc(monitoringId, monitoringVersion, clusterUUID,
                    timestamp, sourceNode, ClusterState.EMPTY_STATE, ClusterHealthStatus.GREEN);
        }
    }
}
