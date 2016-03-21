/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter.local;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStateMonitoringDoc;
import org.elasticsearch.marvel.agent.collector.indices.IndexRecoveryMonitoringDoc;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.joda.time.format.DateTimeFormat;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.dataTemplateName;
import static org.elasticsearch.marvel.agent.exporter.MarvelTemplateUtils.indexTemplateName;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class LocalExporterTests extends MarvelIntegTestCase {
    private final static AtomicLong timeStampGenerator = new AtomicLong();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testSimpleExport() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.agent.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.agent.exporters._local.enabled", true)
                .build());
        securedEnsureGreen();

        Exporter exporter = getLocalExporter("_local");

        logger.debug("--> exporting a single monitoring doc");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        awaitMarvelDocsCount(is(1L));

        deleteMarvelIndices();

        final List<MonitoringDoc> monitoringDocs = new ArrayList<>();
        for (int i=0; i < randomIntBetween(2, 50); i++) {
            monitoringDocs.add(newRandomMarvelDoc());
        }

        logger.debug("--> exporting {} monitoring docs", monitoringDocs.size());
        exporter.export(monitoringDocs);
        awaitMarvelDocsCount(is((long) monitoringDocs.size()));

        SearchResponse response = client().prepareSearch(MONITORING_INDICES_PREFIX + "*").get();
        for (SearchHit hit : response.getHits().hits()) {
            Map<String, Object> source = hit.sourceAsMap();
            assertNotNull(source.get("cluster_uuid"));
            assertNotNull(source.get("timestamp"));
            assertNotNull(source.get("source_node"));
        }
    }

    public void testTemplateCreation() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.agent.exporters._local.type", LocalExporter.TYPE)
                .build());
        securedEnsureGreen();

        // start collecting
        updateMarvelInterval(3L, TimeUnit.SECONDS);

        // lets wait until the monitoring template will be installed
        waitForMarvelTemplate(indexTemplateName());
        waitForMarvelTemplate(dataTemplateName());
    }

    public void testIndexTimestampFormat() throws Exception {
        String timeFormat = randomFrom("YY", "YYYY", "YYYY.MM", "YYYY-MM", "MM.YYYY", "MM");

        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.agent.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.agent.exporters._local." + LocalExporter.INDEX_NAME_TIME_FORMAT_SETTING, timeFormat)
                .build());
        securedEnsureGreen();

        LocalExporter exporter = getLocalExporter("_local");

        // now lets test that the index name resolver works with a doc
        MonitoringDoc doc = newRandomMarvelDoc();
        String indexName = ".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-"
                + DateTimeFormat.forPattern(timeFormat).withZoneUTC().print(doc.getTimestamp());
        assertThat(exporter.getResolvers().getResolver(doc).index(doc), equalTo(indexName));

        logger.debug("--> exporting a random monitoring document");
        exporter.export(Collections.singletonList(doc));
        awaitIndexExists(indexName);

        logger.debug("--> updates the timestamp");
        timeFormat = randomFrom("dd", "dd.MM.YYYY", "dd.MM");
        updateClusterSettings(Settings.builder().put("xpack.monitoring.agent.exporters._local.index.name.time_format", timeFormat));
        exporter = getLocalExporter("_local"); // we need to get it again.. as it was rebuilt
        indexName = ".monitoring-es-" + MarvelTemplateUtils.TEMPLATE_VERSION + "-"
                + DateTimeFormat.forPattern(timeFormat).withZoneUTC().print(doc.getTimestamp());
        assertThat(exporter.getResolvers().getResolver(doc).index(doc), equalTo(indexName));

        logger.debug("--> exporting the document again (this time with the the new index name time format [{}], expecting index name [{}]",
                timeFormat, indexName);
        exporter.export(Collections.singletonList(doc));
        awaitIndexExists(indexName);
    }

    public void testLocalExporterFlush() throws Exception {
        internalCluster().startNode(Settings.builder()
                .put("xpack.monitoring.agent.exporters._local.type", LocalExporter.TYPE)
                .put("xpack.monitoring.agent.exporters._local.enabled", true)
                .build());
        securedEnsureGreen();

        LocalExporter exporter = getLocalExporter("_local");

        logger.debug("--> exporting a single monitoring doc");
        exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        awaitMarvelDocsCount(is(1L));
        assertNull(exporter.getBulk().requestBuilder);

        logger.debug("--> closing monitoring indices");
        assertAcked(client().admin().indices().prepareClose(MONITORING_INDICES_PREFIX + "*").get());

        try {
            logger.debug("--> exporting a second monitoring doc");
            exporter.export(Collections.singletonList(newRandomMarvelDoc()));
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), allOf(containsString("failure in bulk execution"), containsString("IndexClosedException[closed]")));
            assertNull(exporter.getBulk().requestBuilder);
        }
    }

    private LocalExporter getLocalExporter(String name) throws Exception {
        final Exporter exporter = internalCluster().getInstance(Exporters.class).getExporter(name);
        assertThat(exporter, notNullValue());
        assertThat(exporter, instanceOf(LocalExporter.class));
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(exporter.openBulk(), notNullValue());
            }
        });
        return (LocalExporter) exporter;
    }

    private MonitoringDoc newRandomMarvelDoc() {
        if (randomBoolean()) {
            IndexRecoveryMonitoringDoc doc = new IndexRecoveryMonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
            doc.setClusterUUID(internalCluster().getClusterName());
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, Version.CURRENT));
            doc.setRecoveryResponse(new RecoveryResponse());
            return doc;
        } else {
            ClusterStateMonitoringDoc doc = new ClusterStateMonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT.toString());
            doc.setClusterUUID(internalCluster().getClusterName());
            doc.setTimestamp(System.currentTimeMillis());
            doc.setSourceNode(new DiscoveryNode("id", DummyTransportAddress.INSTANCE, Version.CURRENT));
            doc.setClusterState(ClusterState.PROTO);
            doc.setStatus(ClusterHealthStatus.GREEN);
            return doc;
        }
    }
}
