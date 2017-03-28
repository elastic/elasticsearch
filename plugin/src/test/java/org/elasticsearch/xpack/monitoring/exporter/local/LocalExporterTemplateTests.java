/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = TEST, numDataNodes = 0, numClientNodes = 0, transportClientRatio = 0.0)
public class LocalExporterTemplateTests extends MonitoringIntegTestCase {

    private final Settings localExporter = Settings.builder().put("type", LocalExporter.TYPE).build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put("xpack.monitoring.exporters._exporter.type", LocalExporter.TYPE);
        return settings.build();
    }

    public void testCreateWhenExistingTemplatesAreOld() throws Exception {
        internalCluster().startNode();

        // put an old variant of the monitoring-data-# index so that types need to be added
        final CreateIndexRequest request = new CreateIndexRequest(MonitoringTemplateUtils.DATA_INDEX);

        request.settings(Settings.builder().put("index.mapper.dynamic", false).build());
        // notably absent are: kibana, logstash, and beats
        request.mapping("cluster_info", "{\"enabled\": false}", XContentType.JSON);
        request.mapping("node", "{\"enabled\": false}", XContentType.JSON);
        request.mapping("fake", "{\"enabled\": false}", XContentType.JSON);

        client().admin().indices().create(request).actionGet();

        putTemplate(indexTemplateName());
        putTemplate(dataTemplateName());
        putPipeline(Exporter.EXPORT_PIPELINE_NAME);

        doExporting();

        logger.debug("--> existing templates are old");
        assertTemplateExists(dataTemplateName());
        assertTemplateExists(indexTemplateName());

        logger.debug("--> existing templates are old: new templates should be created");
        for (String template : monitoringTemplateNames()) {
            assertTemplateExists(template);
        }
        assertPipelineExists(Exporter.EXPORT_PIPELINE_NAME);

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        assertIndicesExists(currentTimestampedIndexName());

        // ensure that it added mapping types to monitoring-data-2, without throwing away the index
        assertMapping(MonitoringTemplateUtils.DATA_INDEX, "fake");
        for (final String type : MonitoringTemplateUtils.NEW_DATA_TYPES) {
            assertMapping(MonitoringTemplateUtils.DATA_INDEX, type);
        }
    }

    private void assertMapping(final String index, final String type) throws Exception {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(index).setTypes(type).get();
        ImmutableOpenMap<String, MappingMetaData> mappings = response.getMappings().get(index);
        assertThat(mappings, notNullValue());
        MappingMetaData mappingMetaData = mappings.get(type);
        assertThat(mappingMetaData, notNullValue());
    }

    public void testCreateWhenExistingTemplateAreUpToDate() throws Exception {
        internalCluster().startNode();

        putTemplate(indexTemplateName());
        putTemplate(dataTemplateName());
        putPipeline(Exporter.EXPORT_PIPELINE_NAME);

        doExporting();

        logger.debug("--> existing templates are up to date");
        for (String template : monitoringTemplateNames()) {
            assertTemplateExists(template);
        }
        assertPipelineExists(Exporter.EXPORT_PIPELINE_NAME);

        logger.debug("--> existing templates has the same version: they should not be changed");
        assertTemplateNotUpdated(indexTemplateName());
        assertTemplateNotUpdated(dataTemplateName());
        assertPipelineNotUpdated(Exporter.EXPORT_PIPELINE_NAME);

        doExporting();

        logger.debug("--> indices should have been created");
        awaitIndexExists(currentDataIndexName());
        awaitIndexExists(currentTimestampedIndexName());
    }

    protected void doExporting() throws Exception {
        // TODO: these should be unit tests, not using guice (copied from now-deleted AbstractExporterTemplateTestCase)
        final String node = randomFrom(internalCluster().getNodeNames());
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        XPackLicenseState licenseState = internalCluster().getInstance(XPackLicenseState.class, node);
        LicenseService licenseService = internalCluster().getInstance(LicenseService.class, node);
        InternalClient client = internalCluster().getInstance(InternalClient.class, node);
        Collector collector = new ClusterStatsCollector(clusterService.getSettings(), clusterService,
                new MonitoringSettings(clusterService.getSettings(), clusterService.getClusterSettings()),
                licenseState, client, licenseService);

        Exporters exporters = internalCluster().getInstance(Exporters.class, node);
        assertNotNull(exporters);

        Exporter exporter = exporters.getExporter("_exporter");

        // Wait for exporting bulks to be ready to export
        Runnable busy = () -> assertThat(exporter.openBulk(), notNullValue());
        assertBusy(busy);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        exporters.export(collector.collect(), future);
        future.get();
    }

    private String dataTemplateName() {
        MockDataIndexNameResolver resolver = new MockDataIndexNameResolver(MonitoringTemplateUtils.TEMPLATE_VERSION);
        return resolver.templateName();
    }

    private String indexTemplateName() {
        MockTimestampedIndexNameResolver resolver =
                new MockTimestampedIndexNameResolver(MonitoredSystem.ES, localExporter, MonitoringTemplateUtils.TEMPLATE_VERSION);
        return resolver.templateName();
    }

    private String currentDataIndexName() {
        return MonitoringTemplateUtils.DATA_INDEX;
    }

    private String currentTimestampedIndexName() {
        MonitoringDoc doc = new MonitoringDoc(MonitoredSystem.ES.getSystem(), Version.CURRENT
                .toString(), null, null, null, System.currentTimeMillis(),
                (MonitoringDoc.Node) null);

        MockTimestampedIndexNameResolver resolver =
                new MockTimestampedIndexNameResolver(MonitoredSystem.ES, localExporter, MonitoringTemplateUtils.TEMPLATE_VERSION);
        return resolver.index(doc);
    }

    /** Generates a basic template **/
    private BytesReference generateTemplateSource(String name) throws IOException {
        return jsonBuilder().startObject()
                                .field("template", name)
                                .startObject("settings")
                                    .field("index.number_of_shards", 1)
                                    .field("index.number_of_replicas", 1)
                                .endObject()
                                .startObject("mappings")
                                    .startObject("_default_")
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
                                .endObject()
                            .endObject().bytes();
    }

    private void putTemplate(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().indices().preparePutTemplate(name).setSource(generateTemplateSource(name), XContentType.JSON).get());
    }

    private void putPipeline(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertAcked(client().admin().cluster().preparePutPipeline(name, Exporter.emptyPipeline(XContentType.JSON).bytes(),
                XContentType.JSON).get());
    }

    private void assertTemplateExists(String name) throws Exception {
        waitNoPendingTasksOnAll();
        waitForMonitoringTemplate(name);
    }

    private void assertPipelineExists(String name) throws Exception {
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

    private void assertTemplateNotUpdated(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertTemplateExists(name);
    }

    private void assertPipelineNotUpdated(String name) throws Exception {
        waitNoPendingTasksOnAll();
        assertPipelineExists(name);
    }

}
