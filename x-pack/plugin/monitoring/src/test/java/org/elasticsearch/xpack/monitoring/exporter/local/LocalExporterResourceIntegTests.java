/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = TEST,
                              numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class LocalExporterResourceIntegTests extends LocalExporterIntegTestCase {

    public LocalExporterResourceIntegTests() throws Exception {
        super();
    }

    private final MonitoredSystem system = randomFrom(MonitoredSystem.values());

    public void testCreateWhenResourcesNeedToBeAddedOrUpdated() throws Exception {
        // sometimes they need to be added; sometimes they need to be replaced
        if (randomBoolean()) {
            putResources(oldVersion());
        }

        assertResourcesExist();
    }

    public void testCreateWhenResourcesShouldNotBeReplaced() throws Exception {
        putResources(newEnoughVersion());

        assertResourcesExist();

        // these were "newer" or at least the same version, so they shouldn't be replaced
        assertTemplateNotUpdated();
        assertPipelinesNotUpdated();
    }

    private void createResources() throws Exception {
        // wait until the cluster is ready (this is done at the "Exporters" level)
        // this is not a busy assertion because it's checked earlier
        assertThat(clusterService().state().version(), not(ClusterState.UNKNOWN_VERSION));

        try (LocalExporter exporter = createLocalExporter()) {
            assertBusy(() -> assertThat(exporter.isExporterReady(), is(true)));
        }
    }

    /**
     * Generates a basic template that loosely represents a monitoring template.
     */
    private static BytesReference generateTemplateSource(final String name, final Integer version) throws IOException {
        final XContentBuilder builder = jsonBuilder().startObject();

        // this would totally break Monitoring UI, but the idea is that it's different from a real template and
        // the version controls that; it also won't break indexing (just searching) so this test can use it blindly
        builder
            .field("index_patterns", name)
            .startObject("settings")
                .field("index.number_of_shards", 1)
                .field("index.number_of_replicas", 0)
            .endObject()
            .startObject("mappings")
                // The internal representation still requires a default type of _doc
                .startObject("_doc")
                    .startObject("_meta")
                        .field("test", true)
                    .endObject()
                    .field("enabled", false)
                .endObject()
            .endObject();

        if (version != null) {
            builder.field("version", version);
        }

        return BytesReference.bytes(builder.endObject());
    }

    private void putResources(final Integer version) throws Exception {
        waitNoPendingTasksOnAll();

        putTemplate(version);
        putPipelines(version);
    }

    private void putTemplate(final Integer version) throws Exception {
        final String templateName = MonitoringTemplateUtils.templateName(system.getSystem());
        final BytesReference source = generateTemplateSource(templateName, version);

        assertAcked(client().admin().indices().preparePutTemplate(templateName).setSource(source, XContentType.JSON).get());
    }

    private void putPipelines(final Integer version) throws Exception {
        for (final String pipelineId : MonitoringTemplateUtils.PIPELINE_IDS) {
            putPipeline(MonitoringTemplateUtils.pipelineName(pipelineId), version);
        }
    }

    private void putPipeline(final String pipelineName, final Integer version) throws Exception {
        assertAcked(client().admin().cluster().preparePutPipeline(pipelineName, replaceablePipeline(version), XContentType.JSON).get());
    }

    /**
     * Create a pipeline with nothing in it whose description is literally "test".
     *
     * @param version Version to add to the pipeline, if any
     * @return Never {@code null}.
     */
    private BytesReference replaceablePipeline(final Integer version) {
        try {
            final XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());

            builder.startObject();

            {
                builder.startArray("processors").endArray();
                // something we can quickly check to ensure we have/have not replaced it
                builder.field("description", getTestName());

                // sometimes give it a version that should be overwritten (and sometimes don't give it a version at all)
                if (version != null) {
                    builder.field("version", version);
                }
            }

            return BytesReference.bytes(builder.endObject());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create pipeline", e);
        }
    }

    private Integer oldVersion() {
        final int minimumVersion = Math.min(ClusterAlertsUtil.LAST_UPDATED_VERSION, MonitoringTemplateUtils.LAST_UPDATED_VERSION);

        // randomly supply an older version, or no version at all
        return randomBoolean() ? minimumVersion - randomIntBetween(1, 100000) : null;
    }

    private int newEnoughVersion() {
        final int maximumVersion = Math.max(ClusterAlertsUtil.LAST_UPDATED_VERSION, MonitoringTemplateUtils.LAST_UPDATED_VERSION);

        // randomly supply a newer version or the expected version
        return randomFrom(maximumVersion + randomIntBetween(1, 100000), maximumVersion);
    }

    private void assertTemplatesExist() {
        for (String templateName : monitoringTemplateNames()) {
            assertTemplateInstalled(templateName);
        }
    }

    private void assertPipelinesExist() {
        for (PipelineConfiguration pipeline : client().admin().cluster().prepareGetPipeline("xpack_monitoring_*").get().pipelines()) {
            final Object description = pipeline.getConfigAsMap().get("description");

            // this just ensures that it's set; not who set it
            assertThat(description, notNullValue());
        }
    }

    private void assertResourcesExist() throws Exception {
        createResources();

        waitNoPendingTasksOnAll();

        assertBusy(() -> {
            assertTemplatesExist();
            assertPipelinesExist();
        });
    }

    private void assertTemplateNotUpdated() {
        final String name = MonitoringTemplateUtils.templateName(system.getSystem());

        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates(name).get().getIndexTemplates()) {
            final String docMapping = template.getMappings().get("_doc").toString();

            assertThat(docMapping, notNullValue());
            assertThat(docMapping, containsString("test"));
        }
    }

    private void assertPipelinesNotUpdated() {
        for (PipelineConfiguration pipeline : client().admin().cluster().prepareGetPipeline("xpack_monitoring_*").get().pipelines()) {
            final Object description = pipeline.getConfigAsMap().get("description");

            assertThat(description, equalTo(getTestName()));
        }
    }
}
