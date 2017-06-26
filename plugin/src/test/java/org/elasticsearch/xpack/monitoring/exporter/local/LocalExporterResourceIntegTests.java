/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.local;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.ClusterAlertsUtil;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.watcher.client.WatchSourceBuilders;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.NeverCondition;
import org.elasticsearch.xpack.watcher.input.simple.SimpleInput;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchRequest;
import org.elasticsearch.xpack.watcher.trigger.manual.ManualTrigger;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = TEST,
                              numDataNodes = 1, numClientNodes = 0, transportClientRatio = 0.0, supportsDedicatedMasters = false)
public class LocalExporterResourceIntegTests extends LocalExporterIntegTestCase {

    private final boolean useClusterAlerts = enableWatcher() && randomBoolean();
    private final MonitoredSystem system = randomFrom(MonitoredSystem.values());
    private final MockTimestampedIndexNameResolver resolver =
            new MockTimestampedIndexNameResolver(system, Settings.EMPTY, MonitoringTemplateUtils.TEMPLATE_VERSION);

    @Override
    protected boolean useClusterAlerts() {
        return useClusterAlerts;
    }

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
        assertWatchesNotUpdated();
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
                .startObject("doc")
                    .startObject("_meta")
                        .field("test", true)
                    .endObject()
                    .field("enabled", false)
                .endObject()
            .endObject();

        if (version != null) {
            builder.field("version", version);
        }

        return builder.endObject().bytes();
    }

    private void putResources(final Integer version) throws Exception {
        waitNoPendingTasksOnAll();

        putTemplate(version);
        putPipelines(version);
        putWatches(version);
    }

    private void putTemplate(final Integer version) throws Exception {
        final BytesReference source = generateTemplateSource(resolver.templateName(), version);

        assertAcked(client().admin().indices().preparePutTemplate(resolver.templateName()).setSource(source, XContentType.JSON).get());
    }

    private void putPipelines(final Integer version) throws Exception {
        for (final String pipelineId : MonitoringTemplateUtils.PIPELINE_IDS) {
            putPipeline(MonitoringTemplateUtils.pipelineName(pipelineId), version);
        }
    }

    private void putPipeline(final String pipelineName, final Integer version) throws Exception {
        assertAcked(client().admin().cluster().preparePutPipeline(pipelineName, replaceablePipeline(version), XContentType.JSON).get());
    }

    private void putWatches(final Integer version) throws Exception {
        if (enableWatcher()) {
            // wait until the cluster is ready so that we can get the cluster's UUID
            assertBusy(() -> assertThat(clusterService().state().version(), not(ClusterState.UNKNOWN_VERSION)));

            for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
                putWatch(ClusterAlertsUtil.createUniqueWatchId(clusterService(), watchId), version);
            }
        }
    }

    private void putWatch(final String watchName, final Integer version) throws Exception {
        final WatcherClient watcherClient = new WatcherClient(client());

        assertThat(watcherClient.putWatch(new PutWatchRequest(watchName, replaceableWatch(version))).get().isCreated(), is(true));
    }

    /**
     * Create a Watch with nothing in it that will never fire its action, with a metadata field named after {@linkplain #getTestName()}.
     *
     * @param version Version to add to the watch, if any
     * @return Never {@code null}.
     */
    private WatchSourceBuilder replaceableWatch(final Integer version) {
        final WatchSourceBuilder builder = WatchSourceBuilders.watchBuilder();
        final Map<String, Object> metadata = new HashMap<>();

        // add a marker so that we know this test made the watch
        metadata.put(getTestName(), true);

        if (version != null) {
            metadata.put("xpack", MapBuilder.<String, Object>newMapBuilder().put("version_created", version).map());
        }

        builder.metadata(metadata);
        builder.trigger(new ManualTrigger());
        builder.input(new SimpleInput(Payload.EMPTY));
        builder.condition(NeverCondition.INSTANCE);
        builder.addAction("_action", LoggingAction.builder(new TextTemplate("never runs")));

        return builder;
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

            return builder.endObject().bytes();
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

    private void assertWatchesExist() {
        if (enableWatcher()) {
            final WatcherClient watcherClient = new WatcherClient(client());

            for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
                final String watchName = ClusterAlertsUtil.createUniqueWatchId(clusterService(), watchId);
                final GetWatchResponse response = watcherClient.prepareGetWatch(watchName).get();

                // this just ensures that it's set; not who set it
                assertThat(response.isFound(), is(true));
            }
        }
    }

    private void assertResourcesExist() throws Exception {
        createResources();

        waitNoPendingTasksOnAll();

        assertBusy(() -> {
            assertTemplatesExist();
            assertPipelinesExist();
            if (useClusterAlerts) {
                assertWatchesExist();
            } else {
                assertWatchesNotUpdated();
            }
        });
    }

    private void assertTemplateNotUpdated() {
        final String name = resolver.templateName();

        for (IndexTemplateMetaData template : client().admin().indices().prepareGetTemplates(name).get().getIndexTemplates()) {
            final String docMapping = template.getMappings().get("doc").toString();

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

    private void assertWatchesNotUpdated() throws Exception {
        if (enableWatcher()) {
            final WatcherClient watcherClient = new WatcherClient(client());

            for (final String watchId : ClusterAlertsUtil.WATCH_IDS) {
                final String watchName = ClusterAlertsUtil.createUniqueWatchId(clusterService(), watchId);
                final GetWatchResponse response = watcherClient.prepareGetWatch(watchName).get();

                // if we didn't find the watch, then we certainly didn't update it
                if (response.isFound()) {
                    // ensure that the watch still contains a value associated with the test rather than the real watch
                    assertThat(response.getSource().getValue("metadata." + getTestName()), notNullValue());
                }
            }
        }
    }

}
