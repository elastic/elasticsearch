/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.trace.data.SpanData;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskTracer;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.service.MasterService.STATE_UPDATE_ACTION_NAME;
import static org.elasticsearch.indices.recovery.PeerRecoverySourceService.Actions.START_RECOVERY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ApmIT extends SecurityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), APM.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        ((MockSecureSettings) builder.getSecureSettings()).setString(
            APMTracer.APM_ENDPOINT_SETTING.getKey(),
            System.getProperty("tests.apm.endpoint", "")
        );
        ((MockSecureSettings) builder.getSecureSettings()).setString(
            APMTracer.APM_TOKEN_SETTING.getKey(),
            System.getProperty("tests.apm.token", "")
        );
        builder.put(APMTracer.APM_ENABLED_SETTING.getKey(), true).put("xpack.security.authz.tracing", true);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @After
    public void clearRecordedSpans() {
        APMTracer.CAPTURING_SPAN_EXPORTER.clear();
    }

    public void testModule() {
        List<APM> plugins = internalCluster().getMasterNodeInstance(PluginsService.class).filterPlugins(APM.class);
        assertThat(plugins, hasSize(1));

        TransportService transportService = internalCluster().getInstance(TransportService.class);
        final TaskTracer taskTracer = transportService.getTaskManager().getTaskTracer();
        assertThat(taskTracer, notNullValue());

        final Task testTask = new Task(randomNonNegativeLong(), "test", "action", "", TaskId.EMPTY_TASK_ID, Collections.emptyMap());

        APMTracer.CAPTURING_SPAN_EXPORTER.clear();

        taskTracer.onTaskRegistered(testTask);
        taskTracer.onTaskUnregistered(testTask);

        final List<SpanData> capturedSpans = APMTracer.CAPTURING_SPAN_EXPORTER.getCapturedSpans();
        boolean found = false;
        final Long targetId = testTask.getId();
        for (SpanData capturedSpan : capturedSpans) {
            if (targetId.equals(capturedSpan.getAttributes().get(AttributeKey.longKey("es.task.id")))) {
                found = true;
                assertTrue(capturedSpan.hasEnded());
            }
        }
        assertTrue(found);
    }

    public void testRecordsNestedSpans() {

        APMTracer.CAPTURING_SPAN_EXPORTER.clear();// removing start related events

        client().admin().cluster().prepareListTasks().get();

        var parentTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists").collect(toList());
        assertThat(parentTasks, hasSize(1));
        var parentTask = parentTasks.get(0);
        assertThat(parentTask.getParentSpanId(), equalTo("0000000000000000"));

        var childrenTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists[n]").collect(toList());
        assertThat(childrenTasks, hasSize(internalCluster().size()));
        for (SpanData childrenTask : childrenTasks) {
            assertThat(childrenTask.getParentSpanId(), equalTo(parentTask.getSpanId()));
            assertThat(childrenTask.getTraceId(), equalTo(parentTask.getTraceId()));
        }
    }

    public void testRecovery() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test-index")
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                )
        );

        ensureGreen("test-index");

        indexRandom(true, true, client().prepareIndex("test-index").setSource("{}", XContentType.JSON));
        flushAndRefresh("test-index");

        final APMTracer.CapturingSpanExporter spanExporter = APMTracer.CAPTURING_SPAN_EXPORTER;
        spanExporter.clear();

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test-index")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

        ensureGreen("test-index");

        final SpanData clusterUpdateSpan = spanExporter.findSpanByName(STATE_UPDATE_ACTION_NAME)
            .findAny()
            .orElseThrow(() -> new AssertionError("not found"));

        final List<String> clusterUpdateChildActions = spanExporter.findSpan(
            spanData -> spanData.getParentSpanId().equals(clusterUpdateSpan.getSpanId())
        ).map(SpanData::getName).collect(toList());

        assertThat(
            clusterUpdateChildActions,
            hasItems(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME, PublicationTransportHandler.COMMIT_STATE_ACTION_NAME)
        );

        final SpanData recoverySpan = spanExporter.findSpanByName(START_RECOVERY)
            .findAny()
            .orElseThrow(() -> new AssertionError("not found"));
        final List<String> recoveryChildActions = spanExporter.findSpan(
            spanData -> spanData.getParentSpanId().equals(recoverySpan.getSpanId())
        ).map(SpanData::getName).collect(toList());

        assertThat(
            recoveryChildActions,
            hasItems(
                PeerRecoveryTargetService.Actions.FILES_INFO,
                PeerRecoveryTargetService.Actions.FILE_CHUNK,
                PeerRecoveryTargetService.Actions.CLEAN_FILES,
                PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG,
                PeerRecoveryTargetService.Actions.FINALIZE
            )
        );

    }

    public void testSearch() throws Exception {

        internalCluster().ensureAtLeastNumDataNodes(2);
        final int nodeCount = internalCluster().numDataNodes();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test-matching")
                .setMapping("{\"properties\":{\"message\":{\"type\":\"text\"},\"@timestamp\":{\"type\":\"date\"}}}")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, nodeCount * 6)
                )
        );

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test-notmatching")
                .setMapping("{\"properties\":{\"message\":{\"type\":\"text\"},\"@timestamp\":{\"type\":\"date\"}}}")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, nodeCount * 6)
                )
        );

        ensureGreen("test-matching", "test-notmatching");

        final String matchingDate = "2021-11-17";
        final String nonMatchingDate = "2021-01-01";

        final BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 1000; i++) {
            final boolean isMatching = randomBoolean();
            final IndexRequestBuilder indexRequestBuilder = client().prepareIndex(isMatching ? "test-matching" : "test-notmatching");
            indexRequestBuilder.setSource(
                "{\"@timestamp\":\"" + (isMatching ? matchingDate : nonMatchingDate) + "\",\"message\":\"\"}",
                XContentType.JSON
            );
            bulkRequestBuilder.add(indexRequestBuilder);
        }

        assertFalse(bulkRequestBuilder.execute().actionGet(10, TimeUnit.SECONDS).hasFailures());

        final APMTracer.CapturingSpanExporter spanExporter = APMTracer.CAPTURING_SPAN_EXPORTER;
        spanExporter.clear();

        final Request searchRequest = new Request("GET", "_search");
        searchRequest.addParameter("search_type", "query_then_fetch");
        searchRequest.addParameter("pre_filter_shard_size", "1");
        searchRequest.setJsonEntity("{\"query\":{\"range\":{\"@timestamp\":{\"gt\":\"2021-11-01\"}}}}");
        searchRequest.setOptions(
            searchRequest.getOptions()
                .toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(
                        SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
                    )
                )
        );

        final Response searchResponse = getRestClient().performRequest(searchRequest);

        assertTrue(spanExporter.findSpanByName(SearchAction.NAME).findAny().isPresent());
        assertTrue(spanExporter.findSpanByName(SearchTransportService.QUERY_CAN_MATCH_NODE_NAME).findAny().isPresent());
    }

    public void testDoesNotRecordSpansWhenDisabled() {

        client().admin()
            .cluster()
            .updateSettings(
                new ClusterUpdateSettingsRequest().persistentSettings(
                    Settings.builder().put(APMTracer.APM_ENABLED_SETTING.getKey(), false).build()
                )
            )
            .actionGet();

        try {
            APMTracer.CAPTURING_SPAN_EXPORTER.clear();

            client().admin().cluster().prepareListTasks().get();

            assertThat(APMTracer.CAPTURING_SPAN_EXPORTER.getCapturedSpans(), empty());
        } finally {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(APMTracer.APM_ENABLED_SETTING.getKey(), (String) null).build()
                    )
                )
                .actionGet();
        }
    }

    public void testFilterByNameGivenSingleCompleteMatch() {

        client().admin()
            .cluster()
            .updateSettings(
                new ClusterUpdateSettingsRequest().persistentSettings(
                    Settings.builder().put(APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), "cluster:monitor/tasks/lists").build()
                )
            )
            .actionGet();

        APMTracer.CAPTURING_SPAN_EXPORTER.clear();// removing start related events

        try {
            client().admin().cluster().prepareListTasks().get();

            var parentTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists").collect(toList());
            assertThat(parentTasks, hasSize(1));

            var childrenTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists[n]").collect(toList());
            assertThat(childrenTasks, empty());
        } finally {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), (String) null).build()
                    )
                )
                .actionGet();
        }
    }

    public void testFilterByNameGivenSinglePattern() {

        client().admin()
            .cluster()
            .updateSettings(
                new ClusterUpdateSettingsRequest().persistentSettings(
                    Settings.builder().put(APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), "*/tasks/lists*").build()
                )
            )
            .actionGet();

        APMTracer.CAPTURING_SPAN_EXPORTER.clear();// removing start related events

        try {
            client().admin().cluster().prepareListTasks().get();

            var parentTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists").collect(toList());
            assertThat(parentTasks, hasSize(1));

            var childrenTasks = APMTracer.CAPTURING_SPAN_EXPORTER.findSpanByName("cluster:monitor/tasks/lists[n]").collect(toList());
            assertThat(childrenTasks, hasSize(internalCluster().size()));
        } finally {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), (String) null).build()
                    )
                )
                .actionGet();
        }
    }

    public void testFilterByNameGivenTwoPatterns() {

        client().admin()
            .cluster()
            .updateSettings(
                new ClusterUpdateSettingsRequest().persistentSettings(
                    Settings.builder().put(APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), "*/tasks/lists,*/nodes/stats").build()
                )
            )
            .actionGet();

        APMTracer.CAPTURING_SPAN_EXPORTER.clear();// removing start related events

        try {
            client().admin().cluster().prepareListTasks().get();
            client().admin().cluster().nodesStats(new NodesStatsRequest()).actionGet();

            var spans = APMTracer.CAPTURING_SPAN_EXPORTER.getCapturedSpans().stream().map(SpanData::getName).collect(Collectors.toSet());
            assertThat(spans, contains("cluster:monitor/nodes/stats", "cluster:monitor/tasks/lists"));
        } finally {
            client().admin()
                .cluster()
                .updateSettings(
                    new ClusterUpdateSettingsRequest().persistentSettings(
                        Settings.builder().put(APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING.getKey(), (String) null).build()
                    )
                )
                .actionGet();
        }
    }
}
