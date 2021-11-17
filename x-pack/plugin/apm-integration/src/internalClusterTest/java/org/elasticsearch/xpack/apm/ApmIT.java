/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.trace.data.SpanData;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.TracingPlugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskTracer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class ApmIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), APM.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(APMTracer.APM_ENDPOINT_SETTING.getKey(), System.getProperty("tests.apm.endpoint", ""));
        secureSettings.setString(APMTracer.APM_TOKEN_SETTING.getKey(), System.getProperty("tests.apm.token", ""));
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).setSecureSettings(secureSettings).build();
    }

    public void testModule() {
        List<TracingPlugin> plugins = internalCluster().getMasterNodeInstance(PluginsService.class).filterPlugins(TracingPlugin.class);
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

        client().prepareSearch()
            .setQuery(new RangeQueryBuilder("@timestamp").gt("2021-11-01"))
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setPreFilterShardSize(1)
            .execute()
            .actionGet(10, TimeUnit.SECONDS);

        assertTrue(spanExporter.findSpanByName(SearchAction.NAME).findAny().isPresent());
        assertTrue(spanExporter.findSpanByName(SearchTransportService.QUERY_CAN_MATCH_NODE_NAME).findAny().isPresent());
    }
}
