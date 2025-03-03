/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class LogsPatternUsageServiceIntegrationTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LogsDBPlugin.class, DataStreamsPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("logsdb.usage_check.max_period", "1s").build();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testLogsPatternUsage() throws Exception {
        var template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("logs-*-*"))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("1").indexTemplate(template)
            ).actionGet()
        );

        IndexRequest indexRequest = new IndexRequest("my-index").create(true).source("field", "value");
        var indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        {
            var response = client().execute(ClusterGetSettingsAction.INSTANCE, new ClusterGetSettingsAction.Request(TimeValue.ONE_MINUTE))
                .actionGet();
            assertThat(response.persistentSettings().get("logsdb.prior_logs_usage"), nullValue());
        }

        indexRequest = new IndexRequest("logs-myapp-prod").create(true).source("@timestamp", "2000-01-01T00:00");
        indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        assertBusy(() -> {
            var response = client().execute(ClusterGetSettingsAction.INSTANCE, new ClusterGetSettingsAction.Request(TimeValue.ONE_MINUTE))
                .actionGet();
            assertThat(response.persistentSettings().get("logsdb.prior_logs_usage"), equalTo("true"));
        });
    }

    public void testLogsPatternUsageNoLogsStarDashStarUsage() throws Exception {
        var template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("log-*-*"))
            .template(new Template(Settings.builder().put("index.number_of_replicas", 0).build(), null, null))
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("1").indexTemplate(template)
            ).actionGet()
        );

        var indexRequest = new IndexRequest("log-myapp-prod").create(true).source("@timestamp", "2000-01-01T00:00");
        var indexResponse = client().index(indexRequest).actionGet();
        assertThat(indexResponse.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        ensureGreen("log-myapp-prod");
        // Check that LogsPatternUsageService checked three times by checking generic threadpool stats.
        // (the LogsPatternUsageService's check is scheduled via the generic threadpool)
        var threadPool = getInstanceFromNode(ThreadPool.class);
        var beforeStat = getGenericThreadpoolStat(threadPool);
        assertBusy(() -> {
            var stat = getGenericThreadpoolStat(threadPool);
            assertThat(stat.completed(), greaterThanOrEqualTo(beforeStat.completed() + 3));
        });
        var response = client().execute(ClusterGetSettingsAction.INSTANCE, new ClusterGetSettingsAction.Request(TimeValue.ONE_MINUTE))
            .actionGet();
        assertThat(response.persistentSettings().get("logsdb.prior_logs_usage"), nullValue());
    }

    private static ThreadPoolStats.Stats getGenericThreadpoolStat(ThreadPool threadPool) {
        var result = threadPool.stats().stats().stream().filter(stats -> stats.name().equals(ThreadPool.Names.GENERIC)).toList();
        assertThat(result.size(), equalTo(1));
        return result.get(0);
    }

    @Override
    public void tearDown() throws Exception {
        // Need to clean up the data stream and logsdb.prior_logs_usage setting because ESSingleNodeTestCase tests aren't allowed to leave
        // persistent cluster settings around.

        var deleteDataStreamsRequest = new DeleteDataStreamAction.Request(TEST_REQUEST_TIMEOUT, "*");
        deleteDataStreamsRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
        assertAcked(client().execute(DeleteDataStreamAction.INSTANCE, deleteDataStreamsRequest));

        var settings = Settings.builder().put("logsdb.prior_logs_usage", (String) null).build();
        client().admin()
            .cluster()
            .updateSettings(new ClusterUpdateSettingsRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE).persistentSettings(settings))
            .actionGet();

        super.tearDown();
    }
}
