/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.stats.CCSTelemetrySnapshot;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.stats.CCSUsageTelemetry.ASYNC_FEATURE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class CrossClustersUsageTelemetryIT extends AbstractMultiClustersTestCase {
    private static final Logger LOGGER = LogManager.getLogger(CrossClustersUsageTelemetryIT.class);
    private static final String REMOTE1 = "cluster-a";
    private static final String REMOTE2 = "cluster-b";
    private static final String LOCAL_INDEX = "logs-1";
    private static final String REMOTE_INDEX = "logs-2";

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE1, REMOTE2);
    }

    @Rule
    public SkipUnavailableRule skipOverride = new SkipUnavailableRule(REMOTE1, REMOTE2);

    public void testLocalRemote() throws Exception {
        setupClusters();
        var telemetry = getTelemetryFromQuery("from logs-*,c*:logs-* | stats sum (v)", "kibana");

        assertThat(telemetry.getTotalCount(), equalTo(1L));
        assertThat(telemetry.getSuccessCount(), equalTo(1L));
        assertThat(telemetry.getFailureReasons().size(), equalTo(0));
        assertThat(telemetry.getTook().count(), equalTo(1L));
        assertThat(telemetry.getTookMrtFalse().count(), equalTo(0L));
        assertThat(telemetry.getTookMrtTrue().count(), equalTo(0L));
        assertThat(telemetry.getRemotesPerSearchAvg(), equalTo(2.0));
        assertThat(telemetry.getRemotesPerSearchMax(), equalTo(2L));
        assertThat(telemetry.getSearchCountWithSkippedRemotes(), equalTo(0L));
        assertThat(telemetry.getClientCounts().size(), equalTo(1));
        assertThat(telemetry.getClientCounts().get("kibana"), equalTo(1L));
        assertThat(telemetry.getFeatureCounts().get(ASYNC_FEATURE), equalTo(null));

        var perCluster = telemetry.getByRemoteCluster();
        assertThat(perCluster.size(), equalTo(3));
        for (String clusterAlias : remoteClusterAlias()) {
            var clusterTelemetry = perCluster.get(clusterAlias);
            assertThat(clusterTelemetry.getCount(), equalTo(1L));
            assertThat(clusterTelemetry.getSkippedCount(), equalTo(0L));
            assertThat(clusterTelemetry.getTook().count(), equalTo(1L));
        }

    }

    private CCSTelemetrySnapshot getTelemetryFromQuery(String query) throws ExecutionException, InterruptedException {
        return getTelemetryFromQuery(query, null);
    }

    private CCSTelemetrySnapshot getTelemetryFromQuery(String query, String client) throws ExecutionException, InterruptedException {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.columnar(randomBoolean());
        request.includeCCSMetadata(randomBoolean());
        return getTelemetryFromQuery(request, client);
    }

    private CCSTelemetrySnapshot getTelemetryFromQuery(EsqlQueryRequest request, String client) throws ExecutionException,
        InterruptedException {
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        // We don't care here too much about the response, we just want to trigger the telemetry collection.
        // So we check it's not null and leave the rest to other tests.
        if (client != null) {
            assertResponse(
                cluster(LOCAL_CLUSTER).client(nodeName)
                    .filterWithHeader(Map.of(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, "kibana"))
                    .execute(EsqlQueryAction.INSTANCE, request),
                Assert::assertNotNull
            );

        } else {
            assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).execute(EsqlQueryAction.INSTANCE, request), Assert::assertNotNull);
        }
        return getTelemetrySnapshot(nodeName);
    }

    private CCSTelemetrySnapshot getTelemetrySnapshot(String nodeName) {
        var usage = cluster(LOCAL_CLUSTER).getInstance(UsageService.class, nodeName);
        return usage.getEsqlUsageHolder().getCCSTelemetrySnapshot();
    }

    Map<String, Object> setupClusters() {
        int numShardsLocal = randomIntBetween(1, 5);
        populateLocalIndices(LOCAL_INDEX, numShardsLocal);

        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE1, REMOTE_INDEX, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", REMOTE_INDEX);

        int numShardsRemote2 = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE2, REMOTE_INDEX, numShardsRemote2);
        clusterInfo.put("remote2.index", REMOTE_INDEX);
        clusterInfo.put("remote2.num_shards", numShardsRemote2);

        return clusterInfo;
    }

    void populateLocalIndices(String indexName, int numShards) {
        Client localClient = client(LOCAL_CLUSTER);
        assertAcked(
            localClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            localClient.prepareIndex(indexName).setSource("id", "local-" + i, "tag", "local", "v", i).get();
        }
        localClient.admin().indices().prepareRefresh(indexName).get();
    }

    void populateRemoteIndices(String clusterAlias, String indexName, int numShards) {
        Client remoteClient = client(clusterAlias);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            remoteClient.prepareIndex(indexName).setSource("id", "remote-" + i, "tag", "remote", "v", i * i).get();
        }
        remoteClient.admin().indices().prepareRefresh(indexName).get();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(CrossClustersQueryIT.InternalExchangePlugin.class);
        return plugins;
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        var map = skipOverride.getMap();
        LOGGER.info("Using skip_unavailable map: [{}]", map);
        return map;
    }

    /**
     * Annotation to mark specific cluster in a test as not to be skipped when unavailable
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface SkipOverride {
        String[] aliases();
    }

    /**
     * Test rule to process skip annotations
     */
    static class SkipUnavailableRule implements TestRule {
        private final Map<String, Boolean> skipMap;

        SkipUnavailableRule(String... clusterAliases) {
            this.skipMap = Arrays.stream(clusterAliases).collect(Collectors.toMap(Function.identity(), alias -> true));
        }

        public Map<String, Boolean> getMap() {
            return skipMap;
        }

        @Override
        public Statement apply(Statement base, Description description) {
            // Check for annotation named "SkipOverride" and set the overrides accordingly
            var aliases = description.getAnnotation(SkipOverride.class);
            if (aliases != null) {
                for (String alias : aliases.aliases()) {
                    skipMap.put(alias, false);
                }
            }
            return base;
        }

    }
}
