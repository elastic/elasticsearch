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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.SkipUnavailableRule;
import org.elasticsearch.usage.UsageService;
import org.junit.Assert;
import org.junit.Rule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class AbstractCrossClustersUsageTelemetryIT extends AbstractMultiClustersTestCase {
    private static final Logger LOGGER = LogManager.getLogger(AbstractCrossClustersUsageTelemetryIT.class);
    private static final String REMOTE1 = "cluster-a";
    private static final String REMOTE2 = "cluster-b";
    private static final String LOCAL_INDEX = "logs-1";
    private static final String REMOTE_INDEX = "logs-2";

    protected CCSTelemetrySnapshot getTelemetryFromQuery(String query, String client) throws ExecutionException, InterruptedException {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.columnar(randomBoolean());
        request.includeCCSMetadata(randomBoolean());
        return getTelemetryFromQuery(request, client);
    }

    protected CCSTelemetrySnapshot getTelemetryFromQuery(EsqlQueryRequest request, String client) throws ExecutionException,
        InterruptedException {
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        // We don't care here too much about the response, we just want to trigger the telemetry collection.
        // So we check it's not null and leave the rest to other tests.
        if (client != null) {
            assertResponse(
                cluster(LOCAL_CLUSTER).client(nodeName)
                    .filterWithHeader(Map.of(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER, client))
                    .execute(EsqlQueryAction.INSTANCE, request),
                Assert::assertNotNull
            );

        } else {
            assertResponse(cluster(LOCAL_CLUSTER).client(nodeName).execute(EsqlQueryAction.INSTANCE, request), Assert::assertNotNull);
        }
        return getTelemetrySnapshot(nodeName);
    }

    protected CCSTelemetrySnapshot getTelemetryFromFailedQuery(String query) throws Exception {
        // We want to send search to a specific node (we don't care which one) so that we could
        // collect the CCS telemetry from it later
        String nodeName = cluster(LOCAL_CLUSTER).getRandomNodeName();
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.columnar(randomBoolean());
        request.includeCCSMetadata(randomBoolean());

        ExecutionException ee = expectThrows(
            ExecutionException.class,
            cluster(LOCAL_CLUSTER).client(nodeName).execute(EsqlQueryAction.INSTANCE, request)::get
        );
        assertNotNull(ee.getCause());

        return getTelemetrySnapshot(nodeName);
    }

    private CCSTelemetrySnapshot getTelemetrySnapshot(String nodeName) {
        var usage = cluster(LOCAL_CLUSTER).getInstance(UsageService.class, nodeName);
        return usage.getEsqlUsageHolder().getCCSTelemetrySnapshot();
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE1, REMOTE2);
    }

    @Rule
    public SkipUnavailableRule skipOverride = new SkipUnavailableRule(REMOTE1, REMOTE2);

    protected Map<String, Object> setupClusters() {
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
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        var map = skipOverride.getMap();
        LOGGER.info("Using skip_unavailable map: [{}]", map);
        return map;
    }
}
