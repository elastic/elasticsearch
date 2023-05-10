/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster.RestartCallback;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

@ClusterScope(numDataNodes = 0, scope = Scope.TEST)
public class QuorumGatewayIT extends ESIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 2;
    }

    public void testQuorumRecovery() throws Exception {
        logger.info("--> starting 3 nodes");
        // we are shutting down nodes - make sure we don't have 2 clusters if we test network
        internalCluster().startNodes(3);

        createIndex("test");
        ensureGreen();
        final NumShards test = getNumShards("test");

        logger.info("--> indexing...");
        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        // We don't check for failures in the flush response: if we do we might get the following:
        // FlushNotAllowedEngineException[[test][1] recovery is in progress, flush [COMMIT_TRANSLOG] is not allowed]
        flush();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("field", "value2").endObject()).get();
        refresh();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2L);
        }
        logger.info("--> restart all nodes");
        internalCluster().fullRestart(new RestartCallback() {
            @Override
            public void doAfterNodes(int numNodes, final Client activeClient) throws Exception {
                if (numNodes == 1) {
                    assertBusy(() -> {
                        logger.info("--> running cluster_health (wait for the shards to startup)");
                        ClusterHealthResponse clusterHealth = activeClient.admin()
                            .cluster()
                            .health(
                                new ClusterHealthRequest(new String[] {}).waitForYellowStatus()
                                    .waitForNodes("2")
                                    .waitForActiveShards(test.numPrimaries * 2)
                            )
                            .actionGet();
                        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());
                        assertFalse(clusterHealth.isTimedOut());
                        assertEquals(ClusterHealthStatus.YELLOW, clusterHealth.getStatus());
                    }, 30, TimeUnit.SECONDS);

                    logger.info("--> one node is closed -- index 1 document into the remaining nodes");
                    activeClient.prepareIndex("test")
                        .setId("3")
                        .setSource(jsonBuilder().startObject().field("field", "value3").endObject())
                        .get();
                    assertNoFailures(activeClient.admin().indices().prepareRefresh().get());
                    for (int i = 0; i < 10; i++) {
                        assertHitCount(activeClient.prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 3L);
                    }
                }
            }

        });
        logger.info("--> all nodes are started back, verifying we got the latest version");
        logger.info("--> running cluster_health (wait for the shards to startup)");
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 3L);
        }
    }
}
