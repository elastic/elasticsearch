/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.AfterClass;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public abstract class AbstractAlertsSingleNodeTests extends ElasticsearchSingleNodeTest {

    @AfterClass
    public static void cleanupSuite() throws Exception {
        node().stop();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    protected IndexResponse index(String index, String type, String id) {
        return index(index, type, id, Collections.<String, Object>emptyMap());
    }

    protected IndexResponse index(String index, String type, String id, Map<String, Object> doc) {
        return client().prepareIndex(index, type, id).setSource(doc).get();
    }

    protected ClusterHealthStatus ensureGreen(String... indices) {
        ClusterHealthResponse actionGet = client().admin().cluster()
                .health(Requests.clusterHealthRequest(indices).timeout(TimeValue.timeValueSeconds(30)).waitForGreenStatus().waitForEvents(Priority.LANGUID).waitForRelocatingShards(0)).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info("ensureGreen timed out, cluster state:\n{}\n{}", client().admin().cluster().prepareState().get().getState().prettyPrint(), client().admin().cluster().preparePendingClusterTasks().get().prettyPrint());
            assertThat("timed out waiting for green state", actionGet.isTimedOut(), equalTo(false));
        }
        assertThat(actionGet.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        logger.debug("indices {} are green", indices.length == 0 ? "[_all]" : indices);
        return actionGet.getStatus();
    }

    protected RefreshResponse refresh() {
        RefreshResponse actionGet = client().admin().indices().prepareRefresh().execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected ClientProxy clientProxy() {
        return ClientProxy.of(client());
    }

    protected ScriptServiceProxy scriptService() {
        return getInstanceFromNode(ScriptServiceProxy.class);
    }
}
