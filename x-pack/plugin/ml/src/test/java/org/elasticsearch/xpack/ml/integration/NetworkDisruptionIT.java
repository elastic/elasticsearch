/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NetworkDisruptionIT extends BaseMlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @TestLogging(value = "org.elasticsearch.persistent.PersistentTasksClusterService:trace",
            reason = "https://github.com/elastic/elasticsearch/issues/49908")
    public void testJobRelocation() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(5);
        ensureStableCluster(5);

        Job.Builder job = createJob("relocation-job", new ByteSizeValue(2, ByteSizeUnit.MB));
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();

        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        AcknowledgedResponse openJobResponse = client().execute(OpenJobAction.INSTANCE, openJobRequest).actionGet();
        assertTrue(openJobResponse.isAcknowledged());
        ensureGreen();

        // Record which node the job starts off on
        String origJobNode = awaitJobOpenedAndAssigned(job.getId(), null);

        // Isolate the node the job is running on from the cluster
        Set<String> isolatedSide = Collections.singleton(origJobNode);
        Set<String> restOfClusterSide = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        restOfClusterSide.remove(origJobNode);
        String notIsolatedNode = restOfClusterSide.iterator().next();

        NetworkDisruption networkDisruption = new NetworkDisruption(new NetworkDisruption.TwoPartitions(isolatedSide, restOfClusterSide),
                new NetworkDisruption.NetworkDisconnect());
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(4, notIsolatedNode);

        // Job should move to a new node in the bigger portion of the cluster
        String newJobNode = awaitJobOpenedAndAssigned(job.getId(), notIsolatedNode);
        assertNotEquals(origJobNode, newJobNode);

        networkDisruption.removeAndEnsureHealthy(internalCluster());
        ensureGreen();

        // Job should remain running on the new node, not the one that temporarily detached from the cluster
        String finalJobNode = awaitJobOpenedAndAssigned(job.getId(), null);
        assertEquals(newJobNode, finalJobNode);

        // The job running on the original node should have been killed, and hence should not have persisted quantiles
        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
                .setQuery(QueryBuilders.idsQuery().addIds(Quantiles.documentId(job.getId())))
                .setTrackTotalHits(true)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertEquals(0L, searchResponse.getHits().getTotalHits().value);

        CloseJobAction.Request closeJobRequest = new CloseJobAction.Request(job.getId());
        CloseJobAction.Response closeJobResponse = client().execute(CloseJobAction.INSTANCE, closeJobRequest).actionGet();
        assertTrue(closeJobResponse.isClosed());

        // The relocated job was closed rather than killed, and hence should have persisted quantiles
        searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
                .setQuery(QueryBuilders.idsQuery().addIds(Quantiles.documentId(job.getId())))
                .setTrackTotalHits(true)
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).execute().actionGet();
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
    }
}
