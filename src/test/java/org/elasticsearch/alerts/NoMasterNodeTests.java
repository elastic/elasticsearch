/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numClientNodes = 0, transportClientRatio = 0, numDataNodes = 0)
public class NoMasterNodeTests extends AbstractAlertingTests {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings settings = super.nodeSettings(nodeOrdinal);
        return ImmutableSettings.builder()
                .put(settings)
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES, 2)
                .build();
    }

    @Test
    public void testSimpleFailure() throws Exception {
        internalTestCluster().startNodesAsync(2).get();
        AlertsClientInterface alertsClient = alertClient();
        createIndex("my-index");
        // Have a sample document in the index, the alert is going to evaluate
        client().prepareIndex("my-index", "my-type").setSource("field", "value").get();
        SearchRequest searchRequest = new SearchRequest("my-index").source(searchSource().query(termQuery("field", "value")));
        BytesReference alertSource = createAlertSource("0/5 * * * * ? *", searchRequest, "hits.total == 1");
        alertsClient.prepareIndexAlert("my-first-alert")
                .setAlertSource(alertSource)
                .get();
        assertAlertTriggered("my-first-alert");

        // Stop the elected master, no new master will be elected b/c of m_m_n is set to 2
        internalTestCluster().stopCurrentMasterNode();
        assertThat(awaitBusy(new Predicate<Object>() {
            public boolean apply(Object obj) {
                ClusterState state = client().admin().cluster().prepareState().setLocal(true).execute().actionGet().getState();
                return state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ID);
            }
        }), equalTo(true));

        // Need to fetch a new client the old one maybe an internal client of the node we just killed.
        alertsClient = alertClient();
        try {
            // any alerting action should fail, because there is no elected master node
            alertsClient.prepareDeleteAlert("my-first-alert").setMasterNodeTimeout(TimeValue.timeValueSeconds(1)).get();
            fail();
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(MasterNotDiscoveredException.class));
        }

        // Bring back the 2nd node and wait for elected master node to come back and alerting to work as expected.
        internalTestCluster().startNode();
        ensureGreen();

        // Delete an existing alert
        DeleteAlertResponse response = alertsClient.prepareDeleteAlert("my-first-alert").get();
        assertThat(response.deleteResponse().isFound(), is(true));
        // Add a new alert and wait for it get triggered
        alertsClient.prepareIndexAlert("my-second-alert")
                .setAlertSource(alertSource)
                .get();
        assertAlertTriggered("my-second-alert");
    }

}
